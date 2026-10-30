package ingress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushes int
}

func (f *flushRecorder) Flush() {
	f.flushes++
}

type noFlushWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *noFlushWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *noFlushWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.WriteString(string(b))
}

func (w *noFlushWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func TestThinkRedactorFilter(t *testing.T) {
	t.Parallel()

	t.Run("no think tags", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.Equal(t, "hello world", redactor.filter("hello world"))
	})

	t.Run("strip think block across chunks", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor

		require.Equal(t, "prefix ", redactor.filter("prefix <think>secret"))
		require.Equal(t, "", redactor.filter(" plan"))
		require.Equal(t, " visible", redactor.filter("</think> visible"))
	})

	t.Run("drop stray closing tag", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.Equal(t, "payload", redactor.filter("</think>payload"))
	})

	t.Run("details reasoning block", func(t *testing.T) {
		t.Parallel()
		var redactor detailsRedactor

		require.Equal(t, "prefix ", redactor.filter("prefix <details type=\"reasoning\">secret"))
		require.Equal(t, "", redactor.filter(" plan"))
		require.Equal(t, " visible", redactor.filter("</details> visible"))
	})

	t.Run("details with attributes", func(t *testing.T) {
		t.Parallel()
		var redactor detailsRedactor

		result := redactor.filter("before <details type=\"reasoning\" done=\"true\" duration=\"1\">thinking")
		require.Equal(t, "before ", result)

		result = redactor.filter(" here</details> after")
		require.Equal(t, " after", result)
	})

	t.Run("details across chunks", func(t *testing.T) {
		t.Parallel()
		var redactor detailsRedactor

		// Chunk 1: partial opening tag
		require.Equal(t, "", redactor.filter("<detail"))

		// Chunk 2: rest of opening tag + content + partial closing tag
		require.Equal(t, "", redactor.filter(`s type="reasoning">secret plan</detail`))

		// Chunk 3: rest of closing tag + visible content
		require.Equal(t, " visible", redactor.filter("s> visible"))
	})

	t.Run("details stray closing tag", func(t *testing.T) {
		t.Parallel()
		var redactor detailsRedactor
		require.Equal(t, "payload", redactor.filter("</details>payload"))
	})
}

func TestLongestThinkSuffix(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, longestThinkSuffix("plain text"))
	require.Equal(t, 4, longestThinkSuffix("prefix<thi"))
	require.Equal(t, 6, longestThinkSuffix("prefix<think"))
	require.Equal(t, 5, longestThinkSuffix("suffix</thi"))
}

func TestStreamingNormaliserStateScrubContent(t *testing.T) {
	t.Parallel()

	require.Equal(t, "content", (*streamingNormaliserState)(nil).scrubContent("content"))

	state := &streamingNormaliserState{}
	require.Equal(t, "visible", state.scrubContent("<think>hidden</think>visible"))
}

func TestNormaliseStreamingChunk(t *testing.T) {
	t.Parallel()

	state := &streamingNormaliserState{}
	raw := `{"choices":[{"finish_reason":"  ","delta":{"role":"assistant","content":"hello"}}]}`
	normalised, err := normaliseStreamingChunk(raw, state)
	require.NoError(t, err)

	var chunk streamingChunk
	require.NoError(t, json.Unmarshal([]byte(normalised), &chunk))
	require.Equal(t, "chat.completion.chunk", chunk.Object)
	require.Len(t, chunk.Choices, 1)
	require.Nil(t, chunk.Choices[0].FinishReason)
	require.Equal(t, "assistant", chunk.Choices[0].Delta.Role)
	require.Equal(t, "hello", chunk.Choices[0].Delta.Content)

	// Second chunk should suppress the role and scrub think content.
	nextRaw := `{"choices":[{"delta":{"role":"assistant","content":"<think>plan</think>respond"}}]}`
	nextNormalised, err := normaliseStreamingChunk(nextRaw, state)
	require.NoError(t, err)

	chunk = streamingChunk{}
	require.NoError(t, json.Unmarshal([]byte(nextNormalised), &chunk))
	require.Len(t, chunk.Choices, 1)
	require.Empty(t, chunk.Choices[0].Delta.Role, "role should be cleared after first emission")
	require.Equal(t, "respond", strings.TrimSpace(chunk.Choices[0].Delta.Content))

	// Invalid JSON returns the original payload.
	rawInvalid := "not-json"
	invalid, err := normaliseStreamingChunk(rawInvalid, nil)
	require.NoError(t, err)
	require.Equal(t, rawInvalid, invalid)
}

func TestNormaliseStreamingChunkWithToolCalls(t *testing.T) {
	t.Parallel()

	state := &streamingNormaliserState{}
	raw := `{"choices":[{"delta":{"role":"assistant","tool_calls":[{"id":"call_123","type":"function","function":{"name":"get_weather","arguments":"{\"location\":\"Moscow\"}"}}]}}]}`
	normalised, err := normaliseStreamingChunk(raw, state)
	require.NoError(t, err)

	var chunk streamingChunk
	require.NoError(t, json.Unmarshal([]byte(normalised), &chunk))
	require.Len(t, chunk.Choices, 1)
	require.Equal(t, "assistant", chunk.Choices[0].Delta.Role)
	require.Len(t, chunk.Choices[0].Delta.ToolCalls, 1)
	require.Equal(t, "call_123", chunk.Choices[0].Delta.ToolCalls[0].ID)
	require.Equal(t, "function", chunk.Choices[0].Delta.ToolCalls[0].Type)
	require.Equal(t, "get_weather", chunk.Choices[0].Delta.ToolCalls[0].Function.Name)
	require.Equal(t, `{"location":"Moscow"}`, chunk.Choices[0].Delta.ToolCalls[0].Function.Arguments)

	// Second chunk with additional tool call should preserve it
	nextRaw := `{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_456","type":"function","function":{"name":"get_time","arguments":"{}"}}]}}]}`
	nextNormalised, err := normaliseStreamingChunk(nextRaw, state)
	require.NoError(t, err)

	chunk = streamingChunk{}
	require.NoError(t, json.Unmarshal([]byte(nextNormalised), &chunk))
	require.Len(t, chunk.Choices, 1)
	require.Empty(t, chunk.Choices[0].Delta.Role, "role should be cleared after first emission")
	require.Len(t, chunk.Choices[0].Delta.ToolCalls, 1)
	require.Equal(t, "call_456", chunk.Choices[0].Delta.ToolCalls[0].ID)
}

func TestEventTimestamp(t *testing.T) {
	t.Parallel()

	before := time.Now()
	got := eventTimestamp(0)
	after := time.Now()
	require.True(t, (got.Equal(before) || got.After(before)) && (got.Equal(after) || got.Before(after)),
		"fallback timestamp should be within call window")

	past := time.Now().Add(-2 * time.Hour).Unix()
	require.Equal(t, time.Unix(past, 0), eventTimestamp(past))
}

func TestStreamResponseRequiresFlusher(t *testing.T) {
	t.Parallel()

	srv := &Server{}
	writer := &noFlushWriter{}
	srv.streamResponse(context.Background(), writer, make(chan *nats.Msg), nil)

	require.Equal(t, http.StatusInternalServerError, writer.status)
	require.Contains(t, writer.body.String(), `"streaming not supported"`)
}

func TestStreamResponseHappyPath(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	msgs := make(chan *nats.Msg, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.streamResponse(ctx, rec, msgs, nil)
	}()

	msgs <- &nats.Msg{Data: []byte(`{"choices":[{"delta":{"role":"assistant","content":"hello"}}]}`)}
	msgs <- &nats.Msg{Data: []byte("[DONE]")}

	wg.Wait()

	body := rec.Body.String()
	chunks := strings.Split(strings.TrimSpace(body), "\n\n")
	require.Len(t, chunks, 2)

	first := strings.TrimPrefix(chunks[0], "data: ")
	var chunk streamingChunk
	require.NoError(t, json.Unmarshal([]byte(first), &chunk))
	require.Equal(t, "chat.completion.chunk", chunk.Object)
	require.Len(t, chunk.Choices, 1)
	require.Nil(t, chunk.Choices[0].FinishReason)
	require.Equal(t, "assistant", chunk.Choices[0].Delta.Role)
	require.Equal(t, "hello", chunk.Choices[0].Delta.Content)

	require.Equal(t, "data: [DONE]", strings.TrimSpace(chunks[1]))
	require.Greater(t, rec.flushes, 0)
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestStreamResponseContextCancelled(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	msgs := make(chan *nats.Msg)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.streamResponse(ctx, rec, msgs, nil)
	}()

	time.AfterFunc(5*time.Millisecond, cancel)
	wg.Wait()

	body := rec.Body.String()
	chunks := strings.Split(strings.TrimSpace(body), "\n\n")
	require.Len(t, chunks, 2)

	errorChunk := strings.TrimPrefix(chunks[0], "data: ")
	require.Contains(t, errorChunk, `"error":{"message":"context canceled"}`)
	require.Equal(t, "data: [DONE]", strings.TrimSpace(chunks[1]))
}

// TestStreamResponseSubscriptionClosed ensures SSE emits an error chunk when the
// response subscription closes before any payloads are streamed (e.g. backlog
// publish failure or dispatcher drop).
func TestStreamResponseSubscriptionClosed(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	msgs := make(chan *nats.Msg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.streamResponse(ctx, rec, msgs, nil)
	}()

	close(msgs)
	wg.Wait()

	body := rec.Body.String()
	chunks := strings.Split(strings.TrimSpace(body), "\n\n")
	require.Len(t, chunks, 2)

	errorChunk := strings.TrimPrefix(chunks[0], "data: ")
	require.Contains(t, errorChunk, `"subscription closed"`)
	require.Equal(t, "data: [DONE]", strings.TrimSpace(chunks[1]))
}

func TestConsumeStateEvent(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	subject := "sessions.demo.dllama.worker-1.state"
	event := conversation.WorkerStateEvent{
		State:     "Idle",
		Active:    0,
		Timestamp: time.Now().Unix(),
	}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	srv.consumeStateEvent(subject, payload)
	srv.consumeStateEvent(subject, []byte("not-json"))

	srv.stateCache.mu.RLock()
	defer srv.stateCache.mu.RUnlock()

	prefix := "sessions.demo.dllama."
	workers := srv.stateCache.workers[prefix]
	require.NotNil(t, workers)
	entry, ok := workers["worker-1"]
	require.True(t, ok)
	require.Equal(t, "Idle", entry.state)
	require.Equal(t, int32(0), entry.active)
}

type mockFlusherWriter struct {
	http.ResponseWriter
	flushed bool
}

func (m *mockFlusherWriter) Flush() {
	m.flushed = true
}

func TestResponseWriterWrapperDelegatesFlush(t *testing.T) {
	t.Parallel()

	inner := &mockFlusherWriter{ResponseWriter: httptest.NewRecorder()}
	wrapper := &responseWriterWrapper{ResponseWriter: inner, statusCode: http.StatusOK}

	// Verify wrapper implements http.Flusher
	var rw http.ResponseWriter = wrapper
	flusher, ok := rw.(http.Flusher)
	require.True(t, ok, "responseWriterWrapper should implement http.Flusher")

	// Verify Flush delegates to inner writer
	flusher.Flush()
	require.True(t, inner.flushed, "Flush should be delegated to inner ResponseWriter")
}

func TestIsEmptyDeltaChunk(t *testing.T) {
	t.Parallel()

	// Empty delta (no role, no content, no finish_reason)
	require.True(t, isEmptyDeltaChunk(`{"choices":[{"delta":{}}]}`))
	require.True(t, isEmptyDeltaChunk(`{"choices":[{"delta":{"role":"","content":""}}]}`))

	// Has content
	require.False(t, isEmptyDeltaChunk(`{"choices":[{"delta":{"content":"hello"}}]}`))

	// Has role
	require.False(t, isEmptyDeltaChunk(`{"choices":[{"delta":{"role":"assistant"}}]}`))

	// Has finish_reason
	require.False(t, isEmptyDeltaChunk(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`))

	// Invalid JSON should not be considered empty (pass through)
	require.False(t, isEmptyDeltaChunk(`not-json`))
}

const (
	// Hex escapes keep the tags unambiguous in generated sources.
	testThinkOpen  = "\x3cthink\x3e"
	testThinkClose = "\x3c/think\x3e"
)

func TestThinkRedactorUnicodeExpansionNoPanic(t *testing.T) {
	t.Parallel()

	// U+023A lowercases to a 3-byte rune in Go, so offsets taken from a
	// lowercased copy can exceed len(buffer): these cases used to panic with
	// "slice bounds out of range" while streaming.
	t.Run("stray close tag after expanding runes", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.NotPanics(t, func() {
			require.Equal(t, "", redactor.filter("\u023a\u023a\u023a"+testThinkClose))
		})
	})

	t.Run("answer after close tag", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.NotPanics(t, func() {
			require.Equal(t, "answer", redactor.filter("x\u023a\u023a\u023a"+testThinkClose+"answer"))
		})
	})

	t.Run("dotted capital I", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.NotPanics(t, func() {
			require.Equal(t, "answer", redactor.filter("\u0130\u0130\u0130"+testThinkClose+"answer"))
		})
	})

	t.Run("think block with expanding runes", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.NotPanics(t, func() {
			require.Equal(t, "visible", redactor.filter(testThinkOpen+"secret\u023a\u023a\u023a"+testThinkClose+"visible"))
		})
	})

	t.Run("split chunks with expanding runes", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.Equal(t, "prefix ", redactor.filter("prefix "+testThinkOpen+"\u023a"))
		require.Equal(t, "", redactor.filter("\u023a"))
		require.Equal(t, "visible", redactor.filter("\u023a"+testThinkClose+"visible"))
	})
}

func TestThinkRedactorSplitTags(t *testing.T) {
	t.Parallel()

	t.Run("open and close tags split across chunks", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor

		var out strings.Builder
		out.WriteString(redactor.filter("\x3cthi"))
		out.WriteString(redactor.filter("nk\x3esecret\x3c/thi"))
		out.WriteString(redactor.filter("nk\x3eanswer"))

		require.Equal(t, "answer", out.String())
		require.NotContains(t, out.String(), "secret")
	})

	t.Run("stray close tag split across chunks", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor

		var out strings.Builder
		out.WriteString(redactor.filter("\x3c/thi"))
		out.WriteString(redactor.filter("nk\x3epayload"))

		require.Equal(t, "payload", out.String())
	})

	t.Run("answer preserved after think", func(t *testing.T) {
		t.Parallel()
		var redactor thinkRedactor
		require.Equal(t, "a c", redactor.filter("a "+testThinkOpen+"b"+testThinkClose+"c"))
	})
}

func TestLongestThinkSuffixShortBuffers(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, longestThinkSuffix(""))
	require.Equal(t, 1, longestThinkSuffix("\x3c"))
	require.Equal(t, 2, longestThinkSuffix("\x3c/"))
	require.Equal(t, 0, longestThinkSuffix(" think"))
	require.Equal(t, 6, longestThinkSuffix("\x3c/THIN"))
	require.Equal(t, 0, longestThinkSuffix("\u023a\u023a"))
}

func TestDetailsRedactorUnicodeNoPanic(t *testing.T) {
	t.Parallel()

	var redactor detailsRedactor
	require.NotPanics(t, func() {
		require.Equal(t, "visible", redactor.filter("\x3cdetails type=\"reasoning\"\x3e\u023a\u023a\u023a\x3c/details\x3evisible"))
	})
}

func TestNormaliseStreamingChunkScrubsExpandingRunes(t *testing.T) {
	t.Parallel()

	state := &streamingNormaliserState{}
	raw := `{"choices":[{"delta":{"role":"assistant","content":"\u023a\u023a\u023a` + testThinkClose + `answer"}}]}`

	normalised, err := normaliseStreamingChunk(raw, state)
	require.NoError(t, err)
	require.NotContains(t, normalised, "response")
	require.Contains(t, normalised, "answer")
}

func TestStreamResponseHeartbeat(t *testing.T) {
	srv := &Server{
		log:                     logrus.New().WithField("component", "test"),
		streamHeartbeatInterval: 5 * time.Millisecond,
	}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	msgs := make(chan *nats.Msg)

	ctx, cancel := context.WithCancel(context.Background())
	heartbeats := make(chan struct{}, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.streamResponse(ctx, rec, msgs, func() {
			select {
			case heartbeats <- struct{}{}:
			default:
			}
		})
	}()

	select {
	case <-heartbeats:
		cancel()
	case <-time.After(time.Second):
		cancel()
		wg.Wait()
		t.Fatal("stream heartbeat was not invoked")
	}
	wg.Wait()
}
