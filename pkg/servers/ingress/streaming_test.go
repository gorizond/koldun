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

func TestServeStreamRequiresFlusher(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("requires-flusher", true)
	writer := &noFlushWriter{}
	srv.serveStream(context.Background(), writer, handle)

	require.Equal(t, http.StatusInternalServerError, writer.status)
	require.Contains(t, writer.body.String(), `"streaming not supported"`)
}

func TestServeStreamHappyPath(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("happy-path", true)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := `{"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}`
	go func() {
		time.Sleep(2 * time.Millisecond)
		handle.Publish(payload)
		handle.Complete()
	}()

	srv.serveStream(ctx, rec, handle)

	body := rec.Body.String()
	chunks := strings.Split(strings.TrimSpace(body), "\n\n")
	require.Len(t, chunks, 2)

	first := strings.TrimPrefix(chunks[0], "data: ")
	require.JSONEq(t, payload, first)

	require.Equal(t, "data: [DONE]", strings.TrimSpace(chunks[1]))
	require.Greater(t, rec.flushes, 0)
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestServeStreamContextCancelled(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("context-cancelled", true)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.serveStream(ctx, rec, handle)
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

// TestPumpSubscriptionClosed ensures the SSE client sees an error chunk when
// the upstream subscription closes before any payloads are streamed.
func TestPumpSubscriptionClosed(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("subscription-closed", true)
	msgs := make(chan *nats.Msg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pumpCtx, pumpCancel := context.WithCancel(ctx)
	go srv.pumpResponse(pumpCtx, pumpCancel, handle, "hash", msgs, nil)
	msgs <- nil

	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	srv.serveStream(ctx, rec, handle)

	body := rec.Body.String()
	chunks := strings.Split(strings.TrimSpace(body), "\n\n")
	require.Len(t, chunks, 2)

	errorChunk := strings.TrimPrefix(chunks[0], "data: ")
	require.Contains(t, errorChunk, `"subscription closed"`)
	require.Equal(t, "data: [DONE]", strings.TrimSpace(chunks[1]))
}

func TestServeStreamHeartbeat(t *testing.T) {
	srv := &Server{
		log:                     logrus.New().WithField("component", "test"),
		streamHeartbeatInterval: 5 * time.Millisecond,
	}
	handle := srv.dedupeBegin("heartbeat", true)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.serveStream(ctx, rec, handle)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	require.Contains(t, rec.Body.String(), ": \n\n")
	require.Greater(t, rec.flushes, 0)
}
