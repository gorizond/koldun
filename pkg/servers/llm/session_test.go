package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/api/openai"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	testhelpers "github.com/gorizond/koldun/pkg/testutil"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingSidecar struct {
	mu     sync.Mutex
	bodies [][]byte
	chunks []string
	status int
}

func (r *recordingSidecar) handle(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)

	r.mu.Lock()
	r.bodies = append(r.bodies, body)
	status := r.status
	chunks := append([]string(nil), r.chunks...)
	r.mu.Unlock()

	if status != 0 && status != http.StatusOK {
		http.Error(w, "sidecar failure", status)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	flusher, _ := w.(http.Flusher)
	for _, chunk := range chunks {
		_, _ = fmt.Fprintf(w, "data: %s\n\n", chunk)
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (r *recordingSidecar) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *recordingSidecar) body(i int) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < 0 || i >= len(r.bodies) {
		return nil
	}
	return append([]byte(nil), r.bodies[i]...)
}

func discardLogger() *logrus.Entry {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logrus.NewEntry(logger)
}

func newSessionTestServer(t *testing.T, sidecar *recordingSidecar) (*Server, *testhelpers.RecordingNATSConn) {
	t.Helper()
	ts := testhelpers.NewHTTPServer(t, http.HandlerFunc(sidecar.handle))
	t.Cleanup(ts.Close)

	nc := &testhelpers.RecordingNATSConn{}
	srv := &Server{
		cfg: Config{
			DllamaName:   "worker-1",
			SidecarURL:   ts.URL,
			MaxSeqLen:    4096,
			StateSubject: "state.test",
		},
		nc:           nc,
		client:       ts.Client(),
		outSubject:   "out.test",
		stateSubject: "state.test",
		log:          discardLogger(),
	}
	srv.initSessionState()
	return srv, nc
}

type chatHistory struct {
	Model    string               `json:"model"`
	Stream   bool                 `json:"stream"`
	Messages []openai.ChatMessage `json:"messages"`
}

func marshalHistory(t *testing.T, messages []openai.ChatMessage) []byte {
	t.Helper()
	raw, err := json.Marshal(chatHistory{Model: "test-model", Stream: true, Messages: messages})
	require.NoError(t, err)
	return raw
}

func buildSessionPayload(t *testing.T, requestID, subject string, messages []openai.ChatMessage, cursor int, prefixHash string) []byte {
	t.Helper()
	raw := marshalHistory(t, messages)
	payload := inboundRequest{
		Hash:            "conv-1",
		ChatID:          "conv-1",
		RequestID:       requestID,
		Request:         openai.ChatCompletionRequest{Model: "test-model", Stream: true, Messages: messages},
		ResponseSubject: subject,
		Cursor:          cursor,
		PrefixHash:      prefixHash,
		Raw:             raw,
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	envelope, err := json.Marshal(conversation.AssignmentEnvelope{
		AssignmentID: "assign-" + requestID,
		RequestID:    requestID,
		Payload:      body,
	})
	require.NoError(t, err)
	return envelope
}

func runHandleMessage(srv *Server, data []byte) {
	srv.handleMessage(&nats.Msg{Subject: "in.test", Data: data})
}

// histogramSampleCount returns the number of observations of the first
// histogram in the collector.
func histogramSampleCount(t *testing.T, collector prometheus.Collector) uint64 {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(collector))
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if histogram := metric.GetHistogram(); histogram != nil {
				return histogram.GetSampleCount()
			}
		}
	}
	return 0
}

// runCacheAccounting drives a conversation where each request extends the
// history by two messages (assistant turn + new user turn).
func runCacheAccounting(t *testing.T, requests int) {
	t.Helper()
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"ok"}}]}`}}
	srv, _ := newSessionTestServer(t, sidecar)

	missBefore := promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("miss"))
	hitBefore := promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("hit"))

	messages := make([]openai.ChatMessage, 0, requests*2)
	prevCount := 0
	prevHash := ""
	for step := 0; step < requests; step++ {
		if step > 0 {
			messages = append(messages, openai.ChatMessage{Role: "assistant", Content: fmt.Sprintf("a%d", step-1)})
		}
		messages = append(messages, openai.ChatMessage{Role: "user", Content: fmt.Sprintf("u%d", step)})

		history := append([]openai.ChatMessage(nil), messages...)
		raw := marshalHistory(t, history)
		currentHash, err := conversation.PrefixHash(raw, len(history))
		require.NoError(t, err)

		payload := buildSessionPayload(t, fmt.Sprintf("req-%d", step), "out.test", history, prevCount, prevHash)
		runHandleMessage(srv, payload)

		prevCount = len(history)
		prevHash = currentHash
	}

	require.Equal(t, requests, sidecar.callCount(), "each request must reach the sidecar once")
	for i := 0; i < requests; i++ {
		var body openai.ChatCompletionRequest
		require.NoError(t, json.Unmarshal(sidecar.body(i), &body))
		require.Len(t, body.Messages, 2*i+1, "request %d must forward its full history", i)
	}

	var second openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(sidecar.body(1), &second))
	assert.Equal(t, "user", second.Messages[0].Role)
	assert.Equal(t, "assistant", second.Messages[1].Role, "assistant turns must be preserved")

	assert.Equal(t, missBefore+1, promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("miss")))
	assert.Equal(t, hitBefore+float64(requests-1), promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("hit")))
}

func TestCacheAccounting3(t *testing.T) {
	runCacheAccounting(t, 3)
}

func TestCacheAccounting10(t *testing.T) {
	runCacheAccounting(t, 10)
}

func TestColdStartOnInstanceChange(t *testing.T) {
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"ok"}}]}`}}
	srv, _ := newSessionTestServer(t, sidecar)

	messages := []openai.ChatMessage{
		{Role: "user", Content: "u0"},
		{Role: "assistant", Content: "a0"},
		{Role: "user", Content: "u1"},
		{Role: "assistant", Content: "a1"},
		{Role: "user", Content: "u2"},
		{Role: "user", Content: "u3"},
	}
	raw := marshalHistory(t, messages)
	prefix, err := conversation.PrefixHash(raw, 5)
	require.NoError(t, err)

	coldBefore := promtestutil.ToFloat64(metrics.ColdStartsTotal.WithLabelValues("instance_change"))
	histogramBefore := histogramSampleCount(t, metrics.ColdStartSeconds)

	runHandleMessage(srv, buildSessionPayload(t, "req-cold", "out.test", messages, 5, prefix))

	assert.Equal(t, coldBefore+1, promtestutil.ToFloat64(metrics.ColdStartsTotal.WithLabelValues("instance_change")))
	assert.Equal(t, histogramBefore+1, histogramSampleCount(t, metrics.ColdStartSeconds))
	require.Equal(t, 1, sidecar.callCount())

	var body openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(sidecar.body(0), &body))
	assert.Len(t, body.Messages, 6, "cold start replays the full client history")
}

func TestTruncationNewMessageOnly(t *testing.T) {
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"ok"}}]}`}}
	srv, _ := newSessionTestServer(t, sidecar)

	messages := []openai.ChatMessage{{Role: "user", Content: "only new message"}}
	truncBefore := promtestutil.ToFloat64(metrics.ContextTruncationsTotal.WithLabelValues("new_message_only"))
	resultBefore := promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("truncated"))

	runHandleMessage(srv, buildSessionPayload(t, "req-trunc", "out.test", messages, 5, "stale"))

	assert.Equal(t, truncBefore+1, promtestutil.ToFloat64(metrics.ContextTruncationsTotal.WithLabelValues("new_message_only")))
	assert.Equal(t, resultBefore+1, promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("truncated")))
	require.Equal(t, 1, sidecar.callCount())

	var body openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(sidecar.body(0), &body))
	assert.Len(t, body.Messages, 1, "only the new message must be forwarded")
}

func TestDedupSameRequestID(t *testing.T) {
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"unique"}}]}`}}
	srv, nc := newSessionTestServer(t, sidecar)

	messages := []openai.ChatMessage{{Role: "user", Content: "hello"}}
	envelope := buildSessionPayload(t, "req-dedup", "out.test", messages, 0, "")

	replayedBefore := promtestutil.ToFloat64(metrics.LLMDedupTotal.WithLabelValues("replayed"))

	runHandleMessage(srv, envelope)
	runHandleMessage(srv, envelope)

	assert.Equal(t, 1, sidecar.callCount(), "a duplicate request must not reach the sidecar")
	assert.Equal(t, replayedBefore+1, promtestutil.ToFloat64(metrics.LLMDedupTotal.WithLabelValues("replayed")))

	replayedChunks := 0
	doneMarkers := 0
	nc.Published = append([]testhelpers.PublishedMessage(nil), nc.Published...)
	for _, message := range nc.Published {
		if message.Subject != "out.test" {
			continue
		}
		if strings.Contains(string(message.Data), "unique") {
			replayedChunks++
		}
		if string(message.Data) == "[DONE]" {
			doneMarkers++
		}
	}
	assert.Equal(t, 2, replayedChunks, "the stored response must be replayed")
	assert.Equal(t, 2, doneMarkers, "each response ends with [DONE]")
}

func TestDedupDifferentSubjectSkips(t *testing.T) {
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"unique"}}]}`}}
	srv, nc := newSessionTestServer(t, sidecar)

	messages := []openai.ChatMessage{{Role: "user", Content: "hello"}}
	runHandleMessage(srv, buildSessionPayload(t, "req-dedup-2", "out.first", messages, 0, ""))
	runHandleMessage(srv, buildSessionPayload(t, "req-dedup-2", "out.second", messages, 0, ""))

	assert.Equal(t, 1, sidecar.callCount())

	for _, message := range nc.Published {
		assert.NotEqual(t, "out.second", message.Subject, "different subjects must not receive a replay")
	}
	skipped := promtestutil.ToFloat64(metrics.LLMDedupTotal.WithLabelValues("skipped"))
	assert.GreaterOrEqual(t, skipped, float64(1))
}

func TestAbortPreTokenRetries(t *testing.T) {
	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := &recordingSidecar{status: http.StatusInternalServerError}
	ts := testhelpers.NewHTTPServer(t, http.HandlerFunc(sidecar.handle))
	t.Cleanup(ts.Close)

	srv := &Server{
		cfg: Config{
			DllamaName:   "worker-1",
			SidecarURL:   ts.URL,
			MaxSeqLen:    4096,
			StateSubject: "state.test",
		},
		nc:           nc,
		client:       ts.Client(),
		outSubject:   "out.test",
		stateSubject: "state.test",
		log:          discardLogger(),
	}
	srv.initSessionState()

	ackSub, err := nc.SubscribeSync("_INBOX.ack")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	t.Cleanup(func() { _ = ackSub.Unsubscribe() })

	preTokenBefore := promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("pre_token"))
	envelope := buildSessionPayload(t, "req-pre", "out.test", []openai.ChatMessage{{Role: "user", Content: "hi"}}, 0, "")

	handle := func() {
		msg := &nats.Msg{Subject: "in.test", Reply: "_INBOX.ack", Sub: ackSub, Data: envelope}
		srv.handleMessage(msg)
		ack, ackErr := ackSub.NextMsg(time.Second)
		require.NoError(t, ackErr)
		require.Equal(t, "+ACK", string(ack.Data), "pre-token failures must be acked (state=error drives the retry)")
	}

	stateSub, err := nc.SubscribeSync("state.test")
	require.NoError(t, err)
	outSub, err := nc.SubscribeSync("out.test")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	handle()
	handle()

	assert.Equal(t, 2, sidecar.callCount(), "pre-token failures must stay retryable")
	assert.Equal(t, preTokenBefore+2, promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("pre_token")))

	var lastState conversation.WorkerStateEvent
	for {
		msg, nextErr := stateSub.NextMsg(50 * time.Millisecond)
		if nextErr != nil {
			break
		}
		require.NoError(t, json.Unmarshal(msg.Data, &lastState))
	}
	assert.Equal(t, "error", lastState.State, "the dispatcher requeues on state=error")

	_, err = outSub.NextMsg(100 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout, "pre-token failures must not publish partial output")
}

func TestAbortPostTokenNoRetry(t *testing.T) {
	reader := &failingStreamReader{}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(reader),
		Header:     make(http.Header),
	}
	resp.Header.Set("Content-Type", "text/event-stream")

	nc := &testhelpers.RecordingNATSConn{}
	srv := &Server{
		cfg:        Config{DllamaName: "worker-1", MaxSeqLen: 4096},
		nc:         nc,
		client:     &http.Client{Transport: staticResponseTransport{resp: resp}},
		outSubject: "out.test",
		log:        discardLogger(),
	}
	srv.initSessionState()

	postTokenBefore := promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("post_token"))
	envelope := buildSessionPayload(t, "req-post", "out.test", []openai.ChatMessage{{Role: "user", Content: "hi"}}, 0, "")

	runHandleMessage(srv, envelope)

	assert.Equal(t, postTokenBefore+1, promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("post_token")))
	require.Len(t, nc.Published, 3, "content, error chunk and [DONE]")
	assert.Contains(t, string(nc.Published[0].Data), "hi-token")

	var interrupted map[string]any
	require.NoError(t, json.Unmarshal(nc.Published[1].Data, &interrupted))
	errBody, ok := interrupted["error"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "stream_interrupted", errBody["code"])
	assert.Equal(t, "[DONE]", string(nc.Published[2].Data))

	// The completed entry replays instead of retrying the sidecar.
	runHandleMessage(srv, envelope)
	assert.Len(t, nc.Published, 6, "duplicate must replay the partial stream")
	assert.Equal(t, postTokenBefore+1, promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("post_token")))
}

func TestHeartbeatKeepsBusy(t *testing.T) {
	nc := &testhelpers.RecordingNATSConn{}
	srv := &Server{
		cfg: Config{
			DllamaName:        "worker-1",
			StateSubject:      "state.test",
			HeartbeatInterval: time.Hour,
		},
		nc:           nc,
		stateSubject: "state.test",
		log:          discardLogger(),
	}

	srv.inFlight.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	srv.startHeartbeatLoop(ctx)
	cancel()
	srv.wg.Wait()

	require.Len(t, nc.Published, 1)
	var busy conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(nc.Published[0].Data, &busy))
	assert.Equal(t, "busy", busy.State)
	assert.Equal(t, int32(1), busy.Active)

	srv.inFlight.Store(false)
	ctx, cancel = context.WithCancel(context.Background())
	srv.startHeartbeatLoop(ctx)
	cancel()
	srv.wg.Wait()

	require.Len(t, nc.Published, 2)
	var idle conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(nc.Published[1].Data, &idle))
	assert.Equal(t, "idle", idle.State)
	assert.Equal(t, int32(0), idle.Active)
}

func buildRawSessionPayload(t *testing.T, requestID, subject string, raw []byte, cursor int, prefixHash string) []byte {
	t.Helper()
	var decoded openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(raw, &decoded))
	payload := inboundRequest{
		Hash:            "conv-1",
		ChatID:          "conv-1",
		RequestID:       requestID,
		Request:         decoded,
		ResponseSubject: subject,
		Cursor:          cursor,
		PrefixHash:      prefixHash,
		Raw:             raw,
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	envelope, err := json.Marshal(conversation.AssignmentEnvelope{
		AssignmentID: "assign-" + requestID,
		RequestID:    requestID,
		Payload:      body,
	})
	require.NoError(t, err)
	return envelope
}

// TestCanonicalPrefixHitWithEscapedHistory proves that hashing the canonical
// bytes (compact + HTML-escaped) makes differently formatted but identical
// histories share a prefix hash, so the second request is a cache hit.
func TestCanonicalPrefixHitWithEscapedHistory(t *testing.T) {
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"ok"}}]}`}}
	srv, _ := newSessionTestServer(t, sidecar)

	pretty := []byte(`{
  "model": "test-model",
  "stream": true,
  "messages": [
    {"role": "user", "content": "a & b <tag> > done"}
  ]
}`)
	extended := []byte(`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"a & b <tag> > done"},{"role":"assistant","content":"ok"},{"role":"user","content":"next"}]}`)

	raw1 := conversation.CanonicalMessages(pretty)
	raw2 := conversation.CanonicalMessages(extended)
	require.NotContains(t, string(raw1), "\n", "canonical history must be compact")

	prefix1, err := conversation.PrefixHash(raw1, 1)
	require.NoError(t, err)
	prefix2, err := conversation.PrefixHash(raw2, 1)
	require.NoError(t, err)
	require.Equal(t, prefix1, prefix2, "canonical prefix hashes must agree across formatting")

	hitBefore := promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("hit"))

	runHandleMessage(srv, buildRawSessionPayload(t, "req-canon-1", "out.test", raw1, 0, ""))
	runHandleMessage(srv, buildRawSessionPayload(t, "req-canon-2", "out.test", raw2, 1, prefix1))

	assert.Equal(t, hitBefore+1, promtestutil.ToFloat64(metrics.SessionMessagesSentTotal.WithLabelValues("hit")))
	require.Equal(t, 2, sidecar.callCount())
	assert.Equal(t, raw2, sidecar.body(1), "the worker must forward the canonical bytes it hashed")
}

type metaThenFailReader struct {
	sent bool
}

func (r *metaThenFailReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		data := []byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		copy(p, data)
		return len(data), nil
	}
	return 0, errors.New("stream read failure")
}

func TestMetaChunkDoesNotCommitStream(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(&metaThenFailReader{}),
		Header:     make(http.Header),
	}
	resp.Header.Set("Content-Type", "text/event-stream")

	nc := &testhelpers.RecordingNATSConn{}
	srv := &Server{
		cfg:        Config{DllamaName: "worker-1", MaxSeqLen: 4096},
		nc:         nc,
		client:     &http.Client{Transport: staticResponseTransport{resp: resp}},
		outSubject: "out.test",
		log:        discardLogger(),
	}
	srv.initSessionState()

	preTokenBefore := promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("pre_token"))
	recorder := &chunkRecorder{}
	err := srv.streamToSidecar(inboundRequest{
		ResponseSubject: "out.test",
		Request:         openai.ChatCompletionRequest{Stream: true},
	}, recorder)

	require.Error(t, err)
	require.NotErrorIs(t, err, errStreamCommitted, "a role-only chunk must not commit the stream")
	require.False(t, recorder.hasToken())
	require.Len(t, nc.Published, 1, "only the role chunk was published")
	assert.Equal(t, preTokenBefore+1, promtestutil.ToFloat64(metrics.LLMStreamAbortsTotal.WithLabelValues("pre_token")))
}

func TestDedupReplayPublishesIdleState(t *testing.T) {
	sidecar := &recordingSidecar{chunks: []string{`{"choices":[{"delta":{"content":"ok"}}]}`}}
	srv, nc := newSessionTestServer(t, sidecar)

	envelope := buildSessionPayload(t, "req-dedup-state", "out.test", []openai.ChatMessage{{Role: "user", Content: "hello"}}, 0, "")
	runHandleMessage(srv, envelope)
	runHandleMessage(srv, envelope)

	require.Equal(t, 1, sidecar.callCount())

	busy := 0
	idle := 0
	for _, message := range nc.Published {
		if message.Subject != "state.test" {
			continue
		}
		var event conversation.WorkerStateEvent
		require.NoError(t, json.Unmarshal(message.Data, &event))
		switch event.State {
		case "busy":
			busy++
		case "idle":
			idle++
		}
	}
	assert.Equal(t, 1, busy)
	assert.Equal(t, 2, idle, "duplicate must publish idle so the dispatcher clears inflight")
}
