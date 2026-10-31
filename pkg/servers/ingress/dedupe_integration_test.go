package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/registry"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// syncRecorder is a concurrency-safe ResponseWriter for handlers whose output
// is inspected while the request is still streaming.
type syncRecorder struct {
	mu      sync.Mutex
	header  http.Header
	body    strings.Builder
	status  int
	flushes int
	notify  chan struct{}
}

func newSyncRecorder() *syncRecorder {
	return &syncRecorder{header: make(http.Header), notify: make(chan struct{}, 1)}
}

func (r *syncRecorder) Header() http.Header {
	return r.header
}

func (r *syncRecorder) WriteHeader(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = status
	}
}

func (r *syncRecorder) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(data)
}

func (r *syncRecorder) Flush() {
	r.mu.Lock()
	r.flushes++
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *syncRecorder) bodyString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

func (r *syncRecorder) statusCode() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

type dedupeHarness struct {
	t              *testing.T
	srv            *Server
	backlogSub     *nats.Subscription
	pub            *nats.Conn
	hash           string
	backlogSubject string
}

const dedupeTestModel = "tenant/chat-1"

func newDedupeHarness(t *testing.T) *dedupeHarness {
	t.Helper()
	return newDedupeHarnessWithConfig(t, nil)
}

func newDedupeHarnessWithConfig(t *testing.T, mutate func(*Config)) *dedupeHarness {
	t.Helper()

	ns := startIngressJetStream(t)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := Config{
		ListenAddress:   "127.0.0.1:0",
		Namespace:       "tenant",
		RootImage:       "test/root:latest",
		WorkerImage:     "test/worker:latest",
		NATSURL:         ns.ClientURL(),
		AllowAnonymous:  true,
		ResponseTimeout: 5 * time.Second,
		Logger:          logrus.NewEntry(logger),
	}
	if mutate != nil {
		mutate(&cfg)
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.raw.Drain()
		srv.raw.Close()
	})

	modelPayload, err := json.Marshal(registry.Model{
		Namespace:           cfg.Namespace,
		Name:                "chat-1",
		OutputPVCName:       "models-pvc",
		ConversionSizeHuman: "1Gi",
	})
	require.NoError(t, err)
	_, err = srv.modelsKV.Put(srv.modelKey(cfg.Namespace, "chat-1"), modelPayload)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("User-Agent", "ingress-test")
	hash, err := conversationHashFromHeadersWithPrincipal(req, dedupeTestModel, "", nil)
	require.NoError(t, err)

	backlogConn, err := nats.Connect(cfg.NATSURL, nats.Timeout(time.Second))
	require.NoError(t, err)
	t.Cleanup(backlogConn.Close)

	backlogSubject := sessionBacklogSubject(hash)
	sub, err := backlogConn.SubscribeSync(backlogSubject)
	require.NoError(t, err)
	require.NoError(t, backlogConn.Flush())
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	pub, err := nats.Connect(cfg.NATSURL, nats.Timeout(time.Second))
	require.NoError(t, err)
	t.Cleanup(pub.Close)

	h := &dedupeHarness{
		t:              t,
		srv:            srv,
		backlogSub:     sub,
		pub:            pub,
		hash:           hash,
		backlogSubject: backlogSubject,
	}

	// Keep the owner's session load above one so the handler does not wait for
	// a worker state event in tests where no worker exists.
	require.Equal(t, int32(1), srv.incrementSessionLoad(hash))
	t.Cleanup(func() { srv.decrementSessionLoad(hash) })

	return h
}

func (h *dedupeHarness) request(body string, ctx context.Context) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ingress-test")
	return req
}

func (h *dedupeHarness) nextBacklog(timeout time.Duration) (*conversation.BacklogMessage, string) {
	h.t.Helper()

	msg, err := h.backlogSub.NextMsg(timeout)
	require.NoError(h.t, err)

	var backlog conversation.BacklogMessage
	require.NoError(h.t, json.Unmarshal(msg.Data, &backlog))

	var payload struct {
		ResponseSubject string `json:"responseSubject"`
	}
	require.NoError(h.t, json.Unmarshal(backlog.Payload, &payload))
	require.NotEmpty(h.t, payload.ResponseSubject)
	return &backlog, payload.ResponseSubject
}

func (h *dedupeHarness) publish(subject string, payloads ...string) {
	h.t.Helper()
	for _, payload := range payloads {
		require.NoError(h.t, h.pub.Publish(subject, []byte(payload)))
	}
	require.NoError(h.t, h.pub.Flush())
}

func (h *dedupeHarness) requireNoBacklog(timeout time.Duration) {
	h.t.Helper()
	_, err := h.backlogSub.NextMsg(timeout)
	require.True(h.t, errors.Is(err, nats.ErrTimeout), "unexpected backlog message: %v", err)
}

func (h *dedupeHarness) dedupeKey(body string) string {
	h.t.Helper()
	canonical := conversation.CanonicalMessages([]byte(body))
	key, err := requestDedupeKey(h.hash, canonical)
	require.NoError(h.t, err)
	return key
}

func (h *dedupeHarness) waitForCached(key string) {
	h.t.Helper()

	coord := h.srv.dedupeCoordinator()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		coord.mu.Lock()
		entry, ok := coord.entries[key]
		cached := ok && entry.state == dedupeStateCached && entry.terminal
		coord.mu.Unlock()
		if cached {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatal("dedupe entry was not cached in time")
}

// waitForWaiters replaces timing sleeps: the test proceeds only once the
// expected number of requests have actually joined the in-flight entry.
func (h *dedupeHarness) waitForWaiters(key string, want int) {
	h.t.Helper()

	coord := h.srv.dedupeCoordinator()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		coord.mu.Lock()
		entry, ok := coord.entries[key]
		waiters := 0
		if ok {
			waiters = len(entry.waiters)
		}
		coord.mu.Unlock()
		if waiters == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	h.t.Fatalf("dedupe entry never reached %d waiters", want)
}

func ssePayloads(body string) []string {
	var payloads []string
	for _, chunk := range strings.Split(strings.TrimSpace(body), "\n\n") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" || strings.HasPrefix(chunk, ":") {
			continue
		}
		payloads = append(payloads, strings.TrimPrefix(chunk, "data: "))
	}
	return payloads
}

func TestDedupeIntegrationNonStreamConcurrentRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`

	recorders := []*syncRecorder{newSyncRecorder(), newSyncRecorder()}
	var wg sync.WaitGroup
	for _, rec := range recorders {
		rec := rec
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.srv.handleChatCompletions(rec, h.request(body, context.Background()))
		}()
	}

	_, responseSubject := h.nextBacklog(5 * time.Second)
	h.waitForWaiters(h.dedupeKey(body), 2)

	response := `{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"pong"}}]}`
	h.publish(responseSubject, response)
	wg.Wait()

	require.Equal(t, http.StatusOK, recorders[0].statusCode())
	require.Equal(t, http.StatusOK, recorders[1].statusCode())
	require.Equal(t, response, recorders[0].bodyString())
	require.Equal(t, response, recorders[1].bodyString())
	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationStreamRetryMidStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}],"stream":true}`

	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(body, context.Background()))
	}()

	_, responseSubject := h.nextBacklog(5 * time.Second)
	h.publish(responseSubject, `{"choices":[{"delta":{"role":"assistant","content":"one"}}]}`)

	// Wait until the owner's waiter has observed the first chunk.
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(first.bodyString(), "one") {
		require.True(t, time.Now().Before(deadline), "first chunk was not streamed")
		time.Sleep(5 * time.Millisecond)
	}

	second := newSyncRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		h.srv.handleChatCompletions(second, h.request(body, context.Background()))
	}()
	h.waitForWaiters(h.dedupeKey(body), 2)

	h.publish(responseSubject,
		`{"choices":[{"delta":{"content":"two"}}]}`,
		"[DONE]",
	)
	<-firstDone
	<-secondDone

	firstPayloads := ssePayloads(first.bodyString())
	require.Len(t, firstPayloads, 3)
	require.Contains(t, firstPayloads[0], `"one"`)
	require.Contains(t, firstPayloads[1], `"two"`)
	require.Equal(t, "[DONE]", firstPayloads[2])

	secondPayloads := ssePayloads(second.bodyString())
	require.Len(t, secondPayloads, 3)
	require.Contains(t, secondPayloads[0], `"one"`, "retry must replay the prefix")
	require.Contains(t, secondPayloads[1], `"two"`)
	require.Equal(t, "[DONE]", secondPayloads[2])

	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationCompletedCacheReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`
	response := `{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"pong"}}]}`

	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(body, context.Background()))
	}()
	_, responseSubject := h.nextBacklog(5 * time.Second)
	h.publish(responseSubject, response)
	<-firstDone

	require.Equal(t, http.StatusOK, first.statusCode())
	require.Equal(t, response, first.bodyString())

	// The retry is a cache hit: byte-identical body, no second backlog.
	second := newSyncRecorder()
	h.srv.handleChatCompletions(second, h.request(body, context.Background()))
	require.Equal(t, http.StatusOK, second.statusCode())
	require.Equal(t, first.bodyString(), second.bodyString())
	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationOwnerDisconnectStillCaches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}],"stream":true}`

	ctx, cancel := context.WithCancel(context.Background())
	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(body, ctx))
	}()

	_, responseSubject := h.nextBacklog(5 * time.Second)
	cancel()
	<-firstDone

	h.publish(responseSubject,
		`{"choices":[{"delta":{"role":"assistant","content":"one"}}]}`,
		`{"choices":[{"delta":{"content":"two"}}]}`,
		"[DONE]",
	)

	h.waitForCached(h.dedupeKey(body))

	second := newSyncRecorder()
	h.srv.handleChatCompletions(second, h.request(body, context.Background()))

	payloads := ssePayloads(second.bodyString())
	require.Len(t, payloads, 3)
	require.Contains(t, payloads[0], `"one"`)
	require.Contains(t, payloads[1], `"two"`)
	require.Equal(t, "[DONE]", payloads[2])
	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationDifferentNextTurnMisses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	firstBody := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"one"}]}`
	secondBody := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"one"},{"role":"assistant","content":"resp"},{"role":"user","content":"two"}]}`

	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(firstBody, context.Background()))
	}()
	_, firstSubject := h.nextBacklog(5 * time.Second)
	firstResponse := `{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"resp"}}]}`
	h.publish(firstSubject, firstResponse)
	<-firstDone

	second := newSyncRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		h.srv.handleChatCompletions(second, h.request(secondBody, context.Background()))
	}()
	_, secondSubject := h.nextBacklog(5 * time.Second)
	require.NotEqual(t, firstSubject, secondSubject)
	secondResponse := `{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"next"}}]}`
	h.publish(secondSubject, secondResponse)
	<-secondDone

	require.Equal(t, firstResponse, first.bodyString())
	require.Equal(t, secondResponse, second.bodyString())
	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationStreamErrorNotCached(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}],"stream":true}`

	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(body, context.Background()))
	}()
	_, responseSubject := h.nextBacklog(5 * time.Second)
	h.publish(responseSubject,
		`{"choices":[{"delta":{"role":"assistant","content":"partial"}}]}`,
		`{"error":{"message":"upstream stream interrupted","type":"server_error","code":"stream_interrupted"}}`,
	)
	<-firstDone

	firstPayloads := ssePayloads(first.bodyString())
	require.Len(t, firstPayloads, 3)
	require.Contains(t, firstPayloads[0], `"partial"`)
	require.Contains(t, firstPayloads[1], `"stream_interrupted"`)
	require.Equal(t, "[DONE]", firstPayloads[2])

	// The error result must not be cached: the retry enqueues a new request.
	second := newSyncRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		h.srv.handleChatCompletions(second, h.request(body, context.Background()))
	}()
	_, retrySubject := h.nextBacklog(5 * time.Second)
	h.publish(retrySubject, "[DONE]")
	<-secondDone

	require.Equal(t, []string{"[DONE]"}, ssePayloads(second.bodyString()))
	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationJSONErrorNotCached(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarness(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`
	errorResponse := `{"error":{"message":"inference failed","type":"server_error","code":"inference_failed"}}`

	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(body, context.Background()))
	}()
	_, responseSubject := h.nextBacklog(5 * time.Second)
	h.publish(responseSubject, errorResponse)
	<-firstDone

	require.Equal(t, http.StatusBadGateway, first.statusCode())
	require.Equal(t, errorResponse, first.bodyString())

	// No sticky failure: the retry enqueues again and can succeed.
	second := newSyncRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		h.srv.handleChatCompletions(second, h.request(body, context.Background()))
	}()
	_, retrySubject := h.nextBacklog(5 * time.Second)
	success := `{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`
	h.publish(retrySubject, success)
	<-secondDone

	require.Equal(t, http.StatusOK, second.statusCode())
	require.Equal(t, success, second.bodyString())
	h.requireNoBacklog(300 * time.Millisecond)
}

func TestDedupeIntegrationTTLExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	h := newDedupeHarnessWithConfig(t, func(cfg *Config) {
		cfg.DedupeTTL = 50 * time.Millisecond
	})
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`
	response := `{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"pong"}}]}`

	first := newSyncRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.srv.handleChatCompletions(first, h.request(body, context.Background()))
	}()
	_, responseSubject := h.nextBacklog(5 * time.Second)
	h.publish(responseSubject, response)
	<-firstDone

	time.Sleep(80 * time.Millisecond)

	second := newSyncRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		h.srv.handleChatCompletions(second, h.request(body, context.Background()))
	}()
	_, retrySubject := h.nextBacklog(5 * time.Second)
	h.publish(retrySubject, response)
	<-secondDone

	require.Equal(t, http.StatusOK, second.statusCode())
	require.Equal(t, response, second.bodyString())
	h.requireNoBacklog(300 * time.Millisecond)
}
