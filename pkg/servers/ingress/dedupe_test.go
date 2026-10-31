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

func TestRequestDigestStableAcrossTopLevelKeyOrder(t *testing.T) {
	t.Parallel()

	first := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.2}`)
	second := []byte(`{"temperature":0.2,"messages":[{"role":"user","content":"hi"}],"model":"m"}`)

	firstDigest, err := requestDigest(first)
	require.NoError(t, err)
	secondDigest, err := requestDigest(second)
	require.NoError(t, err)
	require.Equal(t, firstDigest, secondDigest)
}

func TestRequestDigestSensitiveToRequestSemantics(t *testing.T) {
	t.Parallel()

	base := `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.2,"max_tokens":16,"stream":false}`
	baseDigest, err := requestDigest([]byte(base))
	require.NoError(t, err)

	cases := map[string]string{
		"messages":    `{"model":"m","messages":[{"role":"user","content":"bye"}],"temperature":0.2,"max_tokens":16,"stream":false}`,
		"temperature": `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.9,"max_tokens":16,"stream":false}`,
		"max_tokens":  `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.2,"max_tokens":32,"stream":false}`,
		"tools":       `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.2,"max_tokens":16,"stream":false,"tools":[{"type":"function","function":{"name":"lookup"}}]}`,
		"stream":      `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.2,"max_tokens":16,"stream":true}`,
		"model":       `{"model":"other","messages":[{"role":"user","content":"hi"}],"temperature":0.2,"max_tokens":16,"stream":false}`,
	}

	for name, body := range cases {
		name, body := name, body
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			digest, err := requestDigest([]byte(body))
			require.NoError(t, err)
			require.NotEqual(t, baseDigest, digest)
		})
	}
}

func TestRequestDigestIgnoresVolatileTopLevelKeys(t *testing.T) {
	t.Parallel()

	first := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"user":"alice","metadata":{"a":"1"},"request_id":"r1","trace_id":"t1","session_id":"s1"}`)
	second := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"user":"bob","metadata":{"a":"2"},"request_id":"r2","trace_id":"t2","session_id":"s2"}`)

	firstDigest, err := requestDigest(first)
	require.NoError(t, err)
	secondDigest, err := requestDigest(second)
	require.NoError(t, err)
	require.Equal(t, firstDigest, secondDigest)
}

func TestRequestDedupeKeySeparatesConversations(t *testing.T) {
	t.Parallel()

	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	first, err := requestDedupeKey("conversation-a", body)
	require.NoError(t, err)
	second, err := requestDedupeKey("conversation-b", body)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

// readDedupeEvents collects chunks until the entry terminates. Termination is
// driven by Done, never by a chunk-channel close, so a full buffer cannot hide
// the terminal state.
func readDedupeEvents(t *testing.T, handle *dedupeHandle, timeout time.Duration) ([]string, error) {
	t.Helper()

	var payloads []string
	deadline := time.After(timeout)
	for {
		select {
		case payload := <-handle.Events():
			payloads = append(payloads, payload)
		case <-handle.Done():
			for {
				select {
				case payload := <-handle.Events():
					payloads = append(payloads, payload)
				default:
					return payloads, handle.TerminalError()
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for dedupe events")
			return nil, nil
		}
	}
}

func TestDedupeOwnerAttachCompleteHit(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(0, 0, 0, 0)
	owner := coord.begin("owner-attach", true)
	require.Equal(t, dedupeMiss, owner.Kind())

	attach := coord.begin("owner-attach", true)
	require.Equal(t, dedupeAttach, attach.Kind())
	require.Empty(t, attach.Snapshot())

	owner.Publish("a")
	owner.Publish("b")
	owner.Complete()

	ownerPayloads, err := readDedupeEvents(t, owner, time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, ownerPayloads)

	attachPayloads, err := readDedupeEvents(t, attach, time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, attachPayloads)

	hit := coord.begin("owner-attach", true)
	require.Equal(t, dedupeCacheHit, hit.Kind())
	require.Equal(t, []string{"a", "b"}, hit.Snapshot())
}

func TestDedupeTTLExpiry(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(20*time.Millisecond, 16, 1<<20, 4)
	owner := coord.begin("ttl", true)
	owner.Publish("chunk")
	owner.Complete()

	hit := coord.begin("ttl", true)
	require.Equal(t, dedupeCacheHit, hit.Kind())

	time.Sleep(40 * time.Millisecond)

	miss := coord.begin("ttl", true)
	require.Equal(t, dedupeMiss, miss.Kind())
}

func TestDedupeLRUEviction(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 2, 1<<20, 4)
	for _, key := range []string{"lru-1", "lru-2"} {
		handle := coord.begin(key, false)
		handle.Publish(`{"object":"chat.completion"}`)
		handle.Complete()
	}

	// Touch lru-1 so lru-2 becomes the least recently used entry.
	require.Equal(t, dedupeCacheHit, coord.begin("lru-1", false).Kind())

	third := coord.begin("lru-3", false)
	third.Publish(`{"object":"chat.completion"}`)
	third.Complete()

	require.Equal(t, dedupeCacheHit, coord.begin("lru-1", false).Kind())
	require.Equal(t, dedupeMiss, coord.begin("lru-2", false).Kind())
	require.Equal(t, dedupeCacheHit, coord.begin("lru-3", false).Kind())
}

func TestDedupeByteBudgetEviction(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 10, 4)
	first := coord.begin("bytes-1", false)
	first.Publish("123456") // 6 bytes
	first.Complete()

	second := coord.begin("bytes-2", false)
	second.Publish("123456") // 6 more bytes, over the 10-byte budget
	second.Complete()

	require.Equal(t, dedupeCacheHit, coord.begin("bytes-2", false).Kind())
	require.Equal(t, dedupeMiss, coord.begin("bytes-1", false).Kind())
}

func TestDedupeWaiterCapRejects(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 1<<20, 2)
	owner := coord.begin("waiters", true)
	require.Equal(t, dedupeMiss, owner.Kind())
	require.Equal(t, dedupeAttach, coord.begin("waiters", true).Kind())

	rejected := coord.begin("waiters", true)
	require.Equal(t, dedupeRejected, rejected.Kind())
}

func TestDedupeTruncationRejectsAttachAndCache(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 1<<20, 4)
	owner := coord.begin("truncated", true)
	owner.Publish(strings.Repeat("x", dedupeStreamBufferBytes))
	owner.Publish("overflow")

	require.Equal(t, dedupeRejected, coord.begin("truncated", true).Kind())

	owner.Complete()
	require.Equal(t, dedupeMiss, coord.begin("truncated", true).Kind())
}

func TestDedupeReplayAndLiveOrderWithoutGaps(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 1<<20, 4)
	owner := coord.begin("fan-out", true)
	owner.Publish("p1")
	owner.Publish("p2")

	attach := coord.begin("fan-out", true)
	require.Equal(t, dedupeAttach, attach.Kind())
	require.Equal(t, []string{"p1", "p2"}, attach.Snapshot())

	owner.Publish("p3")
	owner.Publish("p4")
	owner.Complete()

	ownerPayloads, err := readDedupeEvents(t, owner, time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p3", "p4"}, ownerPayloads)

	attachPayloads, err := readDedupeEvents(t, attach, time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"p3", "p4"}, attachPayloads)
}

func TestDedupeSlowWaiterDroppedPublisherNonBlocking(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 1<<20, 4)
	owner := coord.begin("slow-waiter", true)
	slow := coord.begin("slow-waiter", true)

	// Drain the owner after every publish so only the slow waiter fills up.
	start := time.Now()
	ownerCount := 0
	for i := 0; i < 100; i++ {
		owner.Publish("chunk")
		select {
		case payload := <-owner.Events():
			require.Equal(t, "chunk", payload)
			ownerCount++
		case <-time.After(time.Second):
			t.Fatal("owner did not receive a published chunk")
		}
	}
	require.Less(t, time.Since(start), time.Second, "publish must never block on a slow waiter")
	require.Equal(t, 100, ownerCount)

	// The slow waiter was dropped once its 64-slot buffer filled; termination
	// is signalled separately so the handler can emit an explicit error.
	select {
	case <-slow.Dropped():
	default:
		t.Fatal("slow waiter was not dropped")
	}
	slowCount := 0
	for {
		select {
		case <-slow.Events():
			slowCount++
		default:
			require.Equal(t, dedupeWaiterBuffer, slowCount)
			owner.Complete()
			return
		}
	}
}

func TestPumpContinuesAfterOwnerDisconnectAndCaches(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	owner := srv.dedupeBegin("pump-cache", true)
	owner.Close() // HTTP client is gone; inference must continue.

	msgs := make(chan *nats.Msg, 3)
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.pumpResponse(pumpCtx, pumpCancel, owner, "hash", msgs, nil)
	}()

	msgs <- &nats.Msg{Data: []byte(`{"choices":[{"delta":{"role":"assistant","content":"a"}}]}`)}
	msgs <- &nats.Msg{Data: []byte(`{"choices":[{"delta":{"content":"b"}}]}`)}
	msgs <- &nats.Msg{Data: []byte("[DONE]")}
	<-done

	hit := srv.dedupeBegin("pump-cache", true)
	require.Equal(t, dedupeCacheHit, hit.Kind())
	require.Len(t, hit.Snapshot(), 2)
	require.Contains(t, hit.Snapshot()[0], `"content":"a"`)
	require.Contains(t, hit.Snapshot()[1], `"content":"b"`)
}

func TestDedupeRejectDoesNotGrowWaiterSet(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 1<<20, 1)
	owner := coord.begin("reject-map", true)
	require.Equal(t, dedupeRejected, coord.begin("reject-map", true).Kind())

	coord.mu.Lock()
	waiters := len(owner.entry.waiters)
	coord.mu.Unlock()
	require.Equal(t, 1, waiters)
}

type gatedFlushRecorder struct {
	*syncRecorder
	release chan struct{}
	first   chan struct{}
	once    sync.Once
}

func (g *gatedFlushRecorder) Flush() {
	g.once.Do(func() { close(g.first) })
	<-g.release
	g.syncRecorder.Flush()
}

// TestServeStreamSlowWriterStillTerminates reproduces the lossy-terminal bug:
// the handler is blocked in Flush while more than 64 chunks arrive, so its
// waiter buffer overflows. It must still emit an explicit error frame and
// [DONE] instead of ending a 200 SSE response silently.
func TestServeStreamSlowWriterStillTerminates(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("slow-terminal", true)
	rec := &gatedFlushRecorder{
		syncRecorder: newSyncRecorder(),
		release:      make(chan struct{}),
		first:        make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.serveStream(ctx, rec, handle)
	}()

	handle.Publish("chunk-0")
	<-rec.first // the handler is now blocked inside Flush

	for i := 0; i < 100; i++ {
		handle.Publish("chunk")
	}
	select {
	case <-handle.Dropped():
	case <-time.After(time.Second):
		t.Fatal("waiter was not dropped after the buffer overflow")
	}

	close(rec.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not terminate after the slow waiter was dropped")
	}

	body := strings.TrimSpace(rec.bodyString())
	require.True(t, strings.HasSuffix(body, "data: [DONE]"), "SSE response must end with [DONE]")
	require.Contains(t, body, "client too slow")
}

func TestPumpStreamFailureAfterPublish(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("failure-after-publish", true)
	msgs := make(chan *nats.Msg, 2)

	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.pumpResponse(pumpCtx, pumpCancel, handle, "hash", msgs, nil)
	}()

	msgs <- &nats.Msg{Data: []byte(`{"choices":[{"delta":{"role":"assistant","content":"partial"}}]}`)}
	msgs <- nil // subscription closed mid-stream
	<-done

	rec := newSyncRecorder()
	srv.serveStream(context.Background(), rec, handle)

	payloads := ssePayloads(rec.bodyString())
	require.Len(t, payloads, 3)
	require.Contains(t, payloads[0], `"partial"`)
	require.Contains(t, payloads[1], `"subscription closed"`)
	require.Equal(t, "[DONE]", payloads[2])
}

func TestPumpJSONSubscriptionClosedReturns504(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("json-closed", false)
	msgs := make(chan *nats.Msg, 1)

	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.pumpResponse(pumpCtx, pumpCancel, handle, "hash", msgs, nil)
	}()
	msgs <- nil
	<-done

	rec := newSyncRecorder()
	srv.serveJSON(context.Background(), rec, handle)
	require.Equal(t, http.StatusGatewayTimeout, rec.statusCode())
}

func TestPumpStreamErrorNotCached(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("stream-error", true)
	msgs := make(chan *nats.Msg, 1)

	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.pumpResponse(pumpCtx, pumpCancel, handle, "hash", msgs, nil)
	}()
	msgs <- &nats.Msg{Data: []byte(`{"error":{"message":"boom","code":"stream_interrupted"}}`)}
	<-done

	require.Equal(t, dedupeMiss, srv.dedupeBegin("stream-error", true).Kind())
}

func TestPumpJSONErrorNotCached(t *testing.T) {
	t.Parallel()

	srv := &Server{log: logrus.New().WithField("component", "test")}
	handle := srv.dedupeBegin("json-error", false)
	msgs := make(chan *nats.Msg, 1)

	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.pumpResponse(pumpCtx, pumpCancel, handle, "hash", msgs, nil)
	}()
	msgs <- &nats.Msg{Data: []byte(`{"error":{"message":"boom","code":"inference_failed"}}`)}
	<-done

	require.Equal(t, dedupeMiss, srv.dedupeBegin("json-error", false).Kind())
}

func TestDedupeInflightBudgetStopsBuffering(t *testing.T) {
	t.Parallel()

	coord := newDedupeCoordinator(time.Minute, 16, 4, 4)
	owner := coord.begin("inflight-budget", true)
	owner.Publish("12345") // exceeds the 4-byte global budget
	require.Equal(t, dedupeRejected, coord.begin("inflight-budget", true).Kind())
}

func TestRequestDigestRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := requestDigest([]byte("not-json"))
	require.Error(t, err)
}

func newDedupeHTTPTestServer(t *testing.T) *Server {
	t.Helper()

	return &Server{
		cfg: Config{
			AllowAnonymous:   true,
			Namespace:        "tenant",
			ResponseTimeout:  time.Second,
			MaxSeqLen:        4096,
			TTLPrefix:        "nats_ttl_",
			DedupeMaxWaiters: 1,
		},
		log: logrus.New().WithField("component", "test"),
		modelsKV: keyValueStub{
			getFn: func(string) (nats.KeyValueEntry, error) {
				payload, err := json.Marshal(registry.Model{
					Namespace:           "tenant",
					Name:                "chat-1",
					OutputPVCName:       "models-pvc",
					ConversionSizeHuman: "1Gi",
				})
				if err != nil {
					return nil, err
				}
				return &kvEntryStub{value: payload}, nil
			},
		},
	}
}

func newDedupeHTTPRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ingress-test")
	return req
}

func dedupeTestKey(t *testing.T, body string) string {
	t.Helper()

	req := newDedupeHTTPRequest(body)
	hash, err := conversationHashFromHeadersWithPrincipal(req, "tenant/chat-1", "", nil)
	require.NoError(t, err)
	key, err := requestDedupeKey(hash, conversation.CanonicalMessages([]byte(body)))
	require.NoError(t, err)
	return key
}

func TestDedupeWaiterCapRejectedOverHTTP(t *testing.T) {
	t.Parallel()

	srv := newDedupeHTTPTestServer(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`

	// The pre-created in-flight entry already holds its owner waiter, which is
	// the whole cap (DedupeMaxWaiters=1).
	_ = srv.dedupeBegin(dedupeTestKey(t, body), false)

	rec := httptest.NewRecorder()
	srv.handleChatCompletions(rec, newDedupeHTTPRequest(body))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
}

func TestDedupeTruncationRejectedOverHTTP(t *testing.T) {
	t.Parallel()

	srv := newDedupeHTTPTestServer(t)
	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}],"stream":true}`

	owner := srv.dedupeBegin(dedupeTestKey(t, body), true)
	owner.Publish(strings.Repeat("x", dedupeStreamBufferBytes+1))

	rec := httptest.NewRecorder()
	srv.handleChatCompletions(rec, newDedupeHTTPRequest(body))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
}

func TestHandleChatCompletionsDigestErrorReturns400(t *testing.T) {
	t.Parallel()

	srv := newDedupeHTTPTestServer(t)
	srv.dedupeKeyHook = func(string, []byte) (string, error) {
		return "", errors.New("digest failed")
	}

	body := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`
	rec := httptest.NewRecorder()
	srv.handleChatCompletions(rec, newDedupeHTTPRequest(body))

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "invalid chat completion payload")
}
