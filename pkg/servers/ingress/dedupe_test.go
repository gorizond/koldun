package ingress

import (
	"context"
	"strings"
	"testing"
	"time"

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

func readDedupeEvents(t *testing.T, handle *dedupeHandle, timeout time.Duration) ([]string, bool) {
	t.Helper()

	var payloads []string
	deadline := time.After(timeout)
	for {
		select {
		case event, ok := <-handle.Events():
			if !ok {
				return payloads, false
			}
			if event.terminal {
				return payloads, event.err == nil
			}
			payloads = append(payloads, event.payload)
		case <-deadline:
			t.Fatal("timed out waiting for dedupe events")
			return nil, false
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

	ownerPayloads, ok := readDedupeEvents(t, owner, time.Second)
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, ownerPayloads)

	attachPayloads, ok := readDedupeEvents(t, attach, time.Second)
	require.True(t, ok)
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

	ownerPayloads, ok := readDedupeEvents(t, owner, time.Second)
	require.True(t, ok)
	require.Equal(t, []string{"p1", "p2", "p3", "p4"}, ownerPayloads)

	attachPayloads, ok := readDedupeEvents(t, attach, time.Second)
	require.True(t, ok)
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
		case event := <-owner.Events():
			require.False(t, event.terminal)
			ownerCount++
		case <-time.After(time.Second):
			t.Fatal("owner did not receive a published chunk")
		}
	}
	require.Less(t, time.Since(start), time.Second, "publish must never block on a slow waiter")
	require.Equal(t, 100, ownerCount)

	owner.Complete()
	terminal := <-owner.Events()
	require.True(t, terminal.terminal)

	// The slow waiter was dropped after its 64-slot buffer filled; the close
	// is observable as a closed channel.
	slowCount := 0
	for {
		_, ok := <-slow.Events()
		if !ok {
			break
		}
		slowCount++
	}
	require.Equal(t, dedupeWaiterBuffer, slowCount)
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
