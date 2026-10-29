package dispatcher

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/gorizond/koldun/pkg/natsutil"
	testhelpers "github.com/gorizond/koldun/pkg/testutil"
	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// conversationKV is an in-memory conversation.KV with revision checks.
type conversationKV struct {
	mu       sync.Mutex
	data     []byte
	revision uint64
	exists   bool
}

func (kv *conversationKV) Get(string) (nats.KeyValueEntry, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.exists {
		return nil, nats.ErrKeyNotFound
	}
	return &conversationEntry{value: append([]byte(nil), kv.data...), revision: kv.revision}, nil
}

func (kv *conversationKV) Update(_ string, value []byte, revision uint64) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.exists || kv.revision != revision {
		return 0, nats.ErrKeyExists
	}
	kv.data = append([]byte(nil), value...)
	kv.revision++
	return kv.revision, nil
}

func (kv *conversationKV) record(t *testing.T) *conversation.Record {
	t.Helper()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	record, err := conversation.ParseRecord(kv.data)
	require.NoError(t, err)
	return record
}

type conversationEntry struct {
	nats.KeyValueEntry
	value    []byte
	revision uint64
}

func (e *conversationEntry) Value() []byte    { return e.value }
func (e *conversationEntry) Revision() uint64 { return e.revision }

func encodeRecord(t *testing.T, pin string) []byte {
	t.Helper()
	record := &conversation.Record{
		Hash:         "conv-1",
		Namespace:    "ns",
		Model:        "ns/model",
		RootImage:    "root:latest",
		WorkerImage:  "worker:latest",
		PinnedDllama: pin,
	}
	data, err := record.Marshal()
	require.NoError(t, err)
	return data
}

func newConversationKV(t *testing.T, pin string) *conversationKV {
	t.Helper()
	return &conversationKV{data: encodeRecord(t, pin), revision: 1, exists: true}
}

func idleWorker(name string) *workerState {
	return &workerState{name: name, state: "idle", active: 0, lastHeartbeat: time.Now()}
}

func newAffinityServer(t *testing.T, workers map[string]*workerState, pin string) *Server {
	t.Helper()
	return &Server{
		cfg: Config{
			Hash:                "conv-1",
			BacklogSubject:      "backlog",
			AssignmentsBucket:   "assignments",
			DllamaSubjectPrefix: "dllama.",
		},
		nc:            &testhelpers.RecordingNATSConn{},
		assignments:   &mockKeyValue{entries: map[string][]byte{}},
		conversations: newConversationKV(t, pin),
		workers:       workers,
		inflight:      make(map[string]*assignment),
		fallbacks:     make(map[string]int),
		retryConfig:   natsutil.DefaultRetryConfig(),
		log:           logrus.NewEntry(logrus.New()),
	}
}

func TestSelectWorkerForPinnedIdle(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-a": idleWorker("worker-a"),
		"worker-b": idleWorker("worker-b"),
	}, "worker-b")

	worker, outcome := srv.selectWorkerFor("worker-b")
	assert.Equal(t, "worker-b", worker)
	assert.Equal(t, "pinned", outcome)
}

func TestSelectWorkerForPinnedBusyFallback(t *testing.T) {
	busy := idleWorker("worker-a")
	busy.active = 1
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-a": busy,
		"worker-b": idleWorker("worker-b"),
	}, "worker-a")

	worker, outcome := srv.selectWorkerFor("worker-a")
	assert.Equal(t, "worker-b", worker)
	assert.Equal(t, "fallback", outcome)

	// Repeated fallbacks eventually re-pin to avoid permanent pinning of a
	// worker that keeps being busy.
	_, outcome = srv.selectWorkerFor("worker-a")
	assert.Equal(t, "fallback", outcome)
	worker, outcome = srv.selectWorkerFor("worker-a")
	assert.Equal(t, "repin", outcome)
	assert.Equal(t, "worker-b", worker)
}

func TestSelectWorkerForPinnedStaleRepin(t *testing.T) {
	stale := idleWorker("worker-a")
	stale.lastHeartbeat = time.Now().Add(-5 * time.Minute)
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-a": stale,
		"worker-b": idleWorker("worker-b"),
	}, "worker-a")

	worker, outcome := srv.selectWorkerFor("worker-a")
	assert.Equal(t, "worker-b", worker)
	assert.Equal(t, "repin", outcome)
}

func TestSelectWorkerForPinnedErrorRepin(t *testing.T) {
	failed := idleWorker("worker-a")
	failed.state = "error"
	failed.active = 1
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-a": failed,
		"worker-b": idleWorker("worker-b"),
	}, "worker-a")

	worker, outcome := srv.selectWorkerFor("worker-a")
	assert.Equal(t, "worker-b", worker)
	assert.Equal(t, "repin", outcome)
}

func TestSelectWorkerForNoPinDeterministic(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-b": idleWorker("worker-b"),
		"worker-a": idleWorker("worker-a"),
		"worker-c": idleWorker("worker-c"),
	}, "")

	first, outcome := srv.selectWorkerFor("")
	require.Equal(t, "no_pin", outcome)
	require.NotEmpty(t, first)

	for i := 0; i < 5; i++ {
		worker, outcome := srv.selectWorkerFor("")
		assert.Equal(t, "no_pin", outcome)
		assert.Equal(t, first, worker, "selection must be stable for a stable pool")
	}
}

func TestSelectWorkerForNoIdleWorkers(t *testing.T) {
	busy := idleWorker("worker-a")
	busy.active = 1
	srv := newAffinityServer(t, map[string]*workerState{"worker-a": busy}, "worker-a")

	worker, outcome := srv.selectWorkerFor("worker-a")
	assert.Empty(t, worker)
	assert.Equal(t, "no_idle", outcome)
}

func TestPinnedWorkerWithoutBucket(t *testing.T) {
	srv := &Server{cfg: Config{Hash: "conv-1"}}
	assert.Empty(t, srv.pinnedWorker())
}

func TestHandleBacklogPinsWorker(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-a": idleWorker("worker-a"),
	}, "")
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	backlog := conversation.BacklogMessage{ID: "req-1", Payload: json.RawMessage(`{"requestId":"req-1"}`)}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	srv.handleBacklog(&nats.Msg{Subject: "backlog", Data: data})

	require.NotEmpty(t, recorder.Published)
	assert.Equal(t, "dllama.worker-a.in", recorder.Published[0].Subject)
	assert.Equal(t, "worker-a", srv.conversations.(*conversationKV).record(t).PinnedDllama)
}

func TestHandleBacklogKeepsPinWhenPinnedBusy(t *testing.T) {
	busy := idleWorker("pinned")
	busy.active = 1
	srv := newAffinityServer(t, map[string]*workerState{
		"pinned": busy,
		"other":  idleWorker("other"),
	}, "pinned")
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	before := promtestutil.ToFloat64(metrics.DispatcherCacheEvictionsTotal.WithLabelValues("pinned_busy"))

	backlog := conversation.BacklogMessage{ID: "req-2", Payload: json.RawMessage(`{"requestId":"req-2"}`)}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	srv.handleBacklog(&nats.Msg{Subject: "backlog", Data: data})

	require.NotEmpty(t, recorder.Published)
	assert.Equal(t, "dllama.other.in", recorder.Published[0].Subject)
	assert.Equal(t, "pinned", srv.conversations.(*conversationKV).record(t).PinnedDllama, "fallback must keep the pin")
	assert.Equal(t, before+1, promtestutil.ToFloat64(metrics.DispatcherCacheEvictionsTotal.WithLabelValues("pinned_busy")))
}

func TestHandleBacklogRepinsWhenPinnedGone(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{
		"worker-a": idleWorker("worker-a"),
	}, "vanished")
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	before := promtestutil.ToFloat64(metrics.DispatcherCacheEvictionsTotal.WithLabelValues("pinned_gone"))

	backlog := conversation.BacklogMessage{ID: "req-3", Payload: json.RawMessage(`{"requestId":"req-3"}`)}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	srv.handleBacklog(&nats.Msg{Subject: "backlog", Data: data})

	require.NotEmpty(t, recorder.Published)
	assert.Equal(t, "dllama.worker-a.in", recorder.Published[0].Subject)
	assert.Equal(t, "worker-a", srv.conversations.(*conversationKV).record(t).PinnedDllama)
	assert.Equal(t, before+1, promtestutil.ToFloat64(metrics.DispatcherCacheEvictionsTotal.WithLabelValues("pinned_gone")))
}

func TestHandleBacklogRequeuesWithAttempts(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{}, "")
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	backlog := conversation.BacklogMessage{
		ID:       "req-retry",
		Payload:  json.RawMessage(`{"responseSubject":"out.req-retry"}`),
		Attempts: 0,
	}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	srv.handleBacklog(&nats.Msg{Subject: "backlog", Data: data})

	require.Len(t, recorder.Published, 1, "no worker means one requeue, not an error")
	next := recorder.Published[0]
	assert.Equal(t, "backlog", next.Subject)
	var requeued conversation.BacklogMessage
	require.NoError(t, json.Unmarshal(next.Data, &requeued))
	assert.Equal(t, 1, requeued.Attempts)
	assert.Equal(t, backlog.ID, requeued.ID)
}

func TestHandleBacklogFailsAfterMaxAttempts(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{}, "")
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	backlog := conversation.BacklogMessage{
		ID:       "req-dead",
		Payload:  json.RawMessage(`{"responseSubject":"out.req-dead"}`),
		Attempts: maxBacklogAttempts - 1,
	}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	srv.handleBacklog(&nats.Msg{Subject: "backlog", Data: data})

	require.Len(t, recorder.Published, 2, "terminal error and [DONE], no requeue")
	for _, message := range recorder.Published {
		assert.Equal(t, "out.req-dead", message.Subject)
	}
	var failure map[string]any
	require.NoError(t, json.Unmarshal(recorder.Published[0].Data, &failure))
	errBody, ok := failure["error"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "inference_failed", errBody["code"])
	assert.Contains(t, errBody["message"], "inference failed after 3 attempts")
	assert.Equal(t, "[DONE]", string(recorder.Published[1].Data))
}

func TestFinishAssignmentRequeueClearsPinAndIncrementsAttempts(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{}, "worker-a")
	srv.inflight["asn-1"] = &assignment{
		assignmentID: "asn-1",
		requestID:    "req-pin",
		worker:       "worker-a",
		payload:      []byte(`{"responseSubject":"out.req-pin"}`),
		attempts:     0,
	}
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	srv.finishAssignment("asn-1", true)

	assert.Empty(t, srv.conversations.(*conversationKV).record(t).PinnedDllama, "failed worker must lose the pin")
	require.Len(t, recorder.Published, 1)
	assert.Equal(t, "backlog", recorder.Published[0].Subject)
	var requeued conversation.BacklogMessage
	require.NoError(t, json.Unmarshal(recorder.Published[0].Data, &requeued))
	assert.Equal(t, 1, requeued.Attempts)

	srv.mu.Lock()
	inflight := len(srv.inflight)
	srv.mu.Unlock()
	assert.Zero(t, inflight)
}

func TestFinishAssignmentRequeueKeepsNewerPin(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{}, "worker-b")
	srv.inflight["asn-2"] = &assignment{
		assignmentID: "asn-2",
		requestID:    "req-pin-2",
		worker:       "worker-a",
		payload:      []byte(`{"responseSubject":"out.req-pin-2"}`),
	}

	srv.finishAssignment("asn-2", true)

	assert.Equal(t, "worker-b", srv.conversations.(*conversationKV).record(t).PinnedDllama,
		"clearing must not overwrite a pin written for another worker")
}

func TestHandleStateErrorSurfacesWorkerReason(t *testing.T) {
	srv := newAffinityServer(t, map[string]*workerState{"worker-a": idleWorker("worker-a")}, "worker-a")
	srv.inflight["asn-9"] = &assignment{
		assignmentID: "asn-9",
		requestID:    "req-9",
		worker:       "worker-a",
		payload:      []byte(`{"responseSubject":"out.req-9"}`),
		attempts:     maxBacklogAttempts - 1,
	}
	recorder := srv.nc.(*testhelpers.RecordingNATSConn)

	event := conversation.WorkerStateEvent{
		Dllama:       "worker-a",
		State:        "error",
		AssignmentID: "asn-9",
		Error:        "sidecar responded 500: model exploded",
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	srv.handleState(&nats.Msg{Subject: "state.worker-a", Data: data})

	require.Len(t, recorder.Published, 2)
	var failure map[string]any
	require.NoError(t, json.Unmarshal(recorder.Published[0].Data, &failure))
	errBody, ok := failure["error"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "inference_failed", errBody["code"])
	assert.Contains(t, errBody["message"], "sidecar responded 500: model exploded")
	assert.Equal(t, "[DONE]", string(recorder.Published[1].Data))
	assert.Empty(t, srv.conversations.(*conversationKV).record(t).PinnedDllama)
}
