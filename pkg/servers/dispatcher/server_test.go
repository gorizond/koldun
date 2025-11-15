package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/gorizond/koldun/pkg/natsutil"
	testhelpers "github.com/gorizond/koldun/pkg/testutil"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNew validates configuration validation in the New constructor.
func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantError bool
		errSubstr string
		queueErr  bool
	}{
		{
			name: "valid configuration",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "dllama",
				AckWait:             30 * time.Second,
			},
			wantError: false,
		},
		{
			name: "missing hash",
			cfg: Config{
				Hash:                "",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "dllama",
			},
			wantError: true,
			errSubstr: "hash is required",
		},
		{
			name: "missing NATS URL",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "dllama",
			},
			wantError: true,
			errSubstr: "nats url is required",
		},
		{
			name: "missing backlog subject",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "dllama",
			},
			wantError: true,
			errSubstr: "backlog subject is required",
			queueErr:  true,
		},
		{
			name: "missing assignments bucket",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "",
				DllamaSubjectPrefix: "dllama",
			},
			wantError: true,
			errSubstr: "assignments bucket is required",
			queueErr:  true,
		},
		{
			name: "missing dllama subject prefix",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "",
			},
			wantError: true,
			errSubstr: "dllama subject prefix is required",
		},
		{
			name: "missing state subject prefix",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "dllama",
				StateSubjectPrefix:  "   ",
			},
			wantError: true,
			errSubstr: "state subject prefix is required",
		},
		{
			name: "default AckWait is set",
			cfg: Config{
				Hash:                "test-session",
				NATSURL:             "nats://localhost:4222",
				BacklogSubject:      "backlog.test",
				AssignmentsBucket:   "assignments-test",
				DllamaSubjectPrefix: "dllama",
				AckWait:             0, // Should be set to default
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip connection tests for this unit test
			if !tt.wantError {
				t.Skip("Skipping connection test - requires NATS server")
			}

			srv, err := New(tt.cfg)

			if tt.wantError {
				assert.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
				if tt.queueErr {
					assert.ErrorIs(t, err, ErrQueueMisconfigured)
				}
				assert.Nil(t, srv)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, srv)
				if srv != nil {
					// Verify defaults
					if tt.cfg.AckWait == 0 {
						assert.Equal(t, 2*time.Minute, srv.cfg.AckWait)
					}
				}
			}
		})
	}
}

func TestSanitizeStateSubjectPrefix(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{
			name:    "rejects empty prefix",
			input:   "   ",
			wantErr: "state subject prefix is required",
		},
		{
			name:  "appends trailing dot",
			input: "sessions.hash.state",
			want:  "sessions.hash.state.",
		},
		{
			name:  "preserves existing dot",
			input: "sessions.hash.",
			want:  "sessions.hash.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizeStateSubjectPrefix(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestSelectWorker_NoWorkers validates worker selection when no workers are available.
func TestSelectWorker_NoWorkers(t *testing.T) {
	srv := &Server{
		workers: make(map[string]*workerState),
	}

	worker := srv.selectWorker()
	assert.Empty(t, worker, "should return empty string when no workers available")
}

// TestSelectWorker_OnlyBusyWorkers validates worker selection when all workers are busy.
func TestSelectWorker_OnlyBusyWorkers(t *testing.T) {
	srv := &Server{
		workers: map[string]*workerState{
			"worker1": {
				name:          "worker1",
				state:         "busy",
				active:        1,
				lastHeartbeat: time.Now(),
			},
			"worker2": {
				name:          "worker2",
				state:         "processing",
				active:        1,
				lastHeartbeat: time.Now(),
			},
		},
	}

	worker := srv.selectWorker()
	assert.Empty(t, worker, "should return empty string when all workers are busy")
}

// TestSelectWorker_IdleWorkerSelected validates worker selection with idle workers.
func TestSelectWorker_IdleWorkerSelected(t *testing.T) {
	now := time.Now()
	srv := &Server{
		workers: map[string]*workerState{
			"worker1": {
				name:          "worker1",
				state:         "idle",
				active:        0,
				lastHeartbeat: now.Add(-10 * time.Second),
			},
			"worker2": {
				name:          "worker2",
				state:         "busy",
				active:        1,
				lastHeartbeat: now,
			},
			"worker3": {
				name:          "worker3",
				state:         "idle",
				active:        0,
				lastHeartbeat: now.Add(-5 * time.Second),
			},
		},
	}

	worker := srv.selectWorker()
	assert.NotEmpty(t, worker, "should select an idle worker")
	// Should select the least recently used idle worker (worker1)
	assert.Equal(t, "worker1", worker)
}

// TestSnapshotWorkerStats validates worker statistics calculation.
func TestSnapshotWorkerStats(t *testing.T) {
	srv := &Server{
		workers: map[string]*workerState{
			"worker1": {state: "idle", active: 0},
			"worker2": {state: "busy", active: 1},
			"worker3": {state: "idle", active: 0},
			"worker4": {state: "processing", active: 1},
		},
		inflight: map[string]*assignment{
			"assign-1": {assignmentID: "assign-1"},
			"assign-2": {assignmentID: "assign-2"},
		},
	}

	idle, total, inflight := srv.snapshotWorkerStats()

	assert.Equal(t, 2, idle, "should count 2 idle workers")
	assert.Equal(t, 4, total, "should count 4 total workers")
	assert.Equal(t, 2, inflight, "should count 2 inflight assignments")
}

// TestShouldLogNoIdle validates rate limiting of no-idle-workers logs.
func TestShouldLogNoIdle(t *testing.T) {
	srv := &Server{
		lastNoIdleLog: time.Time{},
	}

	// First call should return true
	now := time.Now()
	assert.True(t, srv.shouldLogNoIdle(now))

	// Immediate second call should return false
	assert.False(t, srv.shouldLogNoIdle(now))

	// Call after 6 seconds should return true
	future := now.Add(6 * time.Second)
	assert.True(t, srv.shouldLogNoIdle(future))
}

// TestEnsureTrailingDot validates trailing dot addition to subject prefixes.
func TestEnsureTrailingDot(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"dllama", "dllama."},
		{"dllama.", "dllama."},
		{"", ""},
		{"  ", ""},
		{"test.prefix", "test.prefix."},
		{"  test  ", "test."},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ensureTrailingDot(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSanitizeIdentifier validates identifier sanitization.
func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with-dash", "with-dash"},
		{"with_underscore", "with_underscore"},
		{"with spaces", "with-spaces"},
		{"with@special#chars", "with-special-chars"},
		{"CamelCase123", "CamelCase123"},
		{"", ""},
		{"  ", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func newTestDispatcher(t *testing.T, natsURL string) *Server {
	t.Helper()

	cfg := Config{
		Hash:                fmt.Sprintf("test-%d", time.Now().UnixNano()),
		NATSURL:             natsURL,
		BacklogSubject:      fmt.Sprintf("test.backlog.%d", time.Now().UnixNano()),
		AssignmentsBucket:   fmt.Sprintf("assignments_%d", time.Now().UnixNano()),
		DllamaSubjectPrefix: fmt.Sprintf("test.dllama.%d", time.Now().UnixNano()),
		StateSubjectPrefix:  fmt.Sprintf("test.state.%d", time.Now().UnixNano()),
		Logger:              logrus.NewEntry(logrus.New()),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	t.Cleanup(func() {
		cleanupDispatcherBucket(t, srv.js, srv.cfg.AssignmentsBucket)
		_ = srv.nc.Drain()
		srv.nc.Close()
	})

	return srv
}

func TestFinishAssignmentRequeuesAndCleansUp(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration-style test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	assignment := &assignment{
		assignmentID: "assign-1",
		requestID:    "req-1",
		payload:      []byte(`{"foo":"bar"}`),
	}
	dispatcher.inflight[assignment.assignmentID] = assignment

	env := conversation.AssignmentEnvelope{
		AssignmentID: assignment.assignmentID,
		RequestID:    assignment.requestID,
		Payload:      assignment.payload,
	}
	data, err := json.Marshal(env)
	require.NoError(t, err)

	_, err = dispatcher.assignments.Put(assignment.requestID, data)
	require.NoError(t, err)

	msgCh := make(chan conversation.BacklogMessage, 1)
	sub, err := dispatcher.nc.Subscribe(dispatcher.cfg.BacklogSubject, func(msg *nats.Msg) {
		var backlog conversation.BacklogMessage
		if err := json.Unmarshal(msg.Data, &backlog); err == nil {
			msgCh <- backlog
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})

	dispatcher.finishAssignment(assignment.assignmentID, true)

	select {
	case backlog := <-msgCh:
		assert.Equal(t, assignment.requestID, backlog.ID)
		assert.NotEmpty(t, backlog.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("expected backlog message to be requeued")
	}

	dispatcher.mu.Lock()
	_, exists := dispatcher.inflight[assignment.assignmentID]
	dispatcher.mu.Unlock()
	assert.False(t, exists, "assignment should be removed from inflight map")

	_, err = dispatcher.assignments.Get(assignment.requestID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrKeyNotFound)
}

func TestRecoverAssignmentsRequeuesAndDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration-style test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	keys := []string{"req-a", "req-b"}
	for _, key := range keys {
		payload := json.RawMessage(fmt.Sprintf("{\"request\":\"%s\"}", key))
		env := conversation.AssignmentEnvelope{
			AssignmentID: fmt.Sprintf("assign-%s", key),
			RequestID:    key,
			Payload:      payload,
		}
		payload, err := json.Marshal(env)
		require.NoError(t, err)

		_, err = dispatcher.assignments.Put(key, payload)
		require.NoError(t, err)
	}

	msgCh := make(chan string, len(keys))
	sub, err := dispatcher.nc.Subscribe(dispatcher.cfg.BacklogSubject, func(msg *nats.Msg) {
		var backlog conversation.BacklogMessage
		if err := json.Unmarshal(msg.Data, &backlog); err == nil {
			msgCh <- backlog.ID
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})

	dispatcher.recoverAssignments()

	for range keys {
		select {
		case reqID := <-msgCh:
			assert.Contains(t, keys, reqID)
		case <-time.After(2 * time.Second):
			t.Fatal("expected assignment to be requeued")
		}
	}

	for _, key := range keys {
		_, err := dispatcher.assignments.Get(key)
		assert.Error(t, err)
		assert.ErrorIs(t, err, nats.ErrKeyNotFound)
	}
}

func TestRunHandlesContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration-style dispatcher run test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)
	dispatcher.cfg.MetricsAddr = "127.0.0.1:0"

	dispatcher.workers = map[string]*workerState{
		"worker-1": {
			name:          "worker-1",
			state:         "idle",
			active:        0,
			lastHeartbeat: time.Now(),
		},
	}

	env := conversation.AssignmentEnvelope{
		AssignmentID: "assign-run",
		RequestID:    "req-run",
		Payload:      []byte(`{"hello":"world"}`),
	}
	body, err := json.Marshal(env)
	require.NoError(t, err)
	_, err = dispatcher.assignments.Put(env.RequestID, body)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		dispatcher.mu.Lock()
		defer dispatcher.mu.Unlock()
		return dispatcher.backlogSub != nil && dispatcher.stateSub != nil
	}, 2*time.Second, 50*time.Millisecond, "subscriptions not established")

	cancel()
	require.NoError(t, <-done)
}

func TestRunReturnsErrorWhenStateSubscriptionFails(t *testing.T) {
	stub := &stubNATSConn{
		subscribeErr: errors.New("state subscription failed"),
	}

	srv := &Server{
		cfg: Config{
			NATSURL:             "nats://example:4222",
			BacklogSubject:      "test.backlog",
			StateSubjectPrefix:  "state.",
			QueueGroup:          "dispatcher-test",
			DllamaSubjectPrefix: "dllama.",
		},
		nc:  stub,
		log: logrus.New().WithField("component", "test"),
	}

	err := srv.Run(context.Background())
	require.ErrorIs(t, err, stub.subscribeErr)
	assert.True(t, stub.drained)
	assert.Equal(t, "state.*.state", stub.subscribedSubject)
}

func TestRunReturnsErrorWhenBacklogSubscriptionFails(t *testing.T) {
	stub := &stubNATSConn{
		queueErr: errors.New("backlog subscription failed"),
	}

	srv := &Server{
		cfg: Config{
			NATSURL:             "nats://example:4222",
			BacklogSubject:      "test.backlog",
			StateSubjectPrefix:  "state.",
			QueueGroup:          "dispatcher-test",
			DllamaSubjectPrefix: "dllama.",
		},
		nc:  stub,
		log: logrus.New().WithField("component", "test"),
	}

	err := srv.Run(context.Background())
	require.ErrorIs(t, err, stub.queueErr)
	assert.True(t, stub.drained)
	assert.Equal(t, "state.*.state", stub.subscribedSubject)
	assert.Equal(t, "test.backlog", stub.queueSubject)
	assert.Equal(t, "dispatcher-test", stub.queueGroup)
}

func TestHandleBacklogDispatchesToIdleWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping NATS dependent test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	dispatcher.workers = map[string]*workerState{
		"worker-1": {
			name:          "worker-1",
			state:         "idle",
			active:        0,
			lastHeartbeat: time.Now(),
		},
	}

	assignSubject := fmt.Sprintf("%s%s.in", dispatcher.cfg.DllamaSubjectPrefix, "worker-1")
	assignCh := make(chan *nats.Msg, 1)
	sub, err := dispatcher.nc.Subscribe(assignSubject, func(msg *nats.Msg) {
		assignCh <- msg
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})

	backlog := conversation.BacklogMessage{
		ID:      "request-123",
		Payload: json.RawMessage(`{"foo":"bar"}`),
	}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: dispatcher.cfg.BacklogSubject,
		Data:    data,
	}

	dispatcher.handleBacklog(msg)
	require.NoError(t, dispatcher.nc.Flush())

	var env conversation.AssignmentEnvelope
	select {
	case assignment := <-assignCh:
		require.NoError(t, json.Unmarshal(assignment.Data, &env))
	default:
		t.Fatal("expected assignment to be published to worker subject")
	}

	require.Equal(t, backlog.ID, env.RequestID)
	require.NotEmpty(t, env.AssignmentID)

	dispatcher.mu.Lock()
	_, exists := dispatcher.inflight[env.AssignmentID]
	dispatcher.mu.Unlock()
	require.True(t, exists, "assignment should be tracked as inflight")

	value, err := dispatcher.assignments.Get(backlog.ID)
	require.NoError(t, err)
	require.NotNil(t, value)
}

func TestHandleBacklogNilMessage(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Should not panic with nil message
	dispatcher.handleBacklog(nil)

	// Verify no inflight assignments created
	dispatcher.mu.Lock()
	count := len(dispatcher.inflight)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleBacklogInvalidJSON(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	msg := &nats.Msg{
		Subject: dispatcher.cfg.BacklogSubject,
		Data:    []byte(`{invalid json`),
	}

	// Should handle gracefully without panicking
	dispatcher.handleBacklog(msg)

	// Verify no inflight assignments created
	dispatcher.mu.Lock()
	count := len(dispatcher.inflight)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleBacklogEmptyPayload(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	backlog := conversation.BacklogMessage{
		ID:      "request-456",
		Payload: nil, // Empty payload
	}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: dispatcher.cfg.BacklogSubject,
		Data:    data,
	}

	dispatcher.handleBacklog(msg)

	// Verify no inflight assignments created
	dispatcher.mu.Lock()
	count := len(dispatcher.inflight)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleBacklogNoIdleWorkersRequeues(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping NATS dependent test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Set all workers to busy
	dispatcher.workers = map[string]*workerState{
		"worker-1": {
			name:          "worker-1",
			state:         "busy",
			active:        2,
			lastHeartbeat: time.Now(),
		},
	}

	// Subscribe to backlog to catch requeue
	requeueCh := make(chan *nats.Msg, 1)
	sub, err := dispatcher.nc.Subscribe(dispatcher.cfg.BacklogSubject, func(msg *nats.Msg) {
		requeueCh <- msg
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})

	backlog := conversation.BacklogMessage{
		ID:      "request-789",
		Payload: json.RawMessage(`{"test":"data"}`),
	}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: dispatcher.cfg.BacklogSubject,
		Data:    data,
	}

	dispatcher.handleBacklog(msg)
	require.NoError(t, dispatcher.nc.Flush())

	// Verify message was requeued
	select {
	case requeued := <-requeueCh:
		var requeuedBacklog conversation.BacklogMessage
		require.NoError(t, json.Unmarshal(requeued.Data, &requeuedBacklog))
		require.Equal(t, backlog.ID, requeuedBacklog.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("expected backlog message to be requeued")
	}

	// Verify no assignment was created
	dispatcher.mu.Lock()
	count := len(dispatcher.inflight)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleStateNilMessage(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Should not panic with nil message
	dispatcher.handleState(nil)

	// Verify no workers registered
	dispatcher.mu.Lock()
	count := len(dispatcher.workers)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleStateInvalidJSON(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	msg := &nats.Msg{
		Subject: "test.state",
		Data:    []byte(`{invalid json`),
	}

	// Should handle gracefully without panicking
	dispatcher.handleState(msg)

	// Verify no workers registered
	dispatcher.mu.Lock()
	count := len(dispatcher.workers)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleStateEmptyWorkerName(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	event := conversation.WorkerStateEvent{
		Dllama: "", // Empty worker name
		State:  "idle",
		Active: 0,
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: "test.state",
		Data:    data,
	}

	dispatcher.handleState(msg)

	// Verify no workers registered for empty name
	dispatcher.mu.Lock()
	count := len(dispatcher.workers)
	dispatcher.mu.Unlock()
	require.Equal(t, 0, count)
}

func TestHandleStateRegistersNewWorker(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	event := conversation.WorkerStateEvent{
		Dllama: "worker-1",
		State:  "idle",
		Active: 0,
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: "test.state",
		Data:    data,
	}

	dispatcher.handleState(msg)

	// Verify worker was registered
	dispatcher.mu.Lock()
	worker, exists := dispatcher.workers["worker-1"]
	dispatcher.mu.Unlock()

	require.True(t, exists)
	require.Equal(t, "idle", worker.state)
	require.Equal(t, int32(0), worker.active)
	require.Equal(t, "worker-1", worker.name)
}

func TestHandleStateUpdatesExistingWorker(t *testing.T) {
	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Register initial worker
	dispatcher.workers = map[string]*workerState{
		"worker-1": {
			name:          "worker-1",
			state:         "busy",
			active:        2,
			lastHeartbeat: time.Now().Add(-1 * time.Minute),
		},
	}

	event := conversation.WorkerStateEvent{
		Dllama: "worker-1",
		State:  "idle",
		Active: 0,
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: "test.state",
		Data:    data,
	}

	beforeUpdate := dispatcher.workers["worker-1"].lastHeartbeat
	dispatcher.handleState(msg)

	// Verify worker was updated
	dispatcher.mu.Lock()
	worker := dispatcher.workers["worker-1"]
	dispatcher.mu.Unlock()

	require.Equal(t, "idle", worker.state)
	require.Equal(t, int32(0), worker.active)
	require.True(t, worker.lastHeartbeat.After(beforeUpdate))
}

func TestHandleStateIdleTriggersFinish(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping NATS dependent test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Setup inflight assignment
	assignment := &assignment{
		assignmentID: "assign-123",
		requestID:    "req-123",
		payload:      []byte(`{"test":"data"}`),
	}
	dispatcher.inflight[assignment.assignmentID] = assignment

	// Store in KV
	env := conversation.AssignmentEnvelope{
		AssignmentID: assignment.assignmentID,
		RequestID:    assignment.requestID,
		Payload:      assignment.payload,
	}
	envData, err := json.Marshal(env)
	require.NoError(t, err)
	_, err = dispatcher.assignments.Put(assignment.requestID, envData)
	require.NoError(t, err)

	// Send idle state with assignment ID
	event := conversation.WorkerStateEvent{
		Dllama:       "worker-1",
		State:        "idle",
		Active:       0,
		AssignmentID: assignment.assignmentID,
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: "test.state",
		Data:    data,
	}

	dispatcher.handleState(msg)

	// Verify assignment was finished (removed from inflight)
	dispatcher.mu.Lock()
	_, exists := dispatcher.inflight[assignment.assignmentID]
	dispatcher.mu.Unlock()

	require.False(t, exists, "assignment should be removed from inflight")

	// Verify KV was cleaned up
	_, err = dispatcher.assignments.Get(assignment.requestID)
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrKeyNotFound)
}

func TestHandleStateErrorTriggersRequeue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping NATS dependent test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Setup inflight assignment
	assignment := &assignment{
		assignmentID: "assign-456",
		requestID:    "req-456",
		payload:      []byte(`{"test":"error"}`),
	}
	dispatcher.inflight[assignment.assignmentID] = assignment

	// Subscribe to backlog to catch requeue
	requeueCh := make(chan conversation.BacklogMessage, 1)
	sub, err := dispatcher.nc.Subscribe(dispatcher.cfg.BacklogSubject, func(msg *nats.Msg) {
		var backlog conversation.BacklogMessage
		if err := json.Unmarshal(msg.Data, &backlog); err == nil {
			requeueCh <- backlog
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})

	// Send error state with assignment ID
	event := conversation.WorkerStateEvent{
		Dllama:       "worker-1",
		State:        "error",
		Active:       0,
		AssignmentID: assignment.assignmentID,
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: "test.state",
		Data:    data,
	}

	dispatcher.handleState(msg)
	require.NoError(t, dispatcher.nc.Flush())

	// Verify message was requeued
	select {
	case backlog := <-requeueCh:
		require.Equal(t, assignment.requestID, backlog.ID)
		require.NotEmpty(t, backlog.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("expected backlog message to be requeued after error state")
	}

	// Verify assignment was removed from inflight
	dispatcher.mu.Lock()
	_, exists := dispatcher.inflight[assignment.assignmentID]
	dispatcher.mu.Unlock()
	require.False(t, exists)
}

func TestUpdateMetricsReflectsState(t *testing.T) {
	srv := &Server{
		workers: map[string]*workerState{
			"idle-1": {state: "idle", active: 0, lastHeartbeat: time.Now()},
			"idle-2": {state: "IDLE", active: 0, lastHeartbeat: time.Now()},
			"busy":   {state: "busy", active: 1, lastHeartbeat: time.Now()},
		},
		inflight: map[string]*assignment{
			"req-1": {},
			"req-2": {},
		},
		backlogSub: stubBacklogSubscription{pending: 5},
	}

	metrics.DispatcherWorkersActive.Set(0)
	metrics.DispatcherAssignmentsInflight.Set(0)
	metrics.DispatcherBacklogSize.Set(0)

	srv.updateMetrics()

	workersVal := testutil.ToFloat64(metrics.DispatcherWorkersActive)
	assert.Equal(t, float64(2), workersVal)

	inflightVal := testutil.ToFloat64(metrics.DispatcherAssignmentsInflight)
	assert.Equal(t, float64(2), inflightVal)

	backlogVal := testutil.ToFloat64(metrics.DispatcherBacklogSize)
	assert.Equal(t, float64(5), backlogVal)
}

func TestUpdateMetricsPeriodicallyUsesTicker(t *testing.T) {
	prevFactory := metricsTickerFactory
	fake := newFakeTicker()
	metricsTickerFactory = func(d time.Duration) metricsTicker {
		assert.Equal(t, 5*time.Second, d)
		return fake
	}
	defer func() { metricsTickerFactory = prevFactory }()

	metrics.DispatcherWorkersActive.Set(0)
	metrics.DispatcherAssignmentsInflight.Set(0)

	srv := &Server{
		workers: map[string]*workerState{
			"idle": {state: "idle"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		srv.updateMetricsPeriodically(ctx)
		close(done)
	}()

	fake.tick()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.DispatcherWorkersActive) == 1
	}, time.Second, 10*time.Millisecond)

	cancel()
	<-done
	assert.True(t, fake.stopped)
}

func TestHandleBacklogKVPutFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping NATS dependent test in short mode")
	}

	natsURL := testNATSURL(t)
	dispatcher := newTestDispatcher(t, natsURL)

	// Setup idle worker
	dispatcher.workers = map[string]*workerState{
		"worker-1": {
			name:          "worker-1",
			state:         "idle",
			active:        0,
			lastHeartbeat: time.Now(),
		},
	}

	// Subscribe to assignment subject to verify it was published
	assignSubject := fmt.Sprintf("%s%s.in", dispatcher.cfg.DllamaSubjectPrefix, "worker-1")
	assignCh := make(chan *nats.Msg, 1)
	sub, err := dispatcher.nc.Subscribe(assignSubject, func(msg *nats.Msg) {
		assignCh <- msg
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})

	// Delete the KV bucket to force KVPut failure
	js, err := dispatcher.nc.JetStream()
	require.NoError(t, err)
	err = js.DeleteKeyValue(dispatcher.cfg.AssignmentsBucket)
	require.NoError(t, err)

	backlog := conversation.BacklogMessage{
		ID:      "request-kvfail",
		Payload: json.RawMessage(`{"test":"kvfail"}`),
	}
	data, err := json.Marshal(backlog)
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: dispatcher.cfg.BacklogSubject,
		Data:    data,
	}

	// Should not panic, should continue despite KV failure
	dispatcher.handleBacklog(msg)
	require.NoError(t, dispatcher.nc.Flush())

	// Verify assignment was still dispatched despite KV failure
	select {
	case assignment := <-assignCh:
		var env conversation.AssignmentEnvelope
		require.NoError(t, json.Unmarshal(assignment.Data, &env))
		require.Equal(t, backlog.ID, env.RequestID)
	case <-time.After(2 * time.Second):
		t.Fatal("expected assignment to be published despite KV failure")
	}

	// Verify assignment is still tracked in memory
	dispatcher.mu.Lock()
	count := len(dispatcher.inflight)
	dispatcher.mu.Unlock()
	require.Equal(t, 1, count, "assignment should be tracked in inflight despite KV failure")
}

func TestRecoverAssignmentsKeysError(t *testing.T) {
	mockKV := &mockKeyValue{
		keysErr: errors.New("keys listing failed"),
	}

	srv := &Server{
		assignments: mockKV,
		log:         logrus.New().WithField("component", "test"),
	}

	// Should handle error gracefully without panicking
	require.NotPanics(t, func() {
		srv.recoverAssignments()
	})
}

func TestRecoverAssignmentsGetError(t *testing.T) {
	mockKV := &mockKeyValue{
		keys:   []string{"req-1", "req-2"},
		getErr: errors.New("get failed"),
	}

	srv := &Server{
		assignments: mockKV,
		nc:          &testhelpers.RecordingNATSConn{},
		retryConfig: natsutil.RetryConfig{MaxRetries: 0},
		log:         logrus.New().WithField("component", "test"),
		cfg: Config{
			BacklogSubject: "test.backlog",
		},
	}

	// Should handle error gracefully and skip failed entries
	require.NotPanics(t, func() {
		srv.recoverAssignments()
	})

	// Verify no messages were published due to Get errors
	rec := srv.nc.(*testhelpers.RecordingNATSConn)
	assert.Len(t, rec.Published, 0)
}

func TestRecoverAssignmentsUnmarshalError(t *testing.T) {
	mockKV := &mockKeyValue{
		keys: []string{"req-1"},
		entries: map[string][]byte{
			"req-1": []byte(`{invalid json`),
		},
	}

	srv := &Server{
		assignments: mockKV,
		nc:          &testhelpers.RecordingNATSConn{},
		retryConfig: natsutil.RetryConfig{MaxRetries: 0},
		log:         logrus.New().WithField("component", "test"),
		cfg: Config{
			BacklogSubject: "test.backlog",
		},
	}

	// Should handle unmarshal error gracefully
	require.NotPanics(t, func() {
		srv.recoverAssignments()
	})

	// Verify no messages were published due to unmarshal error
	rec := srv.nc.(*testhelpers.RecordingNATSConn)
	assert.Len(t, rec.Published, 0)
}

func TestRecoverAssignmentsDeleteError(t *testing.T) {
	env := conversation.AssignmentEnvelope{
		AssignmentID: "assign-1",
		RequestID:    "req-1",
		Payload:      []byte(`{"test":"data"}`),
	}
	envData, err := json.Marshal(env)
	require.NoError(t, err)

	mockKV := &mockKeyValue{
		keys: []string{"req-1"},
		entries: map[string][]byte{
			"req-1": envData,
		},
		deleteErr: errors.New("delete failed"),
	}

	srv := &Server{
		assignments: mockKV,
		nc:          &testhelpers.RecordingNATSConn{},
		retryConfig: natsutil.RetryConfig{MaxRetries: 0},
		log:         logrus.New().WithField("component", "test"),
		cfg: Config{
			BacklogSubject: "test.backlog",
		},
	}

	// Should handle delete error gracefully
	require.NotPanics(t, func() {
		srv.recoverAssignments()
	})

	// Verify message was still requeued despite delete error
	rec := srv.nc.(*testhelpers.RecordingNATSConn)
	assert.Len(t, rec.Published, 1)
	assert.Equal(t, "test.backlog", rec.Published[0].Subject)
}

func TestRecoverAssignmentsNoKeysFound(t *testing.T) {
	mockKV := &mockKeyValue{
		keysErr: nats.ErrNoKeysFound,
	}

	srv := &Server{
		assignments: mockKV,
		log:         logrus.New().WithField("component", "test"),
	}

	// Should return early without error
	require.NotPanics(t, func() {
		srv.recoverAssignments()
	})
}

func TestRequeueAssignmentPublishesBacklog(t *testing.T) {
	rec := &testhelpers.RecordingNATSConn{}
	srv := &Server{
		cfg: Config{
			BacklogSubject: "test.backlog",
		},
		nc:          rec,
		retryConfig: natsutil.RetryConfig{MaxRetries: 0},
		log:         logrus.New().WithField("component", "test"),
	}

	asn := &assignment{
		requestID: "req-1",
		payload:   []byte(`{"foo":"bar"}`),
	}

	srv.requeueAssignment(asn)

	require.Len(t, rec.Published, 1)
	assert.Equal(t, "test.backlog", rec.Published[0].Subject)

	var backlog conversation.BacklogMessage
	require.NoError(t, json.Unmarshal(rec.Published[0].Data, &backlog))
	assert.Equal(t, "req-1", backlog.ID)
	assert.NotEmpty(t, backlog.Payload)
}

func TestRequeueAssignmentLogsPublishError(t *testing.T) {
	rec := &testhelpers.RecordingNATSConn{PublishErr: errors.New("boom")}
	srv := &Server{
		cfg: Config{
			BacklogSubject: "test.backlog",
		},
		nc:          rec,
		retryConfig: natsutil.RetryConfig{MaxRetries: 0},
		log:         logrus.New().WithField("component", "test"),
	}

	asn := &assignment{
		requestID: "req-2",
		payload:   []byte(`{"hello":"world"}`),
	}

	require.NotPanics(t, func() {
		srv.requeueAssignment(asn)
	})
	require.Len(t, rec.Published, 1)
	assert.Equal(t, "test.backlog", rec.Published[0].Subject)
}

func TestDispatcherIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	natsURL := testNATSURL(t)

	// Create dispatcher
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := Config{
		Hash:                "test-session",
		NATSURL:             natsURL,
		BacklogSubject:      "test.backlog",
		AssignmentsBucket:   "test-assignments",
		DllamaSubjectPrefix: "test.dllama",
		StateSubjectPrefix:  "test.dllama",
		Logger:              logrus.NewEntry(logger),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)
	t.Cleanup(func() {
		cleanupDispatcherBucket(t, srv.js, cfg.AssignmentsBucket)
		srv.nc.Close()
	})

	// Test 1: Verify initialization
	assert.NotNil(t, srv.nc)
	assert.NotNil(t, srv.js)
	assert.NotNil(t, srv.assignments)

	// Test 2: Start dispatcher
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- srv.Run(ctx)
	}()

	// Give it time to start subscriptions
	time.Sleep(100 * time.Millisecond)

	// Test 3: Simulate worker state event
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()

	stateEvent := conversation.WorkerStateEvent{
		Dllama: "test-worker-1",
		State:  "idle",
		Active: 0,
	}
	stateData, err := json.Marshal(stateEvent)
	require.NoError(t, err)

	err = nc.Publish("test.dllama.test-worker-1.state", stateData)
	require.NoError(t, err)

	// Give it time to process
	time.Sleep(100 * time.Millisecond)

	// Test 4: Verify worker was registered
	srv.mu.Lock()
	worker, exists := srv.workers["test-worker-1"]
	srv.mu.Unlock()
	assert.True(t, exists, "worker should be registered")
	assert.Equal(t, "idle", worker.state)

	// Test 5: Publish backlog message
	backlogMsg := conversation.BacklogMessage{
		ID:        "test-request-1",
		Payload:   []byte(`{"test": "payload"}`),
		CreatedAt: time.Now().Unix(),
	}
	backlogData, err := json.Marshal(backlogMsg)
	require.NoError(t, err)

	// Subscribe to worker inbox to catch assignment
	workerInbox := "test.dllama.test-worker-1.in"
	assignmentReceived := make(chan bool, 1)
	_, err = nc.Subscribe(workerInbox, func(msg *nats.Msg) {
		var env conversation.AssignmentEnvelope
		if err := json.Unmarshal(msg.Data, &env); err == nil {
			if env.RequestID == "test-request-1" {
				assignmentReceived <- true
			}
		}
	})
	require.NoError(t, err)

	err = nc.Publish("test.backlog", backlogData)
	require.NoError(t, err)

	// Test 6: Verify assignment was dispatched
	select {
	case <-assignmentReceived:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("assignment was not dispatched to worker")
	}

	// Wait for context cancellation
	<-ctx.Done()
	err = <-done
	assert.NoError(t, err)
}

type stubBacklogSubscription struct {
	pending int
	err     error
}

func (s stubBacklogSubscription) Pending() (int, int, error) {
	return s.pending, 0, s.err
}

type fakeTicker struct {
	ch      chan time.Time
	stopped bool
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time, 1)}
}

func (f *fakeTicker) C() <-chan time.Time {
	return f.ch
}

func (f *fakeTicker) Stop() {
	f.stopped = true
}

func (f *fakeTicker) tick() {
	f.ch <- time.Now()
}

type stubNATSConn struct {
	subscribeErr      error
	queueErr          error
	subscribedSubject string
	queueSubject      string
	queueGroup        string
	drained           bool
}

func (s *stubNATSConn) Publish(string, []byte) error {
	return nil
}

func (s *stubNATSConn) Subscribe(subject string, handler nats.MsgHandler) (*nats.Subscription, error) {
	s.subscribedSubject = subject
	if s.subscribeErr != nil {
		return nil, s.subscribeErr
	}
	return &nats.Subscription{}, nil
}

func (s *stubNATSConn) SubscribeSync(string) (*nats.Subscription, error) {
	return nil, errors.New("SubscribeSync not implemented")
}

func (s *stubNATSConn) QueueSubscribe(subject, queue string, handler nats.MsgHandler) (*nats.Subscription, error) {
	s.queueSubject = subject
	s.queueGroup = queue
	if s.queueErr != nil {
		return nil, s.queueErr
	}
	return &nats.Subscription{}, nil
}

func (s *stubNATSConn) Flush() error { return nil }

func (s *stubNATSConn) Close() {}

func (s *stubNATSConn) Drain() error {
	s.drained = true
	return nil
}

func (s *stubNATSConn) JetStream(...nats.JSOpt) (nats.JetStreamContext, error) {
	return nil, errors.New("JetStream not implemented")
}

func (s *stubNATSConn) Status() nats.Status { return nats.CONNECTED }

// mockKeyValue is a mock implementation of nats.KeyValue for testing
type mockKeyValue struct {
	keys      []string
	entries   map[string][]byte
	keysErr   error
	getErr    error
	putErr    error
	deleteErr error
}

func (m *mockKeyValue) Get(key string) (nats.KeyValueEntry, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if data, ok := m.entries[key]; ok {
		return &mockKVEntry{value: data}, nil
	}
	return nil, nats.ErrKeyNotFound
}

func (m *mockKeyValue) Put(key string, value []byte) (uint64, error) {
	if m.putErr != nil {
		return 0, m.putErr
	}
	if m.entries == nil {
		m.entries = make(map[string][]byte)
	}
	m.entries[key] = value
	return 1, nil
}

func (m *mockKeyValue) Delete(key string, opts ...nats.DeleteOpt) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.entries, key)
	return nil
}

func (m *mockKeyValue) Keys(opts ...nats.WatchOpt) ([]string, error) {
	if m.keysErr != nil {
		return nil, m.keysErr
	}
	return m.keys, nil
}

func (m *mockKeyValue) Create(string, []byte) (uint64, error) {
	return 0, errors.New("Create not implemented")
}

func (m *mockKeyValue) Update(string, []byte, uint64) (uint64, error) {
	return 0, errors.New("Update not implemented")
}

func (m *mockKeyValue) PutString(string, string) (uint64, error) {
	return 0, errors.New("PutString not implemented")
}

func (m *mockKeyValue) GetRevision(string, uint64) (nats.KeyValueEntry, error) {
	return nil, errors.New("GetRevision not implemented")
}

func (m *mockKeyValue) Purge(string, ...nats.DeleteOpt) error {
	return errors.New("Purge not implemented")
}

func (m *mockKeyValue) Watch(string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("Watch not implemented")
}

func (m *mockKeyValue) WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("WatchAll not implemented")
}

func (m *mockKeyValue) History(string, ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	return nil, errors.New("History not implemented")
}

func (m *mockKeyValue) Bucket() string {
	return "mock-bucket"
}

func (m *mockKeyValue) Status() (nats.KeyValueStatus, error) {
	return nil, errors.New("Status not implemented")
}

type mockKVEntry struct {
	value []byte
}

func (m *mockKVEntry) Key() string {
	return "mock-key"
}

func (m *mockKVEntry) Value() []byte {
	return m.value
}

func (m *mockKVEntry) Bucket() string {
	return "mock-bucket"
}

func (m *mockKVEntry) Created() time.Time {
	return time.Now()
}

func (m *mockKVEntry) Delta() uint64 {
	return 0
}

func (m *mockKVEntry) Operation() nats.KeyValueOp {
	return nats.KeyValuePut
}

func (m *mockKVEntry) Revision() uint64 {
	return 1
}
