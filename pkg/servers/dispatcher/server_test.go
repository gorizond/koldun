package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/nats-io/nats-server/v2/server"
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

func startTestNATSServer(t *testing.T) *server.Server {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server not ready")
	}

	t.Cleanup(func() {
		ns.Shutdown()
	})

	return ns
}

func newTestDispatcher(t *testing.T, natsURL string) *Server {
	t.Helper()

	cfg := Config{
		Hash:                fmt.Sprintf("test-%d", time.Now().UnixNano()),
		NATSURL:             natsURL,
		BacklogSubject:      fmt.Sprintf("test.backlog.%d", time.Now().UnixNano()),
		AssignmentsBucket:   fmt.Sprintf("assignments_%d", time.Now().UnixNano()),
		DllamaSubjectPrefix: fmt.Sprintf("test.dllama.%d", time.Now().UnixNano()),
		Logger:              logrus.NewEntry(logrus.New()),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	t.Cleanup(func() {
		_ = srv.nc.Drain()
		srv.nc.Close()
	})

	return srv
}

func TestFinishAssignmentRequeuesAndCleansUp(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration-style test in short mode")
	}

	ns := startTestNATSServer(t)
	dispatcher := newTestDispatcher(t, ns.ClientURL())

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

	ns := startTestNATSServer(t)
	dispatcher := newTestDispatcher(t, ns.ClientURL())

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
	}

	metrics.DispatcherWorkersActive.Set(0)
	metrics.DispatcherAssignmentsInflight.Set(0)

	srv.updateMetrics()

	workersVal := testutil.ToFloat64(metrics.DispatcherWorkersActive)
	assert.Equal(t, float64(2), workersVal)

	inflightVal := testutil.ToFloat64(metrics.DispatcherAssignmentsInflight)
	assert.Equal(t, float64(2), inflightVal)
}

// Integration test with embedded NATS server
func TestDispatcherIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start embedded NATS server
	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1, // Random port
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	defer ns.Shutdown()

	// Wait for server to be ready
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	natsURL := ns.ClientURL()

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
	defer srv.nc.Close()

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
