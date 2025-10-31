package ingress

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
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
			name: "minimal valid configuration",
			cfg: Config{
				ListenAddress: ":8082",
				Namespace:     "default",
				RootImage:     "test/root:latest",
				WorkerImage:   "test/worker:latest",
				NATSURL:       "nats://localhost:4222",
			},
			wantError: false,
		},
		{
			name: "missing root image",
			cfg: Config{
				ListenAddress: ":8082",
				Namespace:     "default",
				RootImage:     "",
				WorkerImage:   "test/worker:latest",
				NATSURL:       "nats://localhost:4222",
			},
			wantError: true,
			errSubstr: "root image is required",
		},
		{
			name: "missing worker image",
			cfg: Config{
				ListenAddress: ":8082",
				Namespace:     "default",
				RootImage:     "test/root:latest",
				WorkerImage:   "",
				NATSURL:       "nats://localhost:4222",
			},
			wantError: true,
			errSubstr: "worker image is required",
		},
		{
			name: "missing NATS URL uses default",
			cfg: Config{
				ListenAddress: ":8082",
				Namespace:     "default",
				RootImage:     "test/root:latest",
				WorkerImage:   "test/worker:latest",
				NATSURL:       "",
			},
			wantError: false, // Uses default NATS URL
		},
		{
			name: "defaults are set",
			cfg: Config{
				RootImage:   "test/root:latest",
				WorkerImage: "test/worker:latest",
				NATSURL:     "nats://localhost:4222",
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
					if tt.cfg.ListenAddress == "" {
						assert.Equal(t, defaultListenAddress, srv.cfg.ListenAddress)
					}
					if tt.cfg.Namespace == "" {
						assert.Equal(t, defaultNamespace, srv.cfg.Namespace)
					}
				}
			}
		})
	}
}

// TestSanitizeSessionHash validates session hash sanitization.
func TestSanitizeSessionHash(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"UPPERCASE", "uppercase"},
		{"  spaced  ", "spaced"},
		{"very-long-hash-that-exceeds-32-characters-limit", "very-long-hash-that-exceeds-32-c"},
		{"", ""},
		{"MixedCase123", "mixedcase123"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeSessionHash(tt.input)
			assert.Equal(t, tt.expected, result)
			assert.LessOrEqual(t, len(result), 32, "sanitized hash should not exceed 32 characters")
		})
	}
}

// TestEnsureTrailingDot validates trailing dot addition.
func TestEnsureTrailingDot(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"prefix", "prefix."},
		{"prefix.", "prefix."},
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

// TestResponseSubjectPrefix validates response subject prefix generation.
func TestResponseSubjectPrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		hash     string
		expected string
	}{
		{
			name:     "basic prefix and hash",
			prefix:   "out",
			hash:     "abc123",
			expected: "out.abc123.",
		},
		{
			name:     "prefix with trailing dot",
			prefix:   "out.",
			hash:     "abc123",
			expected: "out.abc123.",
		},
		{
			name:     "empty prefix",
			prefix:   "",
			hash:     "abc123",
			expected: "abc123.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := responseSubjectPrefix(tt.prefix, tt.hash)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSessionBacklogSubject validates backlog subject generation.
func TestSessionBacklogSubject(t *testing.T) {
	tests := []struct {
		hash     string
		expected string
	}{
		{"abc123", "sessions.abc123.requests"},
		{"ABC123", "sessions.abc123.requests"},
		{"  test  ", "sessions.test.requests"},
	}

	for _, tt := range tests {
		t.Run(tt.hash, func(t *testing.T) {
			result := sessionBacklogSubject(tt.hash)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDllamaSubjectPrefix validates dllama subject prefix generation.
func TestDllamaSubjectPrefix(t *testing.T) {
	tests := []struct {
		hash     string
		expected string
	}{
		{"abc123", "sessions.abc123.dllama."},
		{"ABC123", "sessions.abc123.dllama."},
		{"  test  ", "sessions.test.dllama."},
	}

	for _, tt := range tests {
		t.Run(tt.hash, func(t *testing.T) {
			result := dllamaSubjectPrefix(tt.hash)
			assert.Equal(t, tt.expected, result)
			assert.True(t, strings.HasSuffix(result, "."), "should end with trailing dot")
		})
	}
}

// TestAssignmentsBucketName validates assignments bucket name generation.
func TestAssignmentsBucketName(t *testing.T) {
	tests := []struct {
		hash     string
		expected string
	}{
		{"abc123", "sess_abc123_assign"},
		{"ABC123", "sess_abc123_assign"},
		{"  test  ", "sess_test_assign"},
	}

	for _, tt := range tests {
		t.Run(tt.hash, func(t *testing.T) {
			result := assignmentsBucketName(tt.hash)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestStateStreamName validates state stream name generation.
func TestStateStreamName(t *testing.T) {
	tests := []struct {
		hash     string
		expected string
	}{
		{"abc123", "SESS_ABC123_STATE"},
		{"ABC123", "SESS_ABC123_STATE"},
		{"  test  ", "SESS_TEST_STATE"},
	}

	for _, tt := range tests {
		t.Run(tt.hash, func(t *testing.T) {
			result := stateStreamName(tt.hash)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, strings.ToUpper(result), result, "should be uppercase")
		})
	}
}

// TestNewRequestID validates request ID generation.
func TestNewRequestID(t *testing.T) {
	id1 := newRequestID()
	id2 := newRequestID()

	assert.NotEmpty(t, id1, "request ID should not be empty")
	assert.NotEmpty(t, id2, "request ID should not be empty")
	assert.NotEqual(t, id1, id2, "consecutive request IDs should be unique")
}

// TestAbsDuration validates absolute duration calculation.
func TestAbsDuration(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected time.Duration
	}{
		{5 * time.Second, 5 * time.Second},
		{-5 * time.Second, 5 * time.Second},
		{0, 0},
		{time.Hour, time.Hour},
		{-time.Hour, time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.input.String(), func(t *testing.T) {
			result := absDuration(tt.input)
			assert.Equal(t, tt.expected, result)
			assert.GreaterOrEqual(t, result, time.Duration(0), "result should be non-negative")
		})
	}
}

// TestUniqueSubjects validates unique subjects extraction.
func TestUniqueSubjects(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]struct{}
		expected int
		sorted   bool
	}{
		{
			name:     "empty map",
			input:    map[string]struct{}{},
			expected: 0,
		},
		{
			name: "single subject",
			input: map[string]struct{}{
				"subject.1": {},
			},
			expected: 1,
		},
		{
			name: "multiple subjects",
			input: map[string]struct{}{
				"subject.3": {},
				"subject.1": {},
				"subject.2": {},
			},
			expected: 3,
			sorted:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := uniqueSubjects(tt.input)
			assert.Len(t, result, tt.expected)

			if tt.sorted && len(result) > 1 {
				// Verify sorting
				for i := 1; i < len(result); i++ {
					assert.Less(t, result[i-1], result[i], "subjects should be sorted")
				}
			}
		})
	}
}

// Integration test with embedded NATS server
func TestIngressIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start embedded NATS server
	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	defer ns.Shutdown()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	natsURL := ns.ClientURL()

	// Create ingress server
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := Config{
		ListenAddress: "127.0.0.1:0", // Random port
		Namespace:     "test",
		RootImage:     "test/root:latest",
		WorkerImage:   "test/worker:latest",
		NATSURL:       natsURL,
		Logger:        logrus.NewEntry(logger),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)
	defer srv.raw.Close()

	// Test 1: Verify initialization
	assert.NotNil(t, srv.raw, "raw NATS connection should be initialized")
	assert.NotNil(t, srv.nc, "JetStream context should be initialized")
	assert.NotNil(t, srv.convKV, "conversation KV should be initialized")
	assert.NotNil(t, srv.modelsKV, "models KV should be initialized")
	assert.NotNil(t, srv.tokensKV, "tokens KV should be initialized")

	// Test 2: Health endpoint
	t.Run("health endpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()

		srv.handleHealth(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		// Content-Type may not be set for simple responses
		body := strings.TrimSpace(w.Body.String())
		assert.NotEmpty(t, body, "health response should not be empty")
	})

	// Test 3: Ready endpoint
	t.Run("ready endpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		w := httptest.NewRecorder()

		srv.handleReady(w, req)

		// Should return OK since NATS connection is established
		assert.Equal(t, http.StatusOK, w.Code)
		// Content-Type may not be set for simple responses
	})

	// Test 4: Models endpoint (should return empty list or existing models)
	t.Run("models endpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		w := httptest.NewRecorder()

		srv.handleModels(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		// Should be valid JSON
		var result map[string]interface{}
		err := json.NewDecoder(w.Body).Decode(&result)
		assert.NoError(t, err)
		assert.Equal(t, "list", result["object"])
	})
}
