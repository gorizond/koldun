package ingress

import (
	"crypto/sha256"
	"encoding/hex"
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

// TestIsHexDigest validates hex digest validation.
func TestIsHexDigest(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid 64-char hex",
			input:    strings.Repeat("a", 64),
			expected: true,
		},
		{
			name:     "valid sha256 hash",
			input:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expected: true,
		},
		{
			name:     "uppercase hex",
			input:    "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
			expected: true,
		},
		{
			name:     "too short",
			input:    "abc123",
			expected: false,
		},
		{
			name:     "too long",
			input:    strings.Repeat("a", 65),
			expected: false,
		},
		{
			name:     "invalid characters",
			input:    strings.Repeat("g", 64),
			expected: false,
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "63 chars",
			input:    strings.Repeat("a", 63),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isHexDigest(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSha256Hex validates SHA256 hex encoding.
func TestSha256Hex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:     "simple string",
			input:    "hello",
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			name:     "with spaces",
			input:    "hello world",
			expected: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sha256Hex(tt.input)
			assert.Equal(t, tt.expected, result)
			assert.Len(t, result, 64, "SHA256 hash should be 64 characters")
			assert.True(t, isHexDigest(result), "result should be valid hex")

			// Verify against standard library
			sum := sha256.Sum256([]byte(tt.input))
			expectedStd := hex.EncodeToString(sum[:])
			assert.Equal(t, expectedStd, result)
		})
	}
}

// TestFirstNonEmpty validates first non-empty string selection.
func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "first is non-empty",
			input:    []string{"first", "second", "third"},
			expected: "first",
		},
		{
			name:     "skip empty strings",
			input:    []string{"", "second", "third"},
			expected: "second",
		},
		{
			name:     "skip whitespace",
			input:    []string{"  ", "\t", "value"},
			expected: "value",
		},
		{
			name:     "all empty",
			input:    []string{"", "  ", "\t"},
			expected: "",
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: "",
		},
		{
			name:     "single value",
			input:    []string{"only"},
			expected: "only",
		},
		{
			name:     "returns first trimmed",
			input:    []string{"", "  value  ", "other"},
			expected: "  value  ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := firstNonEmpty(tt.input...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestWriteJSON validates JSON response writing.
func TestWriteJSON(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		payload        any
		expectedStatus int
		expectBody     bool
	}{
		{
			name:           "with payload",
			status:         http.StatusOK,
			payload:        map[string]string{"message": "hello"},
			expectedStatus: http.StatusOK,
			expectBody:     true,
		},
		{
			name:           "nil payload",
			status:         http.StatusNoContent,
			payload:        nil,
			expectedStatus: http.StatusNoContent,
			expectBody:     false,
		},
		{
			name:           "error status with payload",
			status:         http.StatusBadRequest,
			payload:        map[string]string{"error": "invalid request"},
			expectedStatus: http.StatusBadRequest,
			expectBody:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeJSON(w, tt.status, tt.payload)

			assert.Equal(t, tt.expectedStatus, w.Code)
			assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

			if tt.expectBody {
				assert.NotEmpty(t, w.Body.String())
				// Verify it's valid JSON
				var result map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &result)
				assert.NoError(t, err, "response should be valid JSON")
			} else {
				assert.Empty(t, w.Body.String(), "nil payload should produce empty body")
			}
		})
	}
}

// TestMinVal validates minimum value calculation.
func TestMinVal(t *testing.T) {
	tests := []struct {
		name     string
		a        int
		b        int
		expected int
	}{
		{
			name:     "a less than b",
			a:        1,
			b:        5,
			expected: 1,
		},
		{
			name:     "b less than a",
			a:        10,
			b:        3,
			expected: 3,
		},
		{
			name:     "equal values",
			a:        7,
			b:        7,
			expected: 7,
		},
		{
			name:     "negative values",
			a:        -5,
			b:        -10,
			expected: -10,
		},
		{
			name:     "mixed positive negative",
			a:        5,
			b:        -3,
			expected: -3,
		},
		{
			name:     "zero values",
			a:        0,
			b:        0,
			expected: 0,
		},
		{
			name:     "zero and positive",
			a:        0,
			b:        10,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := minVal(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
			assert.LessOrEqual(t, result, tt.a, "result should be <= a")
			assert.LessOrEqual(t, result, tt.b, "result should be <= b")
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
