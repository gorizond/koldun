package ingress

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/api/openai"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/registry"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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

func TestNewEstablishesResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	ns := startIngressJetStream(t)

	cfg := Config{
		RootImage:         "test/root:latest",
		WorkerImage:       "test/worker:latest",
		NATSURL:           ns.ClientURL(),
		InPrefix:          "tenant.in.",
		ConversationTTL:   45 * time.Second,
		SessionMinDllamas: 1,
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	t.Cleanup(func() {
		if srv.stateSub != nil {
			_ = srv.stateSub.Drain()
		}
		if srv.raw != nil {
			_ = srv.raw.Drain()
			srv.raw.Close()
		}
	})

	require.Equal(t, llmRequestStreamName, srv.streamName)
	require.NotNil(t, srv.convKV)
	require.NotNil(t, srv.modelsKV)
	require.NotNil(t, srv.tokensKV)
	require.NotNil(t, srv.stateSub)

	require.Equal(t, defaultListenAddress, srv.cfg.ListenAddress)
	require.Equal(t, defaultNamespace, srv.cfg.Namespace)
	require.Equal(t, "tenant.in.", srv.cfg.InPrefix)
	require.Equal(t, defaultOutPrefix, srv.cfg.OutPrefix)

	status, err := srv.convKV.Status()
	require.NoError(t, err)
	require.InDelta(t, cfg.ConversationTTL.Seconds(), status.TTL().Seconds(), 1.0)

	info, err := srv.nc.StreamInfo(llmRequestStreamName)
	require.NoError(t, err)
	require.Contains(t, info.Config.Subjects, defaultInPrefix+">")
	require.Contains(t, info.Config.Subjects, cfg.InPrefix+">")
}

func TestNewAllowAnonymousSkipsTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	ns := startIngressJetStream(t)

	cfg := Config{
		RootImage:       "test/root:latest",
		WorkerImage:     "test/worker:latest",
		NATSURL:         ns.ClientURL(),
		AllowAnonymous:  true,
		ConversationTTL: 30 * time.Second,
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	t.Cleanup(func() {
		if srv.stateSub != nil {
			_ = srv.stateSub.Drain()
		}
		if srv.raw != nil {
			_ = srv.raw.Drain()
			srv.raw.Close()
		}
	})

	require.NotNil(t, srv.convKV)
	require.NotNil(t, srv.modelsKV)
	require.Nil(t, srv.tokensKV)

	_, err = srv.nc.KeyValue(srv.cfg.TokensBucket)
	require.ErrorIs(t, err, nats.ErrBucketNotFound)
}

func TestRunServesHealthAndReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ns := startIngressJetStream(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cfg := Config{
		ListenAddress:  addr,
		Namespace:      "tenant",
		RootImage:      "test/root:latest",
		WorkerImage:    "test/worker:latest",
		NATSURL:        ns.ClientURL(),
		AllowAnonymous: true,
		Logger:         logrus.New().WithField("component", "test"),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		res, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		return res.StatusCode == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond)

	resp, err := http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()

	resp, err = http.Get("http://" + addr + "/v1/models")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Contains(t, string(body), `"object":"list"`)

	cancel()
	require.NoError(t, <-done)

	if srv.raw != nil {
		srv.raw.Close()
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

func TestEnsureBucketCreatesAndRecreates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	ns := startIngressJetStream(t)
	js, _ := connectIngressJetStream(t, ns)

	_, err := ensureBucket(js, &nats.KeyValueConfig{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bucket name cannot be empty")

	cfg := &nats.KeyValueConfig{
		Bucket: "testbucket",
		TTL:    time.Minute,
	}
	kv, err := ensureBucket(js, cfg)
	require.NoError(t, err)

	status, err := kv.Status()
	require.NoError(t, err)
	require.InDelta(t, cfg.TTL.Seconds(), status.TTL().Seconds(), 1.0)

	cfg.TTL = 2 * time.Minute
	kv, err = ensureBucket(js, cfg)
	require.NoError(t, err)

	status, err = kv.Status()
	require.NoError(t, err)
	require.InDelta(t, cfg.TTL.Seconds(), status.TTL().Seconds(), 1.0)
}

func TestEnsureRequestStreamCreatesAndUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	ns := startIngressJetStream(t)
	js, _ := connectIngressJetStream(t, ns)

	name, err := ensureRequestStream(js, "tenant.in.")
	require.NoError(t, err)
	require.Equal(t, llmRequestStreamName, name)

	info, err := js.StreamInfo(name)
	require.NoError(t, err)
	require.Contains(t, info.Config.Subjects, "tenant.in.>")
	require.Contains(t, info.Config.Subjects, defaultInPrefix+">")

	name, err = ensureRequestStream(js, "tenant.extra.")
	require.NoError(t, err)
	require.Equal(t, llmRequestStreamName, name)

	info, err = js.StreamInfo(name)
	require.NoError(t, err)
	require.Contains(t, info.Config.Subjects, "tenant.extra.>")

	_, err = ensureRequestStream(js, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "in-prefix is required")

	_, err = ensureRequestStream(js, "invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must end with '.'")
}

func TestConversationHashFromHeadersPlain(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Trace-Id", "abc-123")
	req.Header.Set("X-Request-Id", "req-1")
	req.Header.Set("User-Agent", "ingress-test")

	actual, err := conversationHashFromHeaders(req, nil)
	require.NoError(t, err)

	pairs := []string{
		"user-agent=ingress-test",
		"x-request-id=req-1",
		"x-trace-id=abc-123",
	}
	sort.Strings(pairs)
	expectedSum := sha256.Sum256([]byte(strings.Join(pairs, "&")))
	assert.Equal(t, hex.EncodeToString(expectedSum[:]), actual)
}

func TestConversationHashFromHeadersWithSecret(t *testing.T) {
	secret := []byte("topsecret")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Trace-Id", "abc-123")
	req.Header.Set("User-Agent", "ingress-test")
	req.Header.Set("X-Forwarded-Server", "ignored")

	actual, err := conversationHashFromHeaders(req, secret)
	require.NoError(t, err)

	pairs := []string{
		"user-agent=ingress-test",
		"x-trace-id=abc-123",
	}
	sort.Strings(pairs)
	message := []byte(strings.Join(pairs, "&"))
	mac := hmac.New(sha256.New, secret)
	mac.Write(message)
	assert.Equal(t, hex.EncodeToString(mac.Sum(nil)), actual)
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

func TestServerSessionLoadLifecycle(t *testing.T) {
	srv := &Server{}

	require.Zero(t, srv.sessionLoadValue("alpha"))

	require.Equal(t, int32(1), srv.incrementSessionLoad("alpha"))
	require.Equal(t, int32(2), srv.incrementSessionLoad("alpha"))
	require.Equal(t, int32(2), srv.sessionLoadValue("alpha"))

	require.Equal(t, int32(1), srv.decrementSessionLoad("alpha"))
	require.Equal(t, int32(0), srv.decrementSessionLoad("alpha"))
	require.Zero(t, srv.sessionLoadValue("alpha"))

	srv.sessionLoad.mu.Lock()
	_, exists := srv.sessionLoad.lastActivity["alpha"]
	require.False(t, exists, "last activity should be removed when load reaches zero and cleanup disabled")
	require.Nil(t, srv.sessionLoad.idleTimers["alpha"])
	srv.sessionLoad.mu.Unlock()
}

func TestServerScheduleSessionCleanup(t *testing.T) {
	srv := &Server{
		cfg: Config{SessionScaleDownIdleSeconds: 1},
	}

	now := time.Now()
	srv.sessionLoad.mu.Lock()
	srv.scheduleSessionCleanupLocked("beta", now)
	timer := srv.sessionLoad.idleTimers["beta"]
	srv.sessionLoad.mu.Unlock()

	require.NotNil(t, timer, "cleanup timer should be scheduled for idle session")
	require.True(t, timer.Stop(), "timer should be stopped in test to avoid asynchronous cleanup")
}

func TestServerFinalizeSessionCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	t.Run("removes idle record", func(t *testing.T) {
		ns := startIngressJetStream(t)
		js, nc := connectIngressJetStream(t, ns)

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  "ttl_cleanup_idle",
			History: 1,
			TTL:     time.Minute,
		})
		require.NoError(t, err)

		srv := &Server{
			cfg: Config{
				TTLPrefix:                   "nats_ttl_",
				SessionScaleDownIdleSeconds: 1,
			},
			convKV: kv,
			log:    logrus.New().WithField("component", "ingress-test"),
		}
		srv.sessionLoad.lastActivity = map[string]time.Time{
			"idle": time.Now().Add(-2 * time.Second),
		}
		timer := time.NewTimer(time.Hour)
		require.True(t, timer.Stop())
		srv.sessionLoad.idleTimers = map[string]*time.Timer{
			"idle": timer,
		}

		_, err = kv.Put(srv.cfg.TTLPrefix+"idle", []byte("payload"))
		require.NoError(t, err)

		deadline := time.Now().Add(-time.Second)
		srv.finalizeSessionCleanup("idle", deadline)

		require.Eventually(t, func() bool {
			_, err := kv.Get(srv.cfg.TTLPrefix + "idle")
			if err == nil {
				return false
			}
			return errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted)
		}, 5*time.Second, 100*time.Millisecond, "expected conversation record to be removed")

		srv.sessionLoad.mu.Lock()
		_, exists := srv.sessionLoad.lastActivity["idle"]
		require.False(t, exists, "last activity should be cleared after cleanup")
		srv.sessionLoad.mu.Unlock()

		_ = nc.Flush()
	})

	t.Run("skips active session", func(t *testing.T) {
		ns := startIngressJetStream(t)
		js, _ := connectIngressJetStream(t, ns)

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  "ttl_cleanup_active",
			History: 1,
			TTL:     time.Minute,
		})
		require.NoError(t, err)

		srv := &Server{
			cfg: Config{
				TTLPrefix:                   "nats_ttl_",
				SessionScaleDownIdleSeconds: 1,
			},
			convKV: kv,
			log:    logrus.New().WithField("component", "ingress-test"),
		}
		srv.sessionLoad.values = map[string]int32{
			"busy": 2,
		}
		srv.sessionLoad.lastActivity = map[string]time.Time{
			"busy": time.Now(),
		}
		timer := time.NewTimer(time.Hour)
		require.True(t, timer.Stop())
		srv.sessionLoad.idleTimers = map[string]*time.Timer{
			"busy": timer,
		}

		_, err = kv.Put(srv.cfg.TTLPrefix+"busy", []byte("payload"))
		require.NoError(t, err)

		deadline := time.Now().Add(-time.Minute)
		srv.finalizeSessionCleanup("busy", deadline)

		entry, err := kv.Get(srv.cfg.TTLPrefix + "busy")
		require.NoError(t, err, "active session should retain conversation record")
		require.NotNil(t, entry)
	})
}

func TestCacheWorkerStateIdleDetection(t *testing.T) {
	srv := &Server{}

	now := time.Now()
	srv.cacheWorkerState("tenant.prefix", "worker-1", "idle", 0, now)
	require.True(t, srv.hasCachedIdleWorker("tenant.prefix"), "idle worker should be detected")

	srv.cacheWorkerState("tenant.prefix", "worker-1", "running", 1, now.Add(time.Second))
	require.False(t, srv.hasCachedIdleWorker("tenant.prefix"), "non-idle worker should not count")

	stale := time.Now().Add(-time.Minute)
	srv.cacheWorkerState("tenant.prefix", "stale-worker", "idle", 0, stale)
	require.False(t, srv.hasCachedIdleWorker("tenant.prefix"), "stale entries should be purged")
}

func TestApplyCORSHeaders(t *testing.T) {
	srv := &Server{}

	t.Run("reflect origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Origin", "https://example.com")
		w := httptest.NewRecorder()

		srv.applyCORSHeaders(w, req)

		header := w.Result().Header
		require.Equal(t, "https://example.com", header.Get("Access-Control-Allow-Origin"))
		require.Equal(t, corsAllowMethods, header.Get("Access-Control-Allow-Methods"))
		require.Equal(t, corsAllowHeaders, header.Get("Access-Control-Allow-Headers"))
		require.Equal(t, corsExposeHeaders, header.Get("Access-Control-Expose-Headers"))
		require.Equal(t, "3600", header.Get("Access-Control-Max-Age"))
		require.Contains(t, header.Values("Vary"), "Origin", "vary header should include Origin when reflected")
	})

	t.Run("wildcard origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()

		srv.applyCORSHeaders(w, req)

		header := w.Result().Header
		require.Equal(t, "*", header.Get("Access-Control-Allow-Origin"))
		require.Empty(t, header.Get("Vary"), "vary header should remain empty for wildcard origin")
	})
}

func TestAddVaryHeader(t *testing.T) {
	header := http.Header{}

	addVaryHeader(header, "Origin")
	require.Equal(t, "Origin", header.Get("Vary"))

	addVaryHeader(header, "origin")
	require.Equal(t, "Origin", header.Get("Vary"), "duplicate vary values should not be appended")

	addVaryHeader(header, "Authorization")
	require.Equal(t, "Origin, Authorization", header.Get("Vary"))
}

func TestExtractAPIToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-KEY", "priority-token")
	req.Header.Set("Authorization", "Bearer ignored")

	require.Equal(t, "priority-token", extractAPIToken(req), "explicit API key headers should win")

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer actual-token")
	require.Equal(t, "actual-token", extractAPIToken(req))

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic Zm9vOmJhcg==")
	require.Empty(t, extractAPIToken(req), "basic auth should not be treated as API token")

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	require.Empty(t, extractAPIToken(req))
}

func TestStateSubjectParsing(t *testing.T) {
	require.Equal(t, "tenant.session.dllama.", statePrefixFromSubject("tenant.session.dllama.worker.state"))
	require.Equal(t, "", statePrefixFromSubject("invalid"))

	require.Equal(t, "worker", stateWorkerFromSubject("tenant.session.worker.state"))
	require.Equal(t, "", stateWorkerFromSubject("tenant"))
}

func TestHandleReadyFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startIngressJetStream(t)
	js, nc := connectIngressJetStream(t, ns)

	conv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "ttl_ready_conv",
		History: 1,
		TTL:     time.Minute,
	})
	require.NoError(t, err)

	srv := &Server{
		raw: nc,
		cfg: Config{AllowAnonymous: false},
		log: logrus.New().WithField("component", "ingress-test"),
	}
	srv.convKV = conv
	srv.modelsKV = conv

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	srv.handleReady(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, "tokensKV missing should block readiness when anonymous disabled")

	tokens, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "ttl_ready_tokens",
		History: 1,
	})
	require.NoError(t, err)

	srv.tokensKV = tokens
	w = httptest.NewRecorder()
	srv.handleReady(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestListModelsRequiresBucket(t *testing.T) {
	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
	}

	models, err := srv.listModels()
	require.Nil(t, models)
	require.EqualError(t, err, "models bucket unavailable")
}

func TestListModelsReturnsEmptyWhenNoKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startIngressJetStream(t)
	js, _ := connectIngressJetStream(t, ns)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "list_models_empty",
	})
	require.NoError(t, err)

	srv := &Server{
		cfg:      Config{ModelPrefix: registry.DefaultModelPrefix},
		log:      logrus.New().WithField("component", "ingress-test"),
		modelsKV: kv,
	}

	models, err := srv.listModels()
	require.NoError(t, err)
	require.Empty(t, models)
}

func TestListModelsFiltersInvalidEntries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startIngressJetStream(t)
	js, _ := connectIngressJetStream(t, ns)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "list_models_data",
		History: 1,
	})
	require.NoError(t, err)

	valid := registry.Model{
		DisplayName:         "Alpha",
		ConversionSizeHuman: "10GiB",
		OutputPVCName:       "alpha-pvc",
		ReplicaPower:        2,
	}
	payload, err := json.Marshal(valid)
	require.NoError(t, err)

	_, err = kv.Put("model/tenant-alpha/alpha", payload)
	require.NoError(t, err)
	_, err = kv.Put("ignored/key", payload)
	require.NoError(t, err)
	_, err = kv.Put("model/tenant-alpha/bad", []byte("not-json"))
	require.NoError(t, err)

	srv := &Server{
		cfg:      Config{ModelPrefix: registry.DefaultModelPrefix},
		log:      logrus.New().WithField("component", "ingress-test"),
		modelsKV: kv,
	}

	models, err := srv.listModels()
	require.NoError(t, err)
	require.Len(t, models, 1)

	model := models[0]
	require.Equal(t, "tenant-alpha", model.Namespace)
	require.Equal(t, "alpha", model.Name)
	require.Equal(t, "Alpha", model.DisplayName)
	require.Equal(t, int32(2), model.ReplicaPower)
}

func TestListModelsHandlesLookupErrors(t *testing.T) {
	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
		modelsKV: keyValueStub{
			keysFn: func() ([]string, error) {
				return []string{"model/tenant-alpha/missing"}, nil
			},
			getFn: func(string) (nats.KeyValueEntry, error) {
				return nil, nats.ErrKeyNotFound
			},
		},
	}

	models, err := srv.listModels()
	require.NoError(t, err)
	require.Empty(t, models)
}

func TestListModelsPropagatesKeyErrors(t *testing.T) {
	expected := errors.New("kv offline")

	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
		modelsKV: keyValueStub{
			keysFn: func() ([]string, error) {
				return nil, expected
			},
		},
	}

	models, err := srv.listModels()
	require.Nil(t, models)
	require.ErrorIs(t, err, expected)
}

func TestHandleModelsMethodNotAllowed(t *testing.T) {
	t.Parallel()

	srv := &Server{
		log: logrus.New().WithField("component", "ingress-test"),
	}

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/v1/models", nil)
			w := httptest.NewRecorder()

			srv.handleModels(w, req)

			assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
			assert.Contains(t, w.Header().Get("Allow"), "GET")
		})
	}
}

func TestHandleModelsListError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("kv connection failed")
	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
		modelsKV: keyValueStub{
			keysFn: func() ([]string, error) {
				return nil, expectedErr
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()

	srv.handleModels(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "failed to list models")
}

func TestHandleModelsEmptyList(t *testing.T) {
	t.Parallel()

	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
		modelsKV: keyValueStub{
			keysFn: func() ([]string, error) {
				return []string{}, nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()

	srv.handleModels(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var result map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&result)
	assert.NoError(t, err)
	assert.Equal(t, "list", result["object"])

	data := result["data"].([]interface{})
	assert.Equal(t, 0, len(data))
}

func TestHandleModelsFiltersNotReady(t *testing.T) {
	t.Parallel()

	readyModel := `{"namespace":"ns1","name":"ready-model","displayName":"Ready Model","conversionStatus":"completed","conversionSizeBytes":1000,"conversionSizeHuman":"1KB","outputPVCName":"ready-pvc"}`
	notReadyModel := `{"namespace":"ns1","name":"pending-model","displayName":"Pending Model","conversionStatus":"pending"}`

	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
		modelsKV: keyValueStub{
			keysFn: func() ([]string, error) {
				return []string{"model/ns1/ready-model", "model/ns1/pending-model"}, nil
			},
			getFn: func(key string) (nats.KeyValueEntry, error) {
				if key == "model/ns1/ready-model" {
					return &kvEntryStub{value: []byte(readyModel)}, nil
				}
				return &kvEntryStub{value: []byte(notReadyModel)}, nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()

	srv.handleModels(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&result)
	assert.NoError(t, err)

	data := result["data"].([]interface{})
	assert.Equal(t, 1, len(data), "should only include ready models")

	model := data[0].(map[string]interface{})
	assert.Equal(t, "ns1/ready-model", model["id"])
	assert.Equal(t, "Ready Model", model["name"])
}

func TestHandleModelsSortsById(t *testing.T) {
	t.Parallel()

	modelA := `{"namespace":"ns1","name":"alpha","displayName":"Alpha","conversionStatus":"completed","conversionSizeBytes":1000,"conversionSizeHuman":"1KB","outputPVCName":"alpha-pvc"}`
	modelZ := `{"namespace":"ns1","name":"zulu","displayName":"Zulu","conversionStatus":"completed","conversionSizeBytes":2000,"conversionSizeHuman":"2KB","outputPVCName":"zulu-pvc"}`
	modelM := `{"namespace":"ns1","name":"mike","displayName":"Mike","conversionStatus":"completed","conversionSizeBytes":1500,"conversionSizeHuman":"1.5KB","outputPVCName":"mike-pvc"}`

	srv := &Server{
		cfg: Config{ModelPrefix: registry.DefaultModelPrefix},
		log: logrus.New().WithField("component", "ingress-test"),
		modelsKV: keyValueStub{
			keysFn: func() ([]string, error) {
				// Return in non-sorted order
				return []string{"model/ns1/zulu", "model/ns1/alpha", "model/ns1/mike"}, nil
			},
			getFn: func(key string) (nats.KeyValueEntry, error) {
				switch key {
				case "model/ns1/alpha":
					return &kvEntryStub{value: []byte(modelA)}, nil
				case "model/ns1/zulu":
					return &kvEntryStub{value: []byte(modelZ)}, nil
				case "model/ns1/mike":
					return &kvEntryStub{value: []byte(modelM)}, nil
				}
				return nil, nats.ErrKeyNotFound
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()

	srv.handleModels(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&result)
	assert.NoError(t, err)

	data := result["data"].([]interface{})
	assert.Equal(t, 3, len(data))

	// Verify sorted by ID
	ids := make([]string, 3)
	for i, item := range data {
		ids[i] = item.(map[string]interface{})["id"].(string)
	}
	assert.Equal(t, []string{"ns1/alpha", "ns1/mike", "ns1/zulu"}, ids)
}

func TestHandleChatCompletionsRequiresToken(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	srv := &Server{
		cfg: Config{
			AllowAnonymous:  false,
			ResponseTimeout: 200 * time.Millisecond,
		},
		log: logger.WithField("component", "ingress-test"),
	}

	body := strings.NewReader(`{"model":"tenant/model"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.handleChatCompletions(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	var payload openai.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
	require.Contains(t, strings.ToLower(payload.Error.Message), "missing api token")
}

func TestHandleChatCompletionsRejectsInvalidToken(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	srv := &Server{
		cfg: Config{
			AllowAnonymous:  false,
			ResponseTimeout: 200 * time.Millisecond,
		},
		log: logger.WithField("component", "ingress-test"),
	}

	body := strings.NewReader(`{"model":"tenant/model"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer invalid-token")

	w := httptest.NewRecorder()
	srv.handleChatCompletions(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	var payload openai.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
	require.Contains(t, strings.ToLower(payload.Error.Message), "invalid api token")
}

func TestHandleChatCompletionsPublishesAndResponds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startIngressJetStream(t)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := Config{
		ListenAddress:   "127.0.0.1:0",
		Namespace:       "tenant",
		RootImage:       "test/root:latest",
		WorkerImage:     "test/worker:latest",
		NATSURL:         ns.ClientURL(),
		AllowAnonymous:  false,
		ResponseTimeout: 3 * time.Second,
		Logger:          logrus.NewEntry(logger),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.raw.Drain()
		srv.raw.Close()
	})

	token := "super-secret-token"
	tokenHash := sha256Hex(token)
	tokenEntry := registry.Token{
		Hash:      tokenHash,
		Namespace: cfg.Namespace,
	}
	tokenPayload, err := json.Marshal(tokenEntry)
	require.NoError(t, err)
	_, err = srv.tokensKV.Put(srv.cfg.TokenPrefix+tokenHash, tokenPayload)
	require.NoError(t, err)
	srv.invalidateTokenCache()

	modelKey := srv.modelKey(cfg.Namespace, "chat-1")
	modelEntry := registry.Model{
		Namespace:           cfg.Namespace,
		Name:                "chat-1",
		OutputPVCName:       "models-pvc",
		ConversionSizeHuman: "1Gi",
	}
	modelPayload, err := json.Marshal(modelEntry)
	require.NoError(t, err)
	_, err = srv.modelsKV.Put(modelKey, modelPayload)
	require.NoError(t, err)

	requestBody := `{"model":"tenant/chat-1","messages":[{"role":"user","content":"ping"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "ingress-test")
	req.Header.Set("X-Trace-Id", "abc-123")

	hash, err := conversationHashFromHeaders(req, nil)
	require.NoError(t, err)
	backlogSubject := sessionBacklogSubject(hash)

	require.Equal(t, int32(1), srv.incrementSessionLoad(hash))
	defer srv.decrementSessionLoad(hash)

	backlogConn, err := nats.Connect(srv.cfg.NATSURL, nats.Timeout(time.Second))
	require.NoError(t, err)
	t.Cleanup(func() {
		backlogConn.Close()
	})

	sub, err := backlogConn.SubscribeSync(backlogSubject)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
	})
	require.NoError(t, backlogConn.Flush())

	errCh := make(chan error, 1)
	responseCh := make(chan []byte, 1)

	go func() {
		msg, msgErr := sub.NextMsg(5 * time.Second)
		if msgErr != nil {
			errCh <- msgErr
			return
		}

		var backlog conversation.BacklogMessage
		if uErr := json.Unmarshal(msg.Data, &backlog); uErr != nil {
			errCh <- uErr
			return
		}

		var payload struct {
			ResponseSubject string `json:"responseSubject"`
		}
		if uErr := json.Unmarshal(backlog.Payload, &payload); uErr != nil {
			errCh <- uErr
			return
		}

		pubConn, pubErr := nats.Connect(srv.cfg.NATSURL, nats.Timeout(2*time.Second))
		if pubErr != nil {
			errCh <- pubErr
			return
		}
		defer pubConn.Close()

		response := []byte(`{"object":"chat.completion"}`)
		if pubErr := pubConn.Publish(payload.ResponseSubject, response); pubErr != nil {
			errCh <- pubErr
			return
		}
		if flushErr := pubConn.Flush(); flushErr != nil {
			errCh <- flushErr
			return
		}
		responseCh <- response
		errCh <- nil
	}()

	w := httptest.NewRecorder()
	srv.handleChatCompletions(w, req)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for backlog handler")
	}

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	select {
	case expected := <-responseCh:
		require.JSONEq(t, string(expected), w.Body.String())
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for response payload")
	}
}

type keyValueStub struct {
	nats.KeyValue
	keysFn func() ([]string, error)
	getFn  func(string) (nats.KeyValueEntry, error)
}

func (kv keyValueStub) Keys(opts ...nats.WatchOpt) ([]string, error) {
	if kv.keysFn != nil {
		return kv.keysFn()
	}
	if kv.KeyValue != nil {
		return kv.KeyValue.Keys(opts...)
	}
	return nil, errors.New("keys not implemented")
}

func (kv keyValueStub) Get(key string) (nats.KeyValueEntry, error) {
	if kv.getFn != nil {
		return kv.getFn(key)
	}
	if kv.KeyValue != nil {
		return kv.KeyValue.Get(key)
	}
	return nil, errors.New("get not implemented")
}

type kvEntryStub struct {
	nats.KeyValueEntry
	value []byte
}

func (e *kvEntryStub) Value() []byte {
	return e.value
}

func startIngressJetStream(t *testing.T) *server.Server {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	t.Cleanup(ns.Shutdown)

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	return ns
}

func connectIngressJetStream(t *testing.T, ns *server.Server) (nats.JetStreamContext, *nats.Conn) {
	t.Helper()

	nc, err := nats.Connect(ns.ClientURL(), nats.Timeout(2*time.Second))
	require.NoError(t, err)

	js, err := nc.JetStream()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = nc.Drain()
		nc.Close()
	})

	return js, nc
}
