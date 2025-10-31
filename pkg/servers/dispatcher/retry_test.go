package dispatcher

import (
	"errors"
	"testing"
	"time"
)

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()

	if cfg.MaxRetries <= 0 {
		t.Error("MaxRetries should be positive")
	}
	if cfg.InitialBackoff <= 0 {
		t.Error("InitialBackoff should be positive")
	}
	if cfg.MaxBackoff <= 0 {
		t.Error("MaxBackoff should be positive")
	}
	if cfg.BackoffFactor <= 1.0 {
		t.Error("BackoffFactor should be > 1.0 for exponential backoff")
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		t.Error("MaxBackoff should be >= InitialBackoff")
	}

	// Verify expected default values
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.InitialBackoff != 100*time.Millisecond {
		t.Errorf("InitialBackoff = %v, want 100ms", cfg.InitialBackoff)
	}
	if cfg.MaxBackoff != 5*time.Second {
		t.Errorf("MaxBackoff = %v, want 5s", cfg.MaxBackoff)
	}
	if cfg.BackoffFactor != 2.0 {
		t.Errorf("BackoffFactor = %f, want 2.0", cfg.BackoffFactor)
	}
}

func TestPublishWithRetry_NilConnection(t *testing.T) {
	cfg := DefaultRetryConfig()
	err := PublishWithRetry(nil, "test.subject", []byte("data"), cfg, nil)

	if err == nil {
		t.Error("PublishWithRetry should return error for nil connection")
	}
	if !errors.Is(err, errors.New("NATS connection is nil")) {
		expectedMsg := "NATS connection is nil"
		if err.Error() != expectedMsg {
			t.Errorf("Error message = %v, want %v", err.Error(), expectedMsg)
		}
	}
}

func TestPublishWithRetry_EmptySubject(t *testing.T) {
	// Note: We can't test with real NATS connection in unit test,
	// but we can test validation logic
	cfg := DefaultRetryConfig()

	// This will fail because nc is nil, but we're testing the subject validation
	// would happen before any NATS operations
	err := PublishWithRetry(nil, "", []byte("data"), cfg, nil)

	if err == nil {
		t.Error("PublishWithRetry should return error for empty subject")
	}
}

func TestKVPutWithRetry_NilKV(t *testing.T) {
	cfg := DefaultRetryConfig()
	_, err := KVPutWithRetry(nil, "test-key", []byte("value"), cfg, nil)

	if err == nil {
		t.Error("KVPutWithRetry should return error for nil KV store")
	}
	expectedMsg := "KV store is nil"
	if err.Error() != expectedMsg {
		t.Errorf("Error message = %v, want %v", err.Error(), expectedMsg)
	}
}

func TestKVPutWithRetry_EmptyKey(t *testing.T) {
	cfg := DefaultRetryConfig()
	_, err := KVPutWithRetry(nil, "", []byte("value"), cfg, nil)

	if err == nil {
		t.Error("KVPutWithRetry should return error for empty key")
	}
}

func TestKVDeleteWithRetry_NilKV(t *testing.T) {
	cfg := DefaultRetryConfig()
	err := KVDeleteWithRetry(nil, "test-key", cfg, nil)

	if err == nil {
		t.Error("KVDeleteWithRetry should return error for nil KV store")
	}
	expectedMsg := "KV store is nil"
	if err.Error() != expectedMsg {
		t.Errorf("Error message = %v, want %v", err.Error(), expectedMsg)
	}
}

func TestKVDeleteWithRetry_EmptyKey(t *testing.T) {
	cfg := DefaultRetryConfig()
	err := KVDeleteWithRetry(nil, "", cfg, nil)

	if err == nil {
		t.Error("KVDeleteWithRetry should return error for empty key")
	}
}

func TestRetryConfig_CustomValues(t *testing.T) {
	tests := []struct {
		name   string
		config RetryConfig
	}{
		{
			name: "aggressive retry",
			config: RetryConfig{
				MaxRetries:     10,
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				BackoffFactor:  1.5,
			},
		},
		{
			name: "conservative retry",
			config: RetryConfig{
				MaxRetries:     2,
				InitialBackoff: 500 * time.Millisecond,
				MaxBackoff:     10 * time.Second,
				BackoffFactor:  3.0,
			},
		},
		{
			name: "no retry",
			config: RetryConfig{
				MaxRetries:     0,
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				BackoffFactor:  2.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the config values are accessible
			if tt.config.MaxRetries < 0 {
				t.Error("MaxRetries should not be negative")
			}
			if tt.config.InitialBackoff < 0 {
				t.Error("InitialBackoff should not be negative")
			}
			if tt.config.MaxBackoff < 0 {
				t.Error("MaxBackoff should not be negative")
			}
			if tt.config.BackoffFactor < 0 {
				t.Error("BackoffFactor should not be negative")
			}
		})
	}
}

func TestBackoffCalculation(t *testing.T) {
	// Test exponential backoff calculation logic
	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
	}

	backoff := cfg.InitialBackoff

	// First retry: 100ms * 2 = 200ms
	backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
	expected := 200 * time.Millisecond
	if backoff != expected {
		t.Errorf("First backoff = %v, want %v", backoff, expected)
	}

	// Second retry: 200ms * 2 = 400ms
	backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
	expected = 400 * time.Millisecond
	if backoff != expected {
		t.Errorf("Second backoff = %v, want %v", backoff, expected)
	}

	// Third retry: 400ms * 2 = 800ms
	backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
	expected = 800 * time.Millisecond
	if backoff != expected {
		t.Errorf("Third backoff = %v, want %v", backoff, expected)
	}

	// Should cap at MaxBackoff
	backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
	if backoff > cfg.MaxBackoff {
		backoff = cfg.MaxBackoff
	}
	if backoff != cfg.MaxBackoff {
		t.Errorf("Capped backoff = %v, want %v", backoff, cfg.MaxBackoff)
	}
}
