package dispatcher

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()

	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.InitialBackoff)
	assert.Equal(t, 5*time.Second, cfg.MaxBackoff)
	assert.Equal(t, 2.0, cfg.BackoffFactor)
}

func TestPublishWithRetry_NilConnection(t *testing.T) {
	cfg := DefaultRetryConfig()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	err := PublishWithRetry(nil, "test.subject", []byte("data"), cfg, logrus.NewEntry(logger))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NATS connection is nil")
}

func TestPublishWithRetry_EmptySubject(t *testing.T) {
	// Create a mock connection (will fail, but we check error before using it)
	cfg := DefaultRetryConfig()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// We need a valid connection for this test
	opts := &server.Options{
		Port: -1,
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	defer ns.Shutdown()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	err = PublishWithRetry(nc, "", []byte("data"), cfg, logrus.NewEntry(logger))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subject is empty")
}

func TestPublishWithRetry_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start embedded NATS server
	opts := &server.Options{
		Port: -1,
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	defer ns.Shutdown()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	err = PublishWithRetry(nc, "test.subject", []byte("test data"), cfg, logrus.NewEntry(logger))
	assert.NoError(t, err)
}

// Note: NATS Core server doesn't always validate subject names strictly,
// so we can't reliably test for bad subject rejection.
// The retry logic will handle connection-level errors properly.

func TestKVPutWithRetry_NilKV(t *testing.T) {
	cfg := DefaultRetryConfig()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	_, err := KVPutWithRetry(nil, "key", []byte("value"), cfg, logrus.NewEntry(logger))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "KV store is nil")
}

func TestKVPutWithRetry_EmptyKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test-bucket",
	})
	require.NoError(t, err)

	cfg := DefaultRetryConfig()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	_, err = KVPutWithRetry(kv, "", []byte("value"), cfg, logrus.NewEntry(logger))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key is empty")
}

func TestKVPutWithRetry_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test-bucket",
	})
	require.NoError(t, err)

	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	revision, err := KVPutWithRetry(kv, "test-key", []byte("test value"), cfg, logrus.NewEntry(logger))
	assert.NoError(t, err)
	assert.Greater(t, revision, uint64(0))

	// Verify the value was stored
	entry, err := kv.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("test value"), entry.Value())
}

func TestKVDeleteWithRetry_NilKV(t *testing.T) {
	cfg := DefaultRetryConfig()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	err := KVDeleteWithRetry(nil, "key", cfg, logrus.NewEntry(logger))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "KV store is nil")
}

func TestKVDeleteWithRetry_EmptyKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test-bucket",
	})
	require.NoError(t, err)

	cfg := DefaultRetryConfig()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	err = KVDeleteWithRetry(kv, "", cfg, logrus.NewEntry(logger))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key is empty")
}

func TestKVDeleteWithRetry_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test-bucket",
	})
	require.NoError(t, err)

	// Put a value first
	_, err = kv.Put("test-key", []byte("test value"))
	require.NoError(t, err)

	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Delete should succeed
	err = KVDeleteWithRetry(kv, "test-key", cfg, logrus.NewEntry(logger))
	assert.NoError(t, err)

	// Verify the key was deleted
	_, err = kv.Get("test-key")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, nats.ErrKeyNotFound))
}

func TestKVDeleteWithRetry_KeyNotFoundIsSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test-bucket",
	})
	require.NoError(t, err)

	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Delete non-existent key should be treated as success (idempotent)
	err = KVDeleteWithRetry(kv, "non-existent-key", cfg, logrus.NewEntry(logger))
	assert.NoError(t, err, "deleting non-existent key should be idempotent")
}
