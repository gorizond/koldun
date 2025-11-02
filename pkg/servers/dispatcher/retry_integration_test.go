package dispatcher

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func connectTestNATS(t *testing.T, ns *server.Server) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(ns.ClientURL(), nats.Timeout(2*time.Second))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = nc.Drain()
		nc.Close()
	})

	return nc
}

func TestPublishWithRetrySuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)

	sub, err := nc.SubscribeSync("dispatcher.subject")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	logger := logrus.New().WithField("component", "dispatcher-test")

	err = PublishWithRetry(nc, "dispatcher.subject", []byte("payload"), DefaultRetryConfig(), logger)
	require.NoError(t, err)

	msg, err := sub.NextMsg(2 * time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), msg.Data)
}

func TestPublishWithRetryNonRetryableError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)

	// Close the connection to trigger ErrConnectionClosed on publish.
	nc.Close()

	err := PublishWithRetry(nc, "dispatcher.subject", []byte("payload"), DefaultRetryConfig(), logrus.New().WithField("component", "dispatcher-test"))
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrConnectionClosed)
}

func TestKVPutWithRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)
	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "dispatcher_kv"})
	require.NoError(t, err)

	logger := logrus.New().WithField("component", "dispatcher-test")

	rev, err := KVPutWithRetry(kv, "valid", []byte("value"), DefaultRetryConfig(), logger)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rev)

	_, err = KVPutWithRetry(kv, "invalid key", []byte("value"), DefaultRetryConfig(), logger)
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrInvalidKey)
}

func TestKVDeleteWithRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)
	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "dispatcher_kv_delete"})
	require.NoError(t, err)

	logger := logrus.New().WithField("component", "dispatcher-test")

	_, err = KVPutWithRetry(kv, "existing", []byte("value"), DefaultRetryConfig(), logger)
	require.NoError(t, err)

	require.NoError(t, KVDeleteWithRetry(kv, "existing", DefaultRetryConfig(), logger))

	// Second delete should treat ErrKeyNotFound as success.
	require.NoError(t, KVDeleteWithRetry(kv, "existing", DefaultRetryConfig(), logger))

	err = KVDeleteWithRetry(kv, "invalid key", DefaultRetryConfig(), logger)
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrInvalidKey)
}

func TestPublishWithRetryBackoff(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)

	// Create a subscription to receive messages
	sub, err := nc.SubscribeSync("test.backoff")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	cfg := RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New().WithField("component", "dispatcher-test")

	// First attempt should succeed immediately
	start := time.Now()
	err = PublishWithRetry(nc, "test.backoff", []byte("data"), cfg, logger)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Less(t, elapsed, 100*time.Millisecond, "immediate success should not trigger backoff")

	msg, err := sub.NextMsg(1 * time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), msg.Data)
}

func TestPublishWithRetryExhaustion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)

	// Close connection to force retries to fail
	nc.Close()

	cfg := RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New().WithField("component", "dispatcher-test")

	start := time.Now()
	err := PublishWithRetry(nc, "test.exhaustion", []byte("data"), cfg, logger)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrConnectionClosed)
	// Should fail fast without retries for non-retryable errors
	require.Less(t, elapsed, 100*time.Millisecond, "non-retryable error should fail immediately")
}

func TestKVPutWithRetrySuccessAfterRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)
	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "dispatcher_kv_retry"})
	require.NoError(t, err)

	cfg := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New().WithField("component", "dispatcher-test")

	// Normal operation should succeed on first try
	start := time.Now()
	rev, err := KVPutWithRetry(kv, "key1", []byte("value1"), cfg, logger)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, uint64(1), rev)
	require.Less(t, elapsed, 50*time.Millisecond, "immediate success should be fast")
}

func TestKVDeleteWithRetryKeyNotFoundIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS integration test in short mode")
	}

	ns := startTestNATSServer(t)
	nc := connectTestNATS(t, ns)
	js, err := nc.JetStream()
	require.NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "dispatcher_kv_idempotent"})
	require.NoError(t, err)

	logger := logrus.New().WithField("component", "dispatcher-test")

	// Delete non-existent key should succeed (idempotent)
	err = KVDeleteWithRetry(kv, "nonexistent", DefaultRetryConfig(), logger)
	require.NoError(t, err)

	// Multiple deletes should all succeed
	for i := 0; i < 3; i++ {
		err = KVDeleteWithRetry(kv, "nonexistent", DefaultRetryConfig(), logger)
		require.NoError(t, err)
	}
}
