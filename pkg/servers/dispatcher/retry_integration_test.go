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
