package dispatcher

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/testutil"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

const dispatcherTestNATSEnvVar = "KOLDUN_DISPATCHER_NATS_URL"

// testNATSURL returns a NATS endpoint for dispatcher tests. It reuses an external
// compose stack when KOLDUN_DISPATCHER_NATS_URL is set, otherwise it spins up an
// embedded JetStream server that mirrors the previous behavior.
func testNATSURL(t *testing.T) string {
	t.Helper()
	testutil.RequireLoopback(t)

	if external := strings.TrimSpace(os.Getenv(dispatcherTestNATSEnvVar)); external != "" {
		ensureJetStreamReady(t, external)
		return external
	}

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

	ensureJetStreamReady(t, ns.ClientURL())

	t.Cleanup(func() {
		ns.Shutdown()
	})

	return ns.ClientURL()
}

func ensureJetStreamReady(t *testing.T, url string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
		if err == nil {
			if js, jsErr := nc.JetStream(); jsErr == nil {
				if _, infoErr := js.AccountInfo(); infoErr == nil {
					_ = nc.Drain()
					nc.Close()
					return
				} else {
					err = infoErr
				}
			} else {
				err = jsErr
			}
			_ = nc.Drain()
			nc.Close()
		}

		if time.Now().After(deadline) {
			t.Fatalf("NATS JetStream not ready for %s: %v", url, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func cleanupDispatcherBucket(t *testing.T, js nats.JetStreamContext, bucket string) {
	t.Helper()

	if js == nil || strings.TrimSpace(bucket) == "" {
		return
	}

	if err := js.DeleteKeyValue(bucket); err != nil && !errors.Is(err, nats.ErrBucketNotFound) {
		t.Logf("cleanup bucket %s: %v", bucket, err)
	}
}
