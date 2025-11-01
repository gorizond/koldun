package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestServerSidecarEndpoint(t *testing.T) {
	server := &Server{
		cfg: Config{
			SidecarURL: "http://127.0.0.1:9000/base",
		},
		log: logrus.New().WithField("component", "test"),
	}

	u, err := server.sidecarEndpoint("/v1/health")
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:9000/v1/health", u.String())
}

func TestServerSidecarEndpointInvalidURL(t *testing.T) {
	server := &Server{
		cfg: Config{
			SidecarURL: "://invalid-url",
		},
		log: logrus.New().WithField("component", "test"),
	}

	_, err := server.sidecarEndpoint("/v1/health")
	require.Error(t, err)
}

func TestServerWaitForSidecarSuccess(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/models", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}
	ts := httptest.NewServer(http.HandlerFunc(handler))
	defer ts.Close()

	client := ts.Client()
	client.Timeout = 500 * time.Millisecond

	server := &Server{
		cfg: Config{
			SidecarURL:     ts.URL,
			SidecarTimeout: 500 * time.Millisecond,
		},
		client: client,
		log:    logrus.New().WithField("component", "test"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, server.waitForSidecar(ctx))
}

func TestServerWaitForSidecarTimeout(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	ts := httptest.NewServer(http.HandlerFunc(handler))
	defer ts.Close()

	client := ts.Client()
	client.Timeout = 500 * time.Millisecond

	server := &Server{
		cfg: Config{
			SidecarURL:     ts.URL,
			SidecarTimeout: 150 * time.Millisecond,
		},
		client: client,
		log:    logrus.New().WithField("component", "test"),
	}

	err := server.waitForSidecar(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout waiting for dllama-api sidecar")
}

func TestConsumerLastActivity(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-time.Minute)

	t.Run("delivered last", func(t *testing.T) {
		info := &nats.ConsumerInfo{
			Delivered: nats.SequenceInfo{Last: &now},
			AckFloor:  nats.SequenceInfo{Last: &earlier},
			Created:   earlier,
		}
		require.Equal(t, now, consumerLastActivity(info))
	})

	t.Run("ack floor last", func(t *testing.T) {
		info := &nats.ConsumerInfo{
			AckFloor: nats.SequenceInfo{Last: &now},
			Created:  earlier,
		}
		require.Equal(t, now, consumerLastActivity(info))
	})

	t.Run("fallback to created", func(t *testing.T) {
		info := &nats.ConsumerInfo{
			Created: earlier,
		}
		require.Equal(t, earlier, consumerLastActivity(info))
	})

	t.Run("nil info", func(t *testing.T) {
		require.True(t, consumerLastActivity(nil).IsZero())
	})
}

func TestServerSetShutdownErr(t *testing.T) {
	server := &Server{}
	errPrimary := errors.New("primary failure")
	server.setShutdownErr(errPrimary)
	require.Equal(t, errPrimary, server.shutdownError())

	errSecondary := errors.New("secondary failure")
	server.setShutdownErr(errSecondary)
	require.Equal(t, errPrimary, server.shutdownError(), "shutdown error should not be overwritten once set")

	server.setShutdownErr(nil)
	require.Equal(t, errPrimary, server.shutdownError(), "nil error should not change recorded shutdown error")
}

func TestEnsureRequestStreamCreatesAndUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	ns := startJetStreamServer(t)
	js, _ := connectJetStream(t, ns)

	name, err := ensureRequestStream(js, "tenant.in.")
	require.NoError(t, err)
	require.Equal(t, llmRequestStreamName, name)

	info, err := js.StreamInfo(llmRequestStreamName)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"in.>", "tenant.in.>"}, info.Config.Subjects)

	// Second call with a new prefix adds it to the existing subjects.
	name, err = ensureRequestStream(js, "tenant.extra.")
	require.NoError(t, err)
	require.Equal(t, llmRequestStreamName, name)

	info, err = js.StreamInfo(llmRequestStreamName)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"in.>", "tenant.in.>", "tenant.extra.>"}, info.Config.Subjects)
}

func TestEnsureRequestStreamInvalidPrefix(t *testing.T) {
	_, err := ensureRequestStream(nil, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "in-prefix is required")

	_, err = ensureRequestStream(nil, "prefix")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must end with '.'")
}

func TestPublishErrorUsesTargetOrFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS dependent test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	logger := logrus.New().WithField("component", "test")
	srv := &Server{
		nc:         nc,
		outSubject: "out.subject",
		log:        logger,
	}

	customSub, err := nc.SubscribeSync("custom.subject")
	require.NoError(t, err)
	defaultSub, err := nc.SubscribeSync("out.subject")
	require.NoError(t, err)

	require.NoError(t, srv.nc.Flush())

	srv.publishError("custom.subject", "custom failure")
	require.NoError(t, srv.nc.Flush())

	msg, err := customSub.NextMsg(time.Second)
	require.NoError(t, err)
	var payload map[string]string
	require.NoError(t, json.Unmarshal(msg.Data, &payload))
	require.Equal(t, "custom failure", payload["error"])
	msg, err = customSub.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, "[DONE]", string(msg.Data))

	srv.publishError("", "fallback failure")
	require.NoError(t, srv.nc.Flush())

	msg, err = defaultSub.NextMsg(time.Second)
	require.NoError(t, err)
	payload = make(map[string]string)
	require.NoError(t, json.Unmarshal(msg.Data, &payload))
	require.Equal(t, "fallback failure", payload["error"])
	msg, err = defaultSub.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, "[DONE]", string(msg.Data))
}

func TestProbeSidecarUpdatesMetrics(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()

	server := &Server{
		cfg: Config{
			SidecarURL:     healthy.URL,
			DllamaName:     "dllama-healthy",
			SidecarTimeout: 200 * time.Millisecond,
		},
		client: healthy.Client(),
		log:    logrus.New().WithField("component", "test"),
	}

	require.True(t, server.probeSidecar(context.Background()))
	value := testutil.ToFloat64(metrics.LLMSidecarHealthStatus.WithLabelValues("dllama-healthy"))
	require.Equal(t, float64(1), value)

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer unhealthy.Close()

	server = &Server{
		cfg: Config{
			SidecarURL:     unhealthy.URL,
			DllamaName:     "dllama-unhealthy",
			SidecarTimeout: 200 * time.Millisecond,
		},
		client: unhealthy.Client(),
		log:    logrus.New().WithField("component", "test"),
	}

	require.False(t, server.probeSidecar(context.Background()))
	value = testutil.ToFloat64(metrics.LLMSidecarHealthStatus.WithLabelValues("dllama-unhealthy"))
	require.Equal(t, float64(0), value)
}

func startJetStreamServer(t *testing.T) *server.Server {
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

func connectJetStream(t *testing.T, ns *server.Server) (nats.JetStreamContext, *nats.Conn) {
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
