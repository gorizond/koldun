package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/api/openai"
	"github.com/gorizond/koldun/pkg/conversation"
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

func TestHandleMessagePublishesStateAndResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/chat/completions", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp-1"}`))
	}))
	t.Cleanup(sidecar.Close)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName:     "dllama-handle",
			SidecarURL:     sidecar.URL,
			SidecarTimeout: time.Second,
		},
		nc:           nc,
		client:       sidecar.Client(),
		log:          logrus.NewEntry(logger),
		outSubject:   "responses.subject",
		stateSubject: "state.subject",
	}

	stateSub, err := nc.SubscribeSync("state.subject")
	require.NoError(t, err)
	respSub, err := nc.SubscribeSync("responses.subject")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		Hash:            "hash-1",
		ChatID:          "chat-1",
		Model:           "tenant/model",
		Namespace:       "tenant",
		ResponseSubject: "responses.subject",
		Request: openai.ChatCompletionRequest{
			Model: "tenant/model",
		},
	}
	env := conversation.AssignmentEnvelope{
		AssignmentID: "assignment-1",
		Payload:      mustJSON(t, payload),
	}

	msg := &nats.Msg{Data: mustJSON(t, env)}

	successBefore := testutil.ToFloat64(metrics.LLMRequestsTotal.WithLabelValues("success"))

	srv.handleMessage(msg)
	require.NoError(t, nc.Flush())

	successAfter := testutil.ToFloat64(metrics.LLMRequestsTotal.WithLabelValues("success"))
	require.Equal(t, successBefore+1, successAfter, "success counter should increment")

	busyMsg, err := stateSub.NextMsg(time.Second)
	require.NoError(t, err)
	var busy conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(busyMsg.Data, &busy))
	require.Equal(t, "busy", busy.State)
	require.Equal(t, "assignment-1", busy.AssignmentID)
	require.Equal(t, "dllama-handle", busy.Dllama)

	respMsg, err := respSub.NextMsg(time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"resp-1"}`, string(respMsg.Data))

	idleMsg, err := stateSub.NextMsg(time.Second)
	require.NoError(t, err)
	var idle conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(idleMsg.Data, &idle))
	require.Equal(t, "idle", idle.State)
	require.Equal(t, "dllama-handle", idle.Dllama)
	require.Zero(t, idle.Active)
}

func TestStartHeartbeatLoopPublishesIdleImmediately(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-heartbeat",
		},
		nc:           nc,
		log:          logrus.NewEntry(logger),
		stateSubject: "sessions.hash.dllama.worker.state",
	}

	sub, err := nc.SubscribeSync("sessions.hash.dllama.worker.state")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	ctx, cancel := context.WithCancel(context.Background())
	srv.startHeartbeatLoop(ctx)

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)

	var event conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(msg.Data, &event))
	require.Equal(t, "idle", event.State)
	require.Equal(t, "dllama-heartbeat", event.Dllama)

	cancel()
	srv.wg.Wait()
}

func TestStartHeartbeatLoopPublishesPeriodicIdleState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	delay := 25 * time.Millisecond

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName:        "dllama-heartbeat",
			HeartbeatInterval: delay,
		},
		nc:           nc,
		log:          logrus.NewEntry(logger),
		stateSubject: "sessions.hash.dllama.worker.state",
	}

	sub, err := nc.SubscribeSync("sessions.hash.dllama.worker.state")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	ctx, cancel := context.WithCancel(context.Background())
	srv.startHeartbeatLoop(ctx)

	first, err := sub.NextMsg(time.Second)
	require.NoError(t, err)

	second, err := sub.NextMsg(time.Second)
	require.NoError(t, err)

	var immediate conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(first.Data, &immediate))
	require.Equal(t, "idle", immediate.State)
	require.Equal(t, "dllama-heartbeat", immediate.Dllama)

	var periodic conversation.WorkerStateEvent
	require.NoError(t, json.Unmarshal(second.Data, &periodic))
	require.Equal(t, "idle", periodic.State)
	require.Equal(t, "dllama-heartbeat", periodic.Dllama)

	cancel()
	srv.wg.Wait()
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

func mustJSON(t *testing.T, payload any) []byte {
	t.Helper()

	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}
