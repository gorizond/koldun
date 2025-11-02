package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

type staticResponseTransport struct {
	resp *http.Response
	err  error
}

func (s staticResponseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

type failingStreamReader struct {
	sent bool
}

func (r *failingStreamReader) Read(p []byte) (int, error) {
	if !r.sent {
		data := []byte("data: {\"id\":\"chunk-1\"}\n\n")
		copy(p, data)
		r.sent = true
		return len(data), nil
	}
	return 0, errors.New("stream read failure")
}

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

func TestHandleHealth(t *testing.T) {
	t.Run("disconnected", func(t *testing.T) {
		srv := &Server{}

		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rr := httptest.NewRecorder()
		srv.handleHealth(rr, req)

		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	})

	t.Run("connected", func(t *testing.T) {
		ns := startJetStreamServer(t)
		_, nc := connectJetStream(t, ns)

		srv := &Server{
			nc:  nc,
			log: logrus.New().WithField("component", "test"),
		}

		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rr := httptest.NewRecorder()
		srv.handleHealth(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		require.Equal(t, "ok", rr.Body.String())
	})
}

func TestNewRequiresHash(t *testing.T) {
	_, err := New(Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Hash")
}

func TestNewAppliesDefaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	ns := startJetStreamServer(t)

	cfg := Config{
		Hash:     "tenant-1",
		NATSURL:  ns.ClientURL(),
		InPrefix: "tenant.in.",
		Logger:   logrus.New().WithField("component", "test"),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	t.Cleanup(func() {
		if srv.sub != nil {
			_ = srv.sub.Unsubscribe()
		}
		if srv.nc != nil {
			_ = srv.nc.Drain()
			srv.nc.Close()
		}
	})

	require.Equal(t, defaultListenAddress, srv.cfg.ListenAddress)
	require.Equal(t, cfg.InPrefix, srv.cfg.InPrefix)
	require.Equal(t, defaultOutPrefix, srv.cfg.OutPrefix)
	require.Equal(t, cfg.Hash, srv.cfg.DllamaName)
	require.Equal(t, srv.cfg.RequestSubject, srv.inSubject)
	require.Equal(t, srv.cfg.OutPrefix+srv.cfg.Hash, srv.outSubject)
	require.Equal(t, srv.cfg.StateSubject, srv.stateSubject)
	require.Equal(t, llmRequestStreamName, srv.streamName)

	info, err := srv.js.StreamInfo(srv.streamName)
	require.NoError(t, err)
	require.Contains(t, info.Config.Subjects, defaultInPrefix+">")
	require.Contains(t, info.Config.Subjects, srv.cfg.InPrefix+">")
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

func TestStreamToSidecarPublishesChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, sidecarChatCompletionsPath, r.URL.Path)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		require.True(t, ok)
		_, _ = w.Write([]byte("data: {\"id\":\"chunk-1\"}\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte("data: {\"id\":\"chunk-2\"}\n\n"))
		flusher.Flush()
	}))
	t.Cleanup(sidecar.Close)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-stream",
			SidecarURL: sidecar.URL,
		},
		nc:         nc,
		client:     sidecar.Client(),
		log:        logrus.NewEntry(logger),
		outSubject: "stream.responses",
	}

	sub, err := nc.SubscribeSync("stream.responses")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		Request: openai.ChatCompletionRequest{
			Stream: true,
		},
	}

	err = srv.streamToSidecar(payload)
	require.NoError(t, err)

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"chunk-1"}`, string(msg.Data))

	msg, err = sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"chunk-2"}`, string(msg.Data))

	msg, err = sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, "[DONE]", string(msg.Data))
}

func TestStreamToSidecarPublishesErrorOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("sidecar failure"))
	}))
	t.Cleanup(sidecar.Close)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-stream",
			SidecarURL: sidecar.URL,
		},
		nc:     nc,
		client: sidecar.Client(),
		log:    logrus.NewEntry(logger),
	}

	sub, err := nc.SubscribeSync("custom.responses")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		ResponseSubject: "custom.responses",
		Request: openai.ChatCompletionRequest{
			Stream: true,
		},
	}

	err = srv.streamToSidecar(payload)
	require.Error(t, err)

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)
	var body map[string]string
	require.NoError(t, json.Unmarshal(msg.Data, &body))
	require.Contains(t, body["error"], "sidecar responded 500")

	msg, err = sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, "[DONE]", string(msg.Data))
}

func TestStreamToSidecarClientErrorPublishesError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	clientErr := errors.New("client boom")

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-stream",
			SidecarURL: "http://localhost:12345",
		},
		nc:         nc,
		client:     &http.Client{Transport: staticResponseTransport{err: clientErr}},
		log:        logrus.NewEntry(logrus.New()),
		outSubject: "stream.responses",
	}

	sub, err := nc.SubscribeSync("stream.responses")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		Request: openai.ChatCompletionRequest{
			Stream: true,
		},
	}

	err = srv.streamToSidecar(payload)
	require.ErrorIs(t, err, clientErr)

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)

	var body map[string]string
	require.NoError(t, json.Unmarshal(msg.Data, &body))
	require.Contains(t, body["error"], "client boom")

	msg, err = sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, "[DONE]", string(msg.Data))
}

func TestStreamToSidecarScannerError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	reader := &failingStreamReader{}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(reader),
		Header:     make(http.Header),
	}
	resp.Header.Set("Content-Type", "text/event-stream")

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-stream",
			SidecarURL: "http://localhost:12345",
		},
		nc:         nc,
		client:     &http.Client{Transport: staticResponseTransport{resp: resp}},
		log:        logrus.NewEntry(logger),
		outSubject: "stream.responses",
	}

	sub, err := nc.SubscribeSync("stream.responses")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		Request: openai.ChatCompletionRequest{
			Stream: true,
		},
	}

	err = srv.streamToSidecar(payload)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream read failure")

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"chunk-1"}`, string(msg.Data))

	_, err = sub.NextMsg(250 * time.Millisecond)
	require.ErrorIs(t, err, nats.ErrTimeout)
}

func TestStreamToSidecarDonePublishFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping streaming test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, sidecarChatCompletionsPath, r.URL.Path)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		require.True(t, ok)
		_, _ = w.Write([]byte("data: {\"id\":\"chunk-1\"}\n\n"))
		flusher.Flush()
	}))
	t.Cleanup(sidecar.Close)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-stream",
			SidecarURL: sidecar.URL,
		},
		nc:         nc,
		client:     sidecar.Client(),
		log:        logrus.NewEntry(logger),
		outSubject: "stream.responses",
	}

	nc.Close()

	payload := inboundRequest{
		Request: openai.ChatCompletionRequest{
			Stream: true,
		},
	}

	err := srv.streamToSidecar(payload)
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrConnectionClosed)
}

func TestExecuteOncePublishesResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping executeOnce test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, sidecarChatCompletionsPath, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"once-success"}`))
	}))
	t.Cleanup(sidecar.Close)

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	srv := &Server{
		cfg: Config{
			DllamaName:     "dllama-execute",
			SidecarURL:     sidecar.URL,
			SidecarTimeout: time.Second,
		},
		nc:         nc,
		client:     sidecar.Client(),
		log:        logrus.NewEntry(logger),
		outSubject: "execute.responses",
	}

	sub, err := nc.SubscribeSync("execute.responses")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		ResponseSubject: "execute.responses",
		Request: openai.ChatCompletionRequest{
			Model: "model",
		},
	}

	require.NoError(t, srv.executeOnce(payload))

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"once-success"}`, string(msg.Data))
}

func TestExecuteOncePublishesErrorOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping executeOnce test in short mode")
	}

	ns := startJetStreamServer(t)
	_, nc := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(sidecar.Close)

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-execute",
			SidecarURL: sidecar.URL,
		},
		nc:         nc,
		client:     sidecar.Client(),
		log:        logrus.NewEntry(logger),
		outSubject: "execute.errors",
	}

	sub, err := nc.SubscribeSync("execute.errors")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	payload := inboundRequest{
		ResponseSubject: "execute.errors",
		Request: openai.ChatCompletionRequest{
			Model: "model",
		},
	}

	err = srv.executeOnce(payload)
	require.Error(t, err)

	msg, err := sub.NextMsg(time.Second)
	require.NoError(t, err)
	var body map[string]string
	require.NoError(t, json.Unmarshal(msg.Data, &body))
	require.Contains(t, body["error"], "sidecar responded 500")

	msg, err = sub.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, "[DONE]", string(msg.Data))
}

func TestRunStartsAndStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS dependent test in short mode")
	}

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sidecarModelsPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer sidecar.Close()

	ns := startJetStreamServer(t)

	cfg := Config{
		Hash:           "tenant-run",
		NATSURL:        ns.ClientURL(),
		InPrefix:       "tenant.in.",
		SidecarURL:     sidecar.URL,
		SidecarTimeout: 500 * time.Millisecond,
		Logger:         logrus.New().WithField("component", "test"),
		HealthOnly:     true,
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, srv)

	srv.client = sidecar.Client()
	srv.cfg.SidecarMonitorInterval = 0
	srv.cfg.SidecarFailureThreshold = 0
	srv.sidecarMonitorInterval = 0
	srv.sidecarFailureThreshold = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return srv.sub != nil
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	if srv.sub != nil {
		_ = srv.sub.Unsubscribe()
	}
	if srv.nc != nil {
		_ = srv.nc.Drain()
		srv.nc.Close()
	}
}

func TestEnsureQueueSubscriptionUpdatesExistingConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NATS dependent test in short mode")
	}

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sidecarModelsPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer sidecar.Close()

	ns := startJetStreamServer(t)

	cfg := Config{
		Hash:           "tenant-sub",
		NATSURL:        ns.ClientURL(),
		SidecarURL:     sidecar.URL,
		SidecarTimeout: 500 * time.Millisecond,
		Logger:         logrus.New().WithField("component", "test"),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	srv.client = sidecar.Client()

	queue := durableName(srv.cfg.DllamaName)

	sub, err := srv.ensureQueueSubscription(queue, 250*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.NoError(t, srv.nc.Flush())

	require.NoError(t, sub.Unsubscribe())

	_, err = srv.ensureQueueSubscription(queue, 400*time.Millisecond)
	require.NoError(t, err)

	info, err := srv.js.ConsumerInfo(srv.streamName, queue)
	require.NoError(t, err)
	require.Equal(t, 400*time.Millisecond, info.Config.AckWait)

	if srv.nc != nil {
		_ = srv.nc.Drain()
		srv.nc.Close()
	}
}

func TestEnsureQueueSubscriptionReturnsLookupError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startJetStreamServer(t)
	js, nc := connectJetStream(t, ns)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	srv := &Server{
		js:         js,
		nc:         nc,
		log:        logrus.NewEntry(logger),
		inSubject:  "tenant.in.hash",
		streamName: "missing-stream",
	}

	_, err := srv.ensureQueueSubscription("queue-name", time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrStreamNotFound)
	require.Contains(t, err.Error(), "lookup consumer")
}

func TestEnsureQueueSubscriptionRejectsWrongFilterSubject(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startJetStreamServer(t)
	js, _ := connectJetStream(t, ns)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sidecarModelsPath {
			w.WriteHeader(http.StatusOK)
			return
		}
	}))
	defer sidecar.Close()

	cfg := Config{
		Hash:           "test-hash",
		NATSURL:        ns.ClientURL(),
		SidecarURL:     sidecar.URL,
		SidecarTimeout: 500 * time.Millisecond,
		Logger:         logrus.New().WithField("component", "test"),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	srv.client = sidecar.Client()

	queue := durableName(srv.cfg.DllamaName)

	// Create consumer with different filter subject
	_, err = js.AddConsumer(srv.streamName, &nats.ConsumerConfig{
		Durable:       queue,
		FilterSubject: "wrong.subject",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require.NoError(t, err)

	// Attempt to subscribe should fail due to filter mismatch
	_, err = srv.ensureQueueSubscription(queue, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected filter")

	if srv.nc != nil {
		_ = srv.nc.Drain()
		srv.nc.Close()
	}
}

func TestEnsureQueueSubscriptionHandlesUpdateConsumerError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream dependent test in short mode")
	}

	ns := startJetStreamServer(t)

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sidecarModelsPath {
			w.WriteHeader(http.StatusOK)
			return
		}
	}))
	defer sidecar.Close()

	cfg := Config{
		Hash:           "test-hash-update",
		NATSURL:        ns.ClientURL(),
		SidecarURL:     sidecar.URL,
		SidecarTimeout: 500 * time.Millisecond,
		Logger:         logrus.New().WithField("component", "test"),
	}

	srv, err := New(cfg)
	require.NoError(t, err)
	srv.client = sidecar.Client()

	queue := durableName(srv.cfg.DllamaName)

	// Create initial consumer with different AckWait
	sub1, err := srv.ensureQueueSubscription(queue, 100*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, sub1)

	// Close NATS connection to force UpdateConsumer to fail
	if srv.nc != nil {
		srv.nc.Close()
	}

	// Reconnect with new connection
	nc2, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	js2, err := nc2.JetStream()
	require.NoError(t, err)

	srv.nc = nc2
	srv.js = js2

	// Now try to subscribe again with different ackWait - UpdateConsumer will fail
	// but the function should continue and attempt QueueSubscribe
	sub2, err := srv.ensureQueueSubscription(queue, 300*time.Millisecond)
	// Should still succeed in subscribing even if update fails
	require.NoError(t, err)
	require.NotNil(t, sub2)

	if srv.nc != nil {
		_ = srv.nc.Drain()
		srv.nc.Close()
	}
}

func TestEvictDllamaDisabled(t *testing.T) {
	srv := &Server{
		cfg: Config{
			DllamaName: "",
		},
		namespace: "",
	}

	err := srv.evictDllama(context.Background())
	require.ErrorIs(t, err, errEvictionDisabled)
}

func TestEvictDllamaWithoutKube(t *testing.T) {
	srv := &Server{
		cfg: Config{
			DllamaName: "test-dllama",
		},
		namespace: "default",
		kube:      nil,
	}

	err := srv.evictDllama(context.Background())
	require.ErrorIs(t, err, errEvictionDisabled)
}

func TestEvictDllamaWithoutNamespace(t *testing.T) {
	srv := &Server{
		cfg: Config{
			DllamaName: "test-dllama",
		},
		namespace: "",
	}

	err := srv.evictDllama(context.Background())
	require.ErrorIs(t, err, errEvictionDisabled)
}

func TestTriggerEvictionSetsShutdownError(t *testing.T) {
	var cancelled bool
	srv := &Server{
		cfg: Config{
			DllamaName: "dllama-evict",
		},
		log:    logrus.New().WithField("component", "test"),
		cancel: func() { cancelled = true },
	}

	srv.triggerEviction("sidecar failure")
	require.Error(t, srv.shutdownError())
	require.True(t, cancelled)

	// Ensure second trigger does not overwrite error
	srv.triggerEviction("duplicate")
	err := srv.shutdownError()
	require.EqualError(t, err, "sidecar failure")
}

func TestMonitorSidecarTriggersEviction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping monitor test in short mode")
	}

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusInternalServerError)
	}))
	defer sidecar.Close()

	srv := &Server{
		cfg: Config{
			DllamaName:              "dllama-monitor",
			SidecarURL:              sidecar.URL,
			SidecarTimeout:          50 * time.Millisecond,
			SidecarMonitorInterval:  10 * time.Millisecond,
			SidecarFailureThreshold: 1,
		},
		client:                  sidecar.Client(),
		log:                     logrus.New().WithField("component", "test"),
		sidecarMonitorInterval:  10 * time.Millisecond,
		sidecarFailureThreshold: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.monitorSidecar(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return srv.shutdownError() != nil
	}, time.Second, 20*time.Millisecond)

	cancel()
	<-done
	require.Error(t, srv.shutdownError())
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

func TestCleanupInactiveConsumersWithNilJetStream(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	cleanupInactiveConsumers(nil, "test-stream", logger)
}

func TestCleanupInactiveConsumersWithEmptyStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream test in short mode")
	}

	ns := startJetStreamServer(t)
	js, _ := connectJetStream(t, ns)
	logger := logrus.NewEntry(logrus.New())

	cleanupInactiveConsumers(js, "", logger)
	cleanupInactiveConsumers(js, "   ", logger)
}

func TestInClusterNamespaceReturnsEmpty(t *testing.T) {
	ns := inClusterNamespace()
	if ns != "" {
		t.Logf("Running in cluster, namespace: %s", ns)
	}
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
