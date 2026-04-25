package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/api/openai"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/gorizond/koldun/pkg/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/utils/pointer"
)

const (
	defaultListenAddress           = ":8081"
	defaultSidecarURL              = "http://127.0.0.1:8080"
	defaultInPrefix                = "in."
	defaultOutPrefix               = "out."
	defaultSidecarMonitorInterval  = 60 * time.Second // Increased from 15s for CPU inference tolerance
	defaultSidecarFailureThreshold = 10               // Increased from 4 to give ~10min grace period

	sidecarModelsPath          = "/v1/models"
	sidecarChatCompletionsPath = "/v1/chat/completions"

	llmRequestStreamName = "KOLDUN_LLM_REQUESTS"

	maxNonStreamResponseSize = 4 << 20 // 4 MiB cap for single-response payloads

	consumerCleanupInterval      = 5 * time.Minute
	defaultInactiveConsumerGrace = 10 * time.Minute
	consumerListTimeout          = 5 * time.Second
	defaultHeartbeatInterval     = 30 * time.Second
)

var (
	dllamaGVR = schema.GroupVersionResource{
		Group:    "koldun.gorizond.io",
		Version:  "v1",
		Resource: "dllamas",
	}
	errEvictionDisabled   = errors.New("dllama eviction disabled: missing namespace or permissions")
	inactiveConsumerGrace = defaultInactiveConsumerGrace
)

func ensureTrailingDot(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	if strings.HasSuffix(prefix, ".") {
		return prefix
	}
	return prefix + "."
}

// Config holds runtime parameters for the LLM worker.
type Config struct {
	Hash string

	ListenAddress string
	HealthOnly    bool

	NATSURL   string
	InPrefix  string
	OutPrefix string

	RequestSubject string
	StateSubject   string
	DllamaName     string
	Namespace      string

	SidecarURL              string
	SidecarTimeout          time.Duration
	SidecarMonitorInterval  time.Duration
	SidecarFailureThreshold int
	HeartbeatInterval       time.Duration

	Logger *logrus.Entry
}

// Server subscribes to conversation requests and proxies them to a local dllama-api sidecar.
type Server struct {
	cfg Config
	log *logrus.Entry

	nc natsutil.NATSConn
	js nats.JetStreamContext

	httpServer *http.Server
	client     *http.Client

	sub          *nats.Subscription
	streamName   string
	inSubject    string
	outSubject   string
	stateSubject string

	namespace string
	kube      dynamic.Interface

	cancel      context.CancelFunc
	shutdownMu  sync.Mutex
	shutdownErr error
	evictOnce   sync.Once

	sidecarMonitorInterval  time.Duration
	sidecarFailureThreshold int

	wg sync.WaitGroup
}

type inboundRequest struct {
	Hash            string                       `json:"hash"`
	ChatID          string                       `json:"chatId"`
	ChatStart       string                       `json:"chatStart"`
	TokenHash       string                       `json:"tokenHash"`
	Model           string                       `json:"model"`
	Namespace       string                       `json:"namespace"`
	Request         openai.ChatCompletionRequest `json:"request"`
	ResponseSubject string                       `json:"responseSubject"`
	RequestID       string                       `json:"requestId,omitempty"`
}

// New constructs the LLM server and initialises the required clients.
func New(cfg Config) (*Server, error) {
	if strings.TrimSpace(cfg.Hash) == "" {
		return nil, errors.New("conversation hash (Hash) is required")
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = defaultListenAddress
	}
	if cfg.NATSURL == "" {
		cfg.NATSURL = nats.DefaultURL
	}
	if cfg.InPrefix == "" {
		cfg.InPrefix = defaultInPrefix
	}
	if cfg.OutPrefix == "" {
		cfg.OutPrefix = defaultOutPrefix
	}
	if !strings.HasSuffix(cfg.InPrefix, ".") {
		cfg.InPrefix = ensureTrailingDot(cfg.InPrefix)
	}
	if cfg.SidecarURL == "" {
		cfg.SidecarURL = defaultSidecarURL
	}
	if cfg.SidecarTimeout == 0 {
		cfg.SidecarTimeout = 2 * time.Minute
	}
	if strings.TrimSpace(cfg.RequestSubject) == "" {
		cfg.RequestSubject = cfg.InPrefix + cfg.Hash
	}
	if strings.TrimSpace(cfg.StateSubject) == "" {
		base := strings.TrimSuffix(cfg.RequestSubject, ".in")
		cfg.StateSubject = base + ".state"
	}
	if strings.TrimSpace(cfg.DllamaName) == "" {
		cfg.DllamaName = cfg.Hash
	}

	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	if cfg.Namespace == "" {
		cfg.Namespace = inClusterNamespace()
	}
	if cfg.SidecarMonitorInterval <= 0 {
		cfg.SidecarMonitorInterval = defaultSidecarMonitorInterval
	}
	if cfg.SidecarFailureThreshold <= 0 {
		cfg.SidecarFailureThreshold = defaultSidecarFailureThreshold
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}

	log := cfg.Logger
	if log == nil {
		log = logrus.StandardLogger().WithField("component", "koldun-llm")
	}

	natsConn, err := nats.Connect(cfg.NATSURL, nats.Name("koldun-llm"))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := natsConn.JetStream()
	if err != nil {
		natsConn.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}

	nc := natsutil.NewNATSConnWrapper(natsConn)

	streamName, err := ensureRequestStream(js, cfg.InPrefix)
	if err != nil {
		natsConn.Close()
		return nil, fmt.Errorf("request stream: %w", err)
	}

	client := &http.Client{Timeout: cfg.SidecarTimeout}

	var dynClient dynamic.Interface
	if cfg.Namespace != "" && strings.TrimSpace(cfg.DllamaName) != "" {
		restCfg, restErr := rest.InClusterConfig()
		if restErr != nil {
			log.WithError(restErr).Debug("in-cluster config unavailable; dllama eviction disabled")
		} else {
			dynClient, err = dynamic.NewForConfig(restCfg)
			if err != nil {
				log.WithError(err).Warn("failed to initialise dynamic client; dllama eviction disabled")
			}
		}
	}

	srv := &Server{
		cfg:                     cfg,
		log:                     log,
		nc:                      nc,
		js:                      js,
		client:                  client,
		inSubject:               cfg.RequestSubject,
		outSubject:              cfg.OutPrefix + cfg.Hash,
		stateSubject:            cfg.StateSubject,
		streamName:              streamName,
		namespace:               cfg.Namespace,
		kube:                    dynClient,
		sidecarMonitorInterval:  cfg.SidecarMonitorInterval,
		sidecarFailureThreshold: cfg.SidecarFailureThreshold,
	}
	return srv, nil
}

// Run starts the subscription loop and health endpoint until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.cancel = cancel

	if err := s.waitForSidecar(runCtx); err != nil {
		reason := fmt.Sprintf("dllama sidecar failed to become ready: %v", err)
		s.triggerEviction(reason)
		return fmt.Errorf("wait for dllama sidecar: %w", err)
	}

	// Use a durable name derived from the dllama identity so JetStream preserves
	// per-worker consumers while allowing us to restart safely.
	queueName := durableName(s.cfg.DllamaName)
	if queueName == "llm-default" {
		queueName = durableName(s.cfg.Hash)
	}
	ackWait := s.cfg.SidecarTimeout
	if ackWait <= 0 {
		ackWait = 2 * time.Minute
	} else {
		ackWait += 30 * time.Second
	}

	sub, err := s.ensureQueueSubscription(queueName, ackWait)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", s.inSubject, err)
	}
	s.sub = sub
	s.log.WithFields(logrus.Fields{
		"subject": s.inSubject,
		"queue":   queueName,
		"stream":  s.streamName,
	}).Info("subscribed to request stream")

	s.startHeartbeatLoop(runCtx)
	s.startConsumerCleanup(runCtx)
	go s.monitorSidecar(runCtx)

	errCh := make(chan error, 1)

	if !s.cfg.HealthOnly {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", s.handleHealth)
		mux.HandleFunc("/readyz", s.handleHealth)
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics.Handler().ServeHTTP(w, r)
		})

		s.httpServer = &http.Server{
			Addr:              s.cfg.ListenAddress,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		go func() {
			s.log.Infof("health server listening on %s", s.cfg.ListenAddress)
			if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}

	go func() {
		<-runCtx.Done()
		s.log.Info("llm server shutting down")
		if s.sub != nil {
			if err := s.sub.Drain(); err != nil {
				s.log.WithError(err).Warn("drain subscription")
			}
		}
		s.wg.Wait()
		s.nc.Drain()
		if s.httpServer != nil {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelShutdown()
			if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
				s.log.WithError(err).Warn("HTTP shutdown error")
			}
		}
	}()

	select {
	case <-runCtx.Done():
		if err := s.shutdownError(); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) waitForSidecar(ctx context.Context) error {
	endpoint, err := s.sidecarEndpoint(sidecarModelsPath)
	if err != nil {
		return err
	}

	s.log.WithField("endpoint", endpoint.String()).Info("waiting for dllama-api sidecar")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	deadline := time.Now().Add(s.cfg.SidecarTimeout)
	var lastErr error

	for {
		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			cancel()
			return fmt.Errorf("build sidecar probe: %w", err)
		}

		res, err := s.client.Do(req)
		cancel()

		if err == nil {
			var modelsResp struct {
				Data []struct {
					Id string `json:"id"`
				} `json:"data"`
			}
			decodeErr := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&modelsResp)
			res.Body.Close()

			if decodeErr != nil {
				err = fmt.Errorf("decode models response: %w", decodeErr)
			} else if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices {
				if len(modelsResp.Data) > 0 {
					s.log.WithFields(logrus.Fields{
						"endpoint": endpoint.String(),
						"model":    modelsResp.Data[0].Id,
					}).Info("dllama-api sidecar is ready")
					return nil
				}
				err = fmt.Errorf("dllama-api returned no models")
			} else {
				err = fmt.Errorf("unexpected status %d", res.StatusCode)
			}
		} else if ctx.Err() != nil {
			return nil
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("probe timed out: %w", err)
		}

		lastErr = err

		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timeout waiting for dllama-api sidecar: %w", lastErr)
			}
			return fmt.Errorf("timeout waiting for dllama-api sidecar")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Server) sidecarEndpoint(path string) (*url.URL, error) {
	base, err := url.Parse(s.cfg.SidecarURL)
	if err != nil {
		return nil, fmt.Errorf("invalid sidecar url: %w", err)
	}
	return base.ResolveReference(&url.URL{Path: path}), nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.nc == nil || s.nc.Status() != nats.CONNECTED {
		http.Error(w, "nats not connected", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleMessage(msg *nats.Msg) {
	if msg == nil {
		return
	}

	s.wg.Add(1)
	defer s.wg.Done()

	var envelope conversation.AssignmentEnvelope
	if err := json.Unmarshal(msg.Data, &envelope); err != nil {
		s.log.WithError(err).Warn("invalid assignment envelope")
		_ = msg.Term()
		return
	}
	if len(envelope.Payload) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Payload), []byte("null")) {
		s.log.Warn("assignment missing payload")
		_ = msg.Term()
		return
	}

	var payload inboundRequest
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		s.log.WithError(err).Warn("invalid inbound payload")
		_ = msg.Term()
		return
	}

	assignmentID := envelope.AssignmentID
	if assignmentID == "" {
		assignmentID = payload.RequestID
	}
	if assignmentID == "" {
		assignmentID = durableName(fmt.Sprintf("%s-%d", s.cfg.DllamaName, time.Now().UnixNano()))
	}

	s.log.WithFields(logrus.Fields{
		"hash":         payload.Hash,
		"chatId":       payload.ChatID,
		"model":        payload.Model,
		"assignmentId": assignmentID,
	}).Info("processing request")

	// Track active requests
	metrics.LLMRequestsActive.Inc()
	defer metrics.LLMRequestsActive.Dec()

	start := time.Now()
	s.publishState("busy", assignmentID, 1, "")
	ackMode := "ack"
	defer func() {
		duration := time.Since(start)
		metrics.LLMRequestDuration.Observe(duration.Seconds())

		if ackMode == "ack" {
			if err := msg.Ack(); err != nil {
				s.log.WithError(err).Warn("ack request message")
			}
			return
		}
		if err := msg.Nak(); err != nil {
			s.log.WithError(err).Warn("nak request message")
		}
	}()

	var err error
	if payload.Request.Stream {
		err = s.streamToSidecar(payload)
	} else {
		err = s.executeOnce(payload)
	}
	if err != nil {
		metrics.LLMRequestsTotal.WithLabelValues("error").Inc()
		s.publishState("error", assignmentID, 0, err.Error())
		s.log.WithError(err).Warn("request handling failed")
		ackMode = "nak"
		return
	}

	metrics.LLMRequestsTotal.WithLabelValues("success").Inc()
	s.publishState("idle", assignmentID, 0, "")
}

func (s *Server) streamToSidecar(payload inboundRequest) error {
	target := strings.TrimSpace(payload.ResponseSubject)
	if target == "" {
		target = s.outSubject
	}

	endpoint, err := s.sidecarEndpoint(sidecarChatCompletionsPath)
	if err != nil {
		s.log.WithError(err).Error("resolve sidecar endpoint")
		return err
	}

	body, err := json.Marshal(payload.Request)
	if err != nil {
		s.log.WithError(err).Error("marshal request")
		return err
	}

	req, err := http.NewRequest(http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		s.log.WithError(err).Error("build sidecar request")
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	req = req.WithContext(context.Background())

	res, err := s.client.Do(req)
	if err != nil {
		s.log.WithError(err).Error("sidecar request failed")
		s.publishError(target, fmt.Sprintf("sidecar request failed: %v", err))
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		respBody, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		err := fmt.Errorf("sidecar responded %d: %s", res.StatusCode, strings.TrimSpace(string(respBody)))
		s.publishError(target, err.Error())
		return err
	}

	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		normalized := normalizeStreamingChunk([]byte(line), payload.Request.Model)
		if err := s.nc.Publish(target, normalized); err != nil {
			s.log.WithError(err).Warn("publish chunk")
		}
	}
	if err := scanner.Err(); err != nil {
		s.log.WithError(err).Warn("stream read error")
		return err
	}
	if err := s.nc.Publish(target, []byte("[DONE]")); err != nil {
		s.log.WithError(err).Warn("publish done marker")
		return err
	}
	return nil
}

func (s *Server) executeOnce(payload inboundRequest) error {
	target := strings.TrimSpace(payload.ResponseSubject)
	if target == "" {
		target = s.outSubject
	}

	endpoint, err := s.sidecarEndpoint(sidecarChatCompletionsPath)
	if err != nil {
		s.log.WithError(err).Error("resolve sidecar endpoint")
		return err
	}

	body, err := json.Marshal(payload.Request)
	if err != nil {
		s.log.WithError(err).Error("marshal request")
		return err
	}

	reqCtx := context.Background()
	if s.cfg.SidecarTimeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(context.Background(), s.cfg.SidecarTimeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		s.log.WithError(err).Error("build sidecar request")
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := s.client.Do(req)
	if err != nil {
		s.publishError(target, fmt.Sprintf("sidecar request failed: %v", err))
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		respBody, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		err := fmt.Errorf("sidecar responded %d: %s", res.StatusCode, strings.TrimSpace(string(respBody)))
		s.publishError(target, err.Error())
		return err
	}

	limit := int64(maxNonStreamResponseSize)
	data, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		s.publishError(target, fmt.Sprintf("read sidecar response: %v", err))
		return err
	}
	if int64(len(data)) > limit {
		err := fmt.Errorf("sidecar response exceeded %d bytes", limit)
		s.publishError(target, err.Error())
		return err
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		err := errors.New("sidecar returned empty body")
		s.publishError(target, err.Error())
		return err
	}

	// Normalize response for OpenAI API compatibility
	normalized := normalizeChatCompletionResponse(trimmed, payload.Request.Model)

	if err := s.nc.Publish(target, normalized); err != nil {
		return fmt.Errorf("publish response: %w", err)
	}

	return nil
}

func (s *Server) publishState(state, assignmentID string, active int32, errMsg string) {
	if strings.TrimSpace(s.stateSubject) == "" {
		return
	}
	event := conversation.WorkerStateEvent{
		Dllama:       s.cfg.DllamaName,
		State:        state,
		AssignmentID: assignmentID,
		Active:       active,
		Timestamp:    time.Now().Unix(),
		Error:        errMsg,
	}
	data, err := json.Marshal(event)
	if err != nil {
		s.log.WithError(err).Warn("marshal state event")
		return
	}
	if err := s.nc.Publish(s.stateSubject, data); err != nil {
		s.log.WithError(err).WithField("subject", s.stateSubject).Warn("publish state event")
	}
}

func (s *Server) startConsumerCleanup(ctx context.Context) {
	if s.js == nil || strings.TrimSpace(s.streamName) == "" {
		return
	}

	cleanupInactiveConsumers(s.js, s.streamName, s.log)

	if consumerCleanupInterval <= 0 {
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(consumerCleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanupInactiveConsumers(s.js, s.streamName, s.log)
			}
		}
	}()
}

func (s *Server) startHeartbeatLoop(ctx context.Context) {
	// Emit an immediate idle heartbeat so the dispatcher can register this worker.
	s.publishState("idle", "", 0, "")

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		interval := s.cfg.HeartbeatInterval
		if interval <= 0 {
			interval = defaultHeartbeatInterval
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.publishState("idle", "", 0, "")
			}
		}
	}()
}

func (s *Server) ensureQueueSubscription(queueName string, ackWait time.Duration) (*nats.Subscription, error) {
	info, err := s.js.ConsumerInfo(s.streamName, queueName)
	switch {
	case err == nil:
		cfg := info.Config
		if strings.TrimSpace(cfg.FilterSubject) != s.inSubject {
			// Filter subject mismatch - delete old consumer and recreate
			s.log.WithFields(logrus.Fields{
				"consumer":        queueName,
				"stream":          s.streamName,
				"old_filter":      cfg.FilterSubject,
				"expected_filter": s.inSubject,
			}).Warn("consumer has mismatched filter subject, recreating")
			if deleteErr := s.js.DeleteConsumer(s.streamName, queueName); deleteErr != nil {
				return nil, fmt.Errorf("delete consumer %s with mismatched filter: %w", queueName, deleteErr)
			}
			// Fall through to create new consumer
			opts := []nats.SubOpt{
				nats.ManualAck(),
				nats.Durable(queueName),
				nats.BindStream(s.streamName),
				nats.AckWait(ackWait),
				nats.MaxAckPending(32),
			}
			if inactiveConsumerGrace > 0 {
				opts = append(opts, nats.InactiveThreshold(inactiveConsumerGrace))
			}
			return s.js.QueueSubscribe(s.inSubject, queueName, s.handleMessage, opts...)
		}

		updated := false
		if ackWait > 0 && cfg.AckWait != ackWait {
			cfg.AckWait = ackWait
			updated = true
		}
		if cfg.MaxAckPending != 32 {
			cfg.MaxAckPending = 32
			updated = true
		}
		if inactiveConsumerGrace > 0 && cfg.InactiveThreshold != inactiveConsumerGrace {
			cfg.InactiveThreshold = inactiveConsumerGrace
			updated = true
		}
		if updated {
			if _, updateErr := s.js.UpdateConsumer(s.streamName, &cfg); updateErr != nil {
				s.log.WithError(updateErr).WithFields(logrus.Fields{
					"consumer": queueName,
					"stream":   s.streamName,
				}).Warn("update consumer configuration")
			}
		}

		return s.js.QueueSubscribe(
			s.inSubject,
			queueName,
			s.handleMessage,
			nats.ManualAck(),
			nats.Bind(s.streamName, queueName),
		)
	case errors.Is(err, nats.ErrConsumerNotFound):
		opts := []nats.SubOpt{
			nats.ManualAck(),
			nats.Durable(queueName),
			nats.BindStream(s.streamName),
			nats.AckWait(ackWait),
			nats.MaxAckPending(32),
		}
		if inactiveConsumerGrace > 0 {
			opts = append(opts, nats.InactiveThreshold(inactiveConsumerGrace))
		}
		return s.js.QueueSubscribe(s.inSubject, queueName, s.handleMessage, opts...)
	case err != nil:
		return nil, fmt.Errorf("lookup consumer %s: %w", queueName, err)
	default:
		return nil, fmt.Errorf("unknown consumer lookup state for %s", queueName)
	}
}

func (s *Server) shutdownError() error {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	return s.shutdownErr
}

func (s *Server) setShutdownErr(err error) {
	if err == nil {
		return
	}
	s.shutdownMu.Lock()
	if s.shutdownErr == nil {
		s.shutdownErr = err
	}
	s.shutdownMu.Unlock()
}

func (s *Server) triggerEviction(reason string) {
	s.evictOnce.Do(func() {
		s.log.WithField("reason", reason).Warn("evicting dllama instance")
		s.publishState("error", "", 0, reason)
		if err := s.evictDllama(context.Background()); err != nil {
			// Log at debug when eviction is intentionally disabled, warn otherwise.
			if errors.Is(err, errEvictionDisabled) {
				s.log.WithError(err).Debug("dllama eviction disabled")
			} else {
				s.log.WithError(err).Warn("failed to evict dllama instance")
			}
		}
		s.setShutdownErr(errors.New(reason))
		if s.cancel != nil {
			s.cancel()
		}
	})
}

func (s *Server) evictDllama(ctx context.Context) error {
	if s.kube == nil || s.namespace == "" || strings.TrimSpace(s.cfg.DllamaName) == "" {
		return errEvictionDisabled
	}

	deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	propagation := metav1.DeletePropagationBackground
	options := metav1.DeleteOptions{
		GracePeriodSeconds: pointer.Int64(0),
		PropagationPolicy:  &propagation,
	}

	err := s.kube.Resource(dllamaGVR).Namespace(s.namespace).Delete(deleteCtx, s.cfg.DllamaName, options)
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (s *Server) monitorSidecar(ctx context.Context) {
	if s.sidecarMonitorInterval <= 0 || s.sidecarFailureThreshold <= 0 {
		return
	}

	ticker := time.NewTicker(s.sidecarMonitorInterval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.probeSidecar(ctx) {
				failures = 0
				continue
			}
			failures++
			if failures >= s.sidecarFailureThreshold {
				reason := fmt.Sprintf("dllama sidecar health check failed %d times in a row", failures)
				s.triggerEviction(reason)
				return
			}
		}
	}
}

func (s *Server) probeSidecar(ctx context.Context) bool {
	endpoint, err := s.sidecarEndpoint(sidecarModelsPath)
	if err != nil {
		return false
	}

	timeout := 5 * time.Second
	if s.cfg.SidecarTimeout > 0 && s.cfg.SidecarTimeout < timeout {
		timeout = s.cfg.SidecarTimeout
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return false
	}

	res, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))

	healthy := res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices

	// Update health metric
	dllamaName := s.cfg.DllamaName
	if dllamaName == "" {
		dllamaName = "unknown"
	}
	if healthy {
		metrics.LLMSidecarHealthStatus.WithLabelValues(dllamaName).Set(1)
	} else {
		metrics.LLMSidecarHealthStatus.WithLabelValues(dllamaName).Set(0)
	}

	return healthy
}

func (s *Server) publishError(target, msg string) {
	s.log.Warn(msg)
	if target == "" {
		target = s.outSubject
	}
	errPayload := map[string]any{
		"error": msg,
	}
	body, _ := json.Marshal(errPayload)
	_ = s.nc.Publish(target, body)
	_ = s.nc.Publish(target, []byte("[DONE]"))
}

// normalizeChatCompletionResponse fixes known dllama-api response issues
// to improve OpenAI API compatibility.
func normalizeChatCompletionResponse(data []byte, requestedModel string) []byte {
	var resp map[string]any
	if err := json.Unmarshal(data, &resp); err != nil {
		return data // return as-is if unparseable
	}

	// Fix model field to match requested model
	if requestedModel != "" {
		resp["model"] = requestedModel
	}

	// Fix choices array
	if choices, ok := resp["choices"].([]any); ok {
		for i, choice := range choices {
			if choiceMap, ok := choice.(map[string]any); ok {
				// Fix index to be valid non-negative integer
				if idx, ok := choiceMap["index"].(float64); ok && idx < 0 {
					choiceMap["index"] = i
				}
				// Fix empty finish_reason to "stop"
				if reason, ok := choiceMap["finish_reason"].(string); ok && reason == "" {
					choiceMap["finish_reason"] = "stop"
				}

				// Parse tool calls from markdown code blocks in content
				if message, ok := choiceMap["message"].(map[string]any); ok {
					if content, ok := message["content"].(string); ok {
						toolCalls, cleanContent := parseToolCallsFromContent(content)
						if len(toolCalls) > 0 {
							message["tool_calls"] = toolCalls
							message["content"] = cleanContent
							choiceMap["finish_reason"] = "tool_calls"
						}
					}
				}
			}
		}
	}

	normalized, err := json.Marshal(resp)
	if err != nil {
		return data // return original if re-encoding fails
	}
	return normalized
}

// parseToolCallsFromContent scans for markdown code blocks containing JSON with
// "tool_calls" and extracts them into OpenAI-compatible format.
func parseToolCallsFromContent(content string) ([]map[string]any, string) {
	// Look for ```json ... ``` code blocks
	const codeBlockStart = "```json"
	const codeBlockEnd = "```"

	var toolCalls []map[string]any
	var cleanContent strings.Builder

	for {
		start := strings.Index(content, codeBlockStart)
		if start == -1 {
			cleanContent.WriteString(content)
			break
		}

		// Write text before code block
		cleanContent.WriteString(content[:start])

		// Find end of code block
		end := strings.Index(content[start+len(codeBlockStart):], codeBlockEnd)
		if end == -1 {
			// Unclosed code block, keep as-is
			cleanContent.WriteString(content[start:])
			break
		}

		codeBlock := content[start+len(codeBlockStart) : start+len(codeBlockStart)+end]
		codeBlock = strings.TrimSpace(codeBlock)

		// Try to parse JSON with tool_calls
		var parsed map[string]any
		if err := json.Unmarshal([]byte(codeBlock), &parsed); err == nil {
			if calls, ok := parsed["tool_calls"].([]any); ok {
				for _, call := range calls {
					if callMap, ok := call.(map[string]any); ok {
						// Convert arguments from object to JSON string if needed
						if fn, ok := callMap["function"].(map[string]any); ok {
							if args, ok := fn["arguments"]; ok {
								switch a := args.(type) {
								case string:
									// Already a string, keep as-is
								case map[string]any:
									// Convert map to JSON string
									argBytes, _ := json.Marshal(a)
									fn["arguments"] = string(argBytes)
								}
							}
						}
						toolCalls = append(toolCalls, callMap)
					}
				}
			}
		}

		// Continue after code block
		content = content[start+len(codeBlockStart)+end+len(codeBlockEnd):]
	}

	// Trim any trailing whitespace
	result := strings.TrimSpace(cleanContent.String())
	if result == "" {
		result = ""
	}

	return toolCalls, result
}

// normalizeStreamingChunk fixes known dllama-api streaming response issues.
func normalizeStreamingChunk(data []byte, requestedModel string) []byte {
	var resp map[string]any
	if err := json.Unmarshal(data, &resp); err != nil {
		return data // return as-is if unparseable
	}

	// Fix model field to match requested model
	if requestedModel != "" {
		resp["model"] = requestedModel
	}

	normalized, err := json.Marshal(resp)
	if err != nil {
		return data // return original if re-encoding fails
	}
	return normalized
}

type consumerManager interface {
	Consumers(stream string, opts ...nats.JSOpt) <-chan *nats.ConsumerInfo
	DeleteConsumer(stream, consumer string, opts ...nats.JSOpt) error
}

func cleanupInactiveConsumers(js consumerManager, stream string, logger *logrus.Entry) {
	if js == nil {
		return
	}
	stream = strings.TrimSpace(stream)
	if stream == "" {
		return
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
		opts   []nats.JSOpt
	)
	if consumerListTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), consumerListTimeout)
		opts = append(opts, nats.Context(ctx))
		defer cancel()
	}

	now := time.Now()
	for info := range js.Consumers(stream, opts...) {
		if info == nil {
			continue
		}
		durable := strings.TrimSpace(info.Config.Durable)
		if durable == "" || !strings.HasPrefix(durable, "llm-") {
			continue
		}
		if info.PushBound {
			continue
		}
		if info.NumPending == 0 && info.NumAckPending == 0 {
			continue
		}
		last := consumerLastActivity(info)
		if now.Sub(last) < inactiveConsumerGrace {
			continue
		}
		if err := js.DeleteConsumer(stream, info.Name); err != nil {
			if logger != nil {
				logger.WithError(err).WithFields(logrus.Fields{
					"consumer": info.Name,
					"stream":   stream,
				}).Warn("jetstream cleanup: delete inactive consumer")
			}
			continue
		}
		if logger != nil {
			logger.WithFields(logrus.Fields{
				"consumer":    info.Name,
				"stream":      stream,
				"pending":     info.NumPending,
				"ack_pending": info.NumAckPending,
			}).Info("jetstream cleanup: pruned inactive consumer")
		}
	}
}

func consumerLastActivity(info *nats.ConsumerInfo) time.Time {
	if info == nil {
		return time.Time{}
	}
	if info.Delivered.Last != nil && !info.Delivered.Last.IsZero() {
		return *info.Delivered.Last
	}
	if info.AckFloor.Last != nil && !info.AckFloor.Last.IsZero() {
		return *info.AckFloor.Last
	}
	return info.Created
}

func ensureRequestStream(js nats.JetStreamContext, prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", fmt.Errorf("in-prefix is required for request stream")
	}
	if !strings.HasSuffix(prefix, ".") {
		return "", fmt.Errorf("in-prefix %q must end with '.' to enable durable delivery", prefix)
	}

	required := map[string]struct{}{
		(prefix + ">"):          {},
		(defaultInPrefix + ">"): {},
	}

	info, err := js.StreamInfo(llmRequestStreamName)
	switch {
	case err == nil:
		for _, subj := range info.Config.Subjects {
			required[strings.TrimSpace(subj)] = struct{}{}
		}

		cfg := info.Config
		cfg.Subjects = uniqueSubjects(required)
		if _, err := js.UpdateStream(&cfg); err != nil {
			return "", fmt.Errorf("update stream: %w", err)
		}
	case errors.Is(err, nats.ErrStreamNotFound):
		cfg := &nats.StreamConfig{
			Name:      llmRequestStreamName,
			Subjects:  uniqueSubjects(required),
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
		}
		if _, err := js.AddStream(cfg); err != nil {
			return "", fmt.Errorf("create stream: %w", err)
		}
	default:
		return "", fmt.Errorf("stream info: %w", err)
	}

	return llmRequestStreamName, nil
}

func uniqueSubjects(values map[string]struct{}) []string {
	list := make([]string, 0, len(values))
	for subj := range values {
		subj = strings.TrimSpace(subj)
		if subj == "" {
			continue
		}
		list = append(list, subj)
	}
	sort.Strings(list)
	return list
}

func inClusterNamespace() string {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func durableName(hash string) string {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return "llm-default"
	}

	var b strings.Builder
	b.Grow(len(hash) + 4)
	b.WriteString("llm-")
	for _, r := range hash {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return b.String()
}
