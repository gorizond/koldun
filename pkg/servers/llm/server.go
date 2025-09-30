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
	"strings"
	"sync"
	"time"

	"github.com/gorizond/koldun/pkg/api/openai"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	defaultListenAddress = ":8081"
	defaultSidecarURL    = "http://127.0.0.1:8080"
	defaultInPrefix      = "in."
	defaultOutPrefix     = "out."

	sidecarModelsPath          = "/v1/models"
	sidecarChatCompletionsPath = "/v1/chat/completions"

	llmRequestStreamName = "KOLDUN_LLM_REQUESTS"
)

// Config holds runtime parameters for the LLM worker.
type Config struct {
	Hash string

	ListenAddress string
	HealthOnly    bool

	NATSURL   string
	InPrefix  string
	OutPrefix string

	SidecarURL     string
	SidecarTimeout time.Duration

	Logger *logrus.Entry
}

// Server subscribes to conversation requests and proxies them to a local dllama-api sidecar.
type Server struct {
	cfg Config
	log *logrus.Entry

	nc *nats.Conn
	js nats.JetStreamContext

	httpServer *http.Server
	client     *http.Client

	sub        *nats.Subscription
	streamName string

	inSubject  string
	outSubject string

	wg sync.WaitGroup
}

type inboundRequest struct {
	Hash      string                       `json:"hash"`
	ChatID    string                       `json:"chatId"`
	ChatStart string                       `json:"chatStart"`
	TokenHash string                       `json:"tokenHash"`
	Model     string                       `json:"model"`
	Namespace string                       `json:"namespace"`
	Request   openai.ChatCompletionRequest `json:"request"`
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
	if cfg.SidecarURL == "" {
		cfg.SidecarURL = defaultSidecarURL
	}
	if cfg.SidecarTimeout == 0 {
		cfg.SidecarTimeout = 2 * time.Minute
	}

	log := cfg.Logger
	if log == nil {
		log = logrus.StandardLogger().WithField("component", "koldun-llm")
	}

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("koldun-llm"))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}

	streamName, err := ensureRequestStream(js, cfg.InPrefix)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("request stream: %w", err)
	}

	client := &http.Client{Timeout: cfg.SidecarTimeout}

	srv := &Server{
		cfg:        cfg,
		log:        log,
		nc:         nc,
		js:         js,
		client:     client,
		inSubject:  cfg.InPrefix + cfg.Hash,
		outSubject: cfg.OutPrefix + cfg.Hash,
		streamName: streamName,
	}
	return srv, nil
}

// Run starts the subscription loop and health endpoint until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	if err := s.waitForSidecar(ctx); err != nil {
		return fmt.Errorf("wait for dllama sidecar: %w", err)
	}

	queueName := durableName(s.cfg.Hash)
	ackWait := s.cfg.SidecarTimeout
	if ackWait <= 0 {
		ackWait = 2 * time.Minute
	} else {
		ackWait += 30 * time.Second
	}

	sub, err := s.js.QueueSubscribe(
		s.inSubject,
		queueName,
		s.handleMessage,
		nats.ManualAck(),
		nats.Durable(queueName),
		nats.BindStream(s.streamName),
		nats.AckWait(ackWait),
		nats.MaxAckPending(32),
	)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", s.inSubject, err)
	}
	s.sub = sub
	s.log.WithFields(logrus.Fields{
		"subject": s.inSubject,
		"queue":   queueName,
		"stream":  s.streamName,
	}).Info("subscribed to request stream")

	errCh := make(chan error, 1)

	if !s.cfg.HealthOnly {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", s.handleHealth)
		mux.HandleFunc("/readyz", s.handleHealth)

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
		<-ctx.Done()
		s.log.Info("llm server shutting down")
		if s.sub != nil {
			if err := s.sub.Drain(); err != nil {
				s.log.WithError(err).Warn("drain subscription")
			}
		}
		s.wg.Wait()
		s.nc.Drain()
		if s.httpServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = s.httpServer.Shutdown(shutdownCtx)
		}
	}()

	select {
	case <-ctx.Done():
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
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
			res.Body.Close()
			if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices {
				s.log.WithField("endpoint", endpoint.String()).Info("dllama-api sidecar is ready")
				return nil
			}
			err = fmt.Errorf("unexpected status %d", res.StatusCode)
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

	defer func() {
		if err := msg.Ack(); err != nil {
			s.log.WithError(err).Warn("ack request message")
		}
	}()

	var payload inboundRequest
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		s.log.WithError(err).Warn("invalid inbound payload")
		return
	}

	s.log.WithFields(logrus.Fields{
		"hash":   payload.Hash,
		"chatId": payload.ChatID,
		"model":  payload.Model,
	}).Info("processing request")

	var err error
	if payload.Request.Stream {
		err = s.streamToSidecar(payload)
	} else {
		err = s.executeOnce(payload)
	}
	if err != nil {
		s.log.WithError(err).Warn("request handling failed")
	}
}

func (s *Server) streamToSidecar(payload inboundRequest) error {
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
		s.publishError(fmt.Sprintf("sidecar request failed: %v", err))
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		respBody, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		err := fmt.Errorf("sidecar responded %d: %s", res.StatusCode, strings.TrimSpace(string(respBody)))
		s.publishError(err.Error())
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
		if err := s.nc.Publish(s.outSubject, []byte(line)); err != nil {
			s.log.WithError(err).Warn("publish chunk")
		}
	}
	if err := scanner.Err(); err != nil {
		s.log.WithError(err).Warn("stream read error")
		return err
	}
	if err := s.nc.Publish(s.outSubject, []byte("[DONE]")); err != nil {
		s.log.WithError(err).Warn("publish done marker")
		return err
	}
	return nil
}

func (s *Server) executeOnce(payload inboundRequest) error {
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

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		s.log.WithError(err).Error("build sidecar request")
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := s.client.Do(req)
	if err != nil {
		s.publishError(fmt.Sprintf("sidecar request failed: %v", err))
		return err
	}
	defer res.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(res.Body, 10<<20))
	if err != nil {
		s.publishError(fmt.Sprintf("read sidecar response failed: %v", err))
		return err
	}

	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		err := fmt.Errorf("sidecar responded %d: %s", res.StatusCode, strings.TrimSpace(string(respBody)))
		s.publishError(err.Error())
		return err
	}

	if err := s.nc.Publish(s.outSubject, respBody); err != nil {
		s.log.WithError(err).Warn("publish response")
		return err
	}
	return nil
}

func (s *Server) publishError(msg string) {
	s.log.Warn(msg)
	errPayload := map[string]any{
		"error": msg,
	}
	body, _ := json.Marshal(errPayload)
	_ = s.nc.Publish(s.outSubject, body)
	_ = s.nc.Publish(s.outSubject, []byte("[DONE]"))
}

func ensureRequestStream(js nats.JetStreamContext, prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", fmt.Errorf("in-prefix is required for request stream")
	}
	if !strings.HasSuffix(prefix, ".") {
		return "", fmt.Errorf("in-prefix %q must end with '.' to enable durable delivery", prefix)
	}

	subject := prefix + ">"
	cfg := &nats.StreamConfig{
		Name:      llmRequestStreamName,
		Subjects:  []string{subject},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.FileStorage,
	}

	if _, err := js.StreamInfo(llmRequestStreamName); err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			if _, err := js.AddStream(cfg); err != nil {
				return "", fmt.Errorf("create stream: %w", err)
			}
		} else {
			return "", fmt.Errorf("stream info: %w", err)
		}
	} else {
		if _, err := js.UpdateStream(cfg); err != nil {
			return "", fmt.Errorf("update stream: %w", err)
		}
	}

	return llmRequestStreamName, nil
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
