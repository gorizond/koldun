package llm

import (
	"bufio"
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
	defaultInPrefix      = "in_"
	defaultOutPrefix     = "out_"
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

	httpServer *http.Server
	client     *http.Client

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

	client := &http.Client{Timeout: cfg.SidecarTimeout}

	srv := &Server{
		cfg:        cfg,
		log:        log,
		nc:         nc,
		client:     client,
		inSubject:  cfg.InPrefix + cfg.Hash,
		outSubject: cfg.OutPrefix + cfg.Hash,
	}
	return srv, nil
}

// Run starts the subscription loop and health endpoint until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	sub, err := s.nc.Subscribe(s.inSubject, s.handleMessage)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", s.inSubject, err)
	}
	s.log.Infof("subscribed to %s", s.inSubject)

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
		_ = sub.Drain()
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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.nc == nil || s.nc.Status() != nats.CONNECTED {
		http.Error(w, "nats not connected", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleMessage(msg *nats.Msg) {
	s.wg.Add(1)
	defer s.wg.Done()

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

	if payload.Request.Stream {
		s.streamToSidecar(payload)
		return
	}
	s.executeOnce(payload)
}

func (s *Server) streamToSidecar(payload inboundRequest) {
	endpoint, err := url.Parse(s.cfg.SidecarURL)
	if err != nil {
		s.log.WithError(err).Error("invalid sidecar url")
		return
	}
	endpoint = endpoint.ResolveReference(&url.URL{Path: "/v1/chat/completions"})

	body, err := json.Marshal(payload.Request)
	if err != nil {
		s.log.WithError(err).Error("marshal request")
		return
	}

	req, err := http.NewRequest(http.MethodPost, endpoint.String(), strings.NewReader(string(body)))
	if err != nil {
		s.log.WithError(err).Error("build sidecar request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	req = req.WithContext(context.Background())

	res, err := s.client.Do(req)
	if err != nil {
		s.log.WithError(err).Error("sidecar request failed")
		s.publishError(fmt.Sprintf("sidecar request failed: %v", err))
		return
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		s.publishError(fmt.Sprintf("sidecar responded %d: %s", res.StatusCode, strings.TrimSpace(string(respBody))))
		return
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
	}
	if err := s.nc.Publish(s.outSubject, []byte("[DONE]")); err != nil {
		s.log.WithError(err).Warn("publish done marker")
	}
}

func (s *Server) executeOnce(payload inboundRequest) {
	endpoint, err := url.Parse(s.cfg.SidecarURL)
	if err != nil {
		s.log.WithError(err).Error("invalid sidecar url")
		return
	}
	endpoint = endpoint.ResolveReference(&url.URL{Path: "/v1/chat/completions"})

	body, err := json.Marshal(payload.Request)
	if err != nil {
		s.log.WithError(err).Error("marshal request")
		return
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint.String(), strings.NewReader(string(body)))
	if err != nil {
		s.log.WithError(err).Error("build sidecar request")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := s.client.Do(req)
	if err != nil {
		s.publishError(fmt.Sprintf("sidecar request failed: %v", err))
		return
	}
	defer res.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(res.Body, 10<<20))
	if err != nil {
		s.publishError(fmt.Sprintf("read sidecar response failed: %v", err))
		return
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		s.publishError(fmt.Sprintf("sidecar responded %d: %s", res.StatusCode, strings.TrimSpace(string(respBody))))
		return
	}

	if err := s.nc.Publish(s.outSubject, respBody); err != nil {
		s.log.WithError(err).Warn("publish response")
	}
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
