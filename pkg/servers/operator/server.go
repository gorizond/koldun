package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorizond/koldun/pkg/controllers"
	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/sirupsen/logrus"
)

const (
	defaultListenAddress = ":8080"
	shutdownTimeout      = 5 * time.Second
)

// Config wires the operator health server.
type Config struct {
	ListenAddress string
	Health        *controllers.Health
	Logger        *logrus.Entry
}

// Server exposes healthz/readyz endpoints for the operator process.
type Server struct {
	cfg        Config
	log        *logrus.Entry
	httpServer *http.Server
	// shutdownHook allows tests to override Shutdown behavior to exercise error branches.
	shutdownHook func(context.Context) error
}

// New creates a new health server instance.
func New(cfg Config) (*Server, error) {
	if cfg.Health == nil {
		return nil, errors.New("health tracker is required")
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = defaultListenAddress
	}
	log := cfg.Logger
	if log == nil {
		log = logrus.StandardLogger().WithField("component", "koldun-operator-health")
	}

	return &Server{cfg: cfg, log: log}, nil
}

// Run starts the HTTP server and blocks until the context is cancelled or a fatal error occurs.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.Handle("/metrics", metrics.Handler())

	s.httpServer = &http.Server{ //nolint:gosec // listen address is controlled via config/flags
		Addr:    s.cfg.ListenAddress,
		Handler: mux,
	}

	errCh := make(chan error, 1)

	go func() {
		s.log.Infof("health server listening on %s", s.cfg.ListenAddress)
		errCh <- s.httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		shutdown := s.httpServer.Shutdown
		if s.shutdownHook != nil {
			shutdown = s.shutdownHook
		}
		if err := shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.WithError(err).Warn("health server shutdown error")
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.Health.APIHealthy() {
		s.respondStatus(w, http.StatusServiceUnavailable, "unhealthy: kubernetes api not reachable")
		return
	}

	s.respondStatus(w, http.StatusOK, "ok")
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.Health.APIHealthy() {
		s.respondStatus(w, http.StatusServiceUnavailable, "not ready: kubernetes api not reachable")
		return
	}
	if !s.cfg.Health.CachesSynced() {
		s.respondStatus(w, http.StatusServiceUnavailable, "not ready: caches not synced")
		return
	}

	s.respondStatus(w, http.StatusOK, "ready")
}

func (s *Server) respondStatus(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, fmt.Sprintf("%s\n", message))
}
