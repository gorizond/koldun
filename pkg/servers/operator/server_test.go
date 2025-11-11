package operator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/controllers"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
)

func TestNew(t *testing.T) {
	health := controllers.NewHealth()

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			cfg: Config{
				ListenAddress: ":8080",
				Health:        health,
				Logger:        logrus.NewEntry(logrus.New()),
			},
			wantErr: false,
		},
		{
			name: "missing health tracker",
			cfg: Config{
				ListenAddress: ":8080",
				Health:        nil,
				Logger:        logrus.NewEntry(logrus.New()),
			},
			wantErr: true,
			errMsg:  "health tracker is required",
		},
		{
			name: "default listen address",
			cfg: Config{
				ListenAddress: "",
				Health:        health,
				Logger:        nil,
			},
			wantErr: false,
		},
		{
			name: "nil logger uses default",
			cfg: Config{
				ListenAddress: ":9090",
				Health:        health,
				Logger:        nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("New() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
			if !tt.wantErr {
				if server == nil {
					t.Error("New() returned nil server without error")
				}
				if server.cfg.ListenAddress == "" {
					t.Error("ListenAddress should not be empty after initialization")
				}
				if server.log == nil {
					t.Error("Logger should not be nil after initialization")
				}
			}
		})
	}
}

func TestServer_handleHealth(t *testing.T) {
	tests := []struct {
		name       string
		apiHealthy bool
		wantStatus int
		wantBody   string
	}{
		{
			name:       "healthy",
			apiHealthy: true,
			wantStatus: http.StatusOK,
			wantBody:   "ok",
		},
		{
			name:       "unhealthy - api not reachable",
			apiHealthy: false,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "unhealthy: kubernetes api not reachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := controllers.NewHealth()
			health.SetAPIHealthy(tt.apiHealthy)

			server := &Server{
				cfg: Config{
					Health: health,
				},
				log: logrus.NewEntry(logrus.New()),
			}

			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			rec := httptest.NewRecorder()

			server.handleHealth(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("handleHealth() status = %v, want %v", rec.Code, tt.wantStatus)
			}

			body := strings.TrimSpace(rec.Body.String())
			if body != tt.wantBody {
				t.Errorf("handleHealth() body = %q, want %q", body, tt.wantBody)
			}

			contentType := rec.Header().Get("Content-Type")
			if !strings.HasPrefix(contentType, "text/plain") {
				t.Errorf("handleHealth() Content-Type = %q, want text/plain", contentType)
			}
		})
	}
}

func TestServer_handleReady(t *testing.T) {
	tests := []struct {
		name         string
		apiHealthy   bool
		cachesSynced bool
		wantStatus   int
		wantBody     string
	}{
		{
			name:         "ready",
			apiHealthy:   true,
			cachesSynced: true,
			wantStatus:   http.StatusOK,
			wantBody:     "ready",
		},
		{
			name:         "not ready - api not reachable",
			apiHealthy:   false,
			cachesSynced: true,
			wantStatus:   http.StatusServiceUnavailable,
			wantBody:     "not ready: kubernetes api not reachable",
		},
		{
			name:         "not ready - caches not synced",
			apiHealthy:   true,
			cachesSynced: false,
			wantStatus:   http.StatusServiceUnavailable,
			wantBody:     "not ready: caches not synced",
		},
		{
			name:         "not ready - both false",
			apiHealthy:   false,
			cachesSynced: false,
			wantStatus:   http.StatusServiceUnavailable,
			wantBody:     "not ready: kubernetes api not reachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := controllers.NewHealth()
			health.SetAPIHealthy(tt.apiHealthy)
			health.SetCachesSynced(tt.cachesSynced)

			server := &Server{
				cfg: Config{
					Health: health,
				},
				log: logrus.NewEntry(logrus.New()),
			}

			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rec := httptest.NewRecorder()

			server.handleReady(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("handleReady() status = %v, want %v", rec.Code, tt.wantStatus)
			}

			body := strings.TrimSpace(rec.Body.String())
			if body != tt.wantBody {
				t.Errorf("handleReady() body = %q, want %q", body, tt.wantBody)
			}

			contentType := rec.Header().Get("Content-Type")
			if !strings.HasPrefix(contentType, "text/plain") {
				t.Errorf("handleReady() Content-Type = %q, want text/plain", contentType)
			}
		})
	}
}

func TestServer_respondStatus(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		message    string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "200 OK",
			status:     http.StatusOK,
			message:    "healthy",
			wantStatus: http.StatusOK,
			wantBody:   "healthy\n",
		},
		{
			name:       "503 Service Unavailable",
			status:     http.StatusServiceUnavailable,
			message:    "not ready",
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "not ready\n",
		},
		{
			name:       "empty message",
			status:     http.StatusOK,
			message:    "",
			wantStatus: http.StatusOK,
			wantBody:   "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{
				log: logrus.NewEntry(logrus.New()),
			}

			rec := httptest.NewRecorder()
			server.respondStatus(rec, tt.status, tt.message)

			if rec.Code != tt.wantStatus {
				t.Errorf("respondStatus() status = %v, want %v", rec.Code, tt.wantStatus)
			}

			body := rec.Body.String()
			if body != tt.wantBody {
				t.Errorf("respondStatus() body = %q, want %q", body, tt.wantBody)
			}

			contentType := rec.Header().Get("Content-Type")
			if !strings.HasPrefix(contentType, "text/plain") {
				t.Errorf("respondStatus() Content-Type = %q, want text/plain", contentType)
			}
		})
	}
}

func TestServer_Run_ContextCancellation(t *testing.T) {
	health := controllers.NewHealth()
	server, err := New(Config{
		ListenAddress: ":0", // Use port 0 for automatic assignment
		Health:        health,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- server.Run(ctx)
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for Run to return
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error after context cancellation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("Run() did not return after context cancellation")
	}
}

func TestServer_Run_InvalidAddress(t *testing.T) {
	health := controllers.NewHealth()
	server, err := New(Config{
		ListenAddress: "invalid:address:format",
		Health:        health,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	ctx := context.Background()
	err = server.Run(ctx)
	if err == nil {
		t.Error("Run() should have returned an error for invalid address")
	}
}

func TestDefaultListenAddress(t *testing.T) {
	health := controllers.NewHealth()
	server, err := New(Config{
		Health: health,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server.cfg.ListenAddress != defaultListenAddress {
		t.Errorf("Default listen address = %q, want %q", server.cfg.ListenAddress, defaultListenAddress)
	}
}

func TestServer_HTTPMethods(t *testing.T) {
	health := controllers.NewHealth()
	health.SetAPIHealthy(true)
	health.SetCachesSynced(true)

	server := &Server{
		cfg: Config{
			Health: health,
		},
		log: logrus.NewEntry(logrus.New()),
	}

	endpoints := []string{"/healthz", "/readyz"}
	methods := []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodHead,
		http.MethodOptions,
	}

	for _, endpoint := range endpoints {
		for _, method := range methods {
			t.Run(endpoint+"_"+method, func(t *testing.T) {
				req := httptest.NewRequest(method, endpoint, nil)
				rec := httptest.NewRecorder()

				switch endpoint {
				case "/healthz":
					server.handleHealth(rec, req)
				case "/readyz":
					server.handleReady(rec, req)
				}

				// All methods should work and return status
				if rec.Code == 0 {
					t.Errorf("%s %s did not set status code", method, endpoint)
				}
			})
		}
	}
}

func TestServer_ConcurrentRequests(t *testing.T) {
	health := controllers.NewHealth()
	health.SetAPIHealthy(true)
	health.SetCachesSynced(true)

	server := &Server{
		cfg: Config{
			Health: health,
		},
		log: logrus.NewEntry(logrus.New()),
	}

	// Test concurrent requests to both endpoints
	const numRequests = 100
	done := make(chan bool, numRequests*2)

	for i := 0; i < numRequests; i++ {
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			rec := httptest.NewRecorder()
			server.handleHealth(rec, req)
			done <- rec.Code == http.StatusOK
		}()

		go func() {
			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rec := httptest.NewRecorder()
			server.handleReady(rec, req)
			done <- rec.Code == http.StatusOK
		}()
	}

	// Wait for all requests to complete
	for i := 0; i < numRequests*2; i++ {
		if !<-done {
			t.Error("Concurrent request did not return OK status")
		}
	}
}

func TestServer_Run_ServerClosedError(t *testing.T) {
	// Test that ErrServerClosed is handled properly
	health := controllers.NewHealth()
	_ = &Server{
		cfg: Config{
			Health: health,
		},
		log:        logrus.NewEntry(logrus.New()),
		httpServer: &http.Server{},
	}

	// Create a context that's already cancelled
	_, cancel := context.WithCancel(context.Background())
	cancel()

	// Mock the error channel to return ErrServerClosed
	errCh := make(chan error, 1)
	errCh <- http.ErrServerClosed

	// This should not return an error
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Expected ErrServerClosed, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for error")
	}
}

func TestServer_Run_ReturnsNilWhenServerClosed(t *testing.T) {
	health := controllers.NewHealth()
	server, err := New(Config{
		ListenAddress: ":0",
		Health:        health,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(ctx)
	}()

	waitForHTTPServer(t, server)

	if err := server.httpServer.Close(); err != nil {
		t.Fatalf("failed to close http server: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after server.Close()")
	}
}

func TestServer_Run_LogsWarningWhenShutdownFails(t *testing.T) {
	health := controllers.NewHealth()
	logger, hook := logrustest.NewNullLogger()
	server, err := New(Config{
		ListenAddress: ":0",
		Health:        health,
		Logger:        logrus.NewEntry(logger),
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	server.shutdownHook = func(ctx context.Context) error {
		if server.httpServer != nil {
			_ = server.httpServer.Shutdown(ctx)
		}
		return errors.New("forced shutdown failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(ctx)
	}()

	waitForHTTPServer(t, server)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}

	entry := hook.LastEntry()
	if entry == nil {
		t.Fatal("expected shutdown warning log entry, got nil")
	}
	if entry.Level != logrus.WarnLevel {
		t.Fatalf("expected warn level log, got %s", entry.Level)
	}
	if entry.Message != "health server shutdown error" {
		t.Fatalf("unexpected log message: %s", entry.Message)
	}
	errField, ok := entry.Data["error"].(error)
	if !ok {
		t.Fatalf("unexpected type for error field: %T", entry.Data["error"])
	}
	if errField.Error() != "forced shutdown failure" {
		t.Fatalf("unexpected error field: %v", entry.Data["error"])
	}
}

func waitForHTTPServer(t *testing.T, server *Server) {
	t.Helper()

	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("server.httpServer was not initialized in time")
		case <-ticker.C:
			if server.httpServer != nil {
				return
			}
		}
	}
}
