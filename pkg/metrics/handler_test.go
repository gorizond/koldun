package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler(t *testing.T) {
	handler := Handler()
	if handler == nil {
		t.Fatal("Handler() returned nil")
	}

	// Test that handler responds to requests
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v", w.Code, http.StatusOK)
	}

	// Prometheus metrics should be in plain text
	contentType := w.Header().Get("Content-Type")
	if contentType == "" {
		t.Error("Handler should set Content-Type header")
	}
}

func TestNewServeMux(t *testing.T) {
	mux := NewServeMux()
	if mux == nil {
		t.Fatal("NewServeMux() returned nil")
	}

	// Test that /metrics endpoint is registered
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("NewServeMux /metrics returned wrong status code: got %v want %v", w.Code, http.StatusOK)
	}
}

func TestNewServeMux_NotFound(t *testing.T) {
	mux := NewServeMux()

	// Test that other paths return 404
	req := httptest.NewRequest(http.MethodGet, "/notfound", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("NewServeMux /notfound should return 404: got %v want %v", w.Code, http.StatusNotFound)
	}
}

func TestHandler_Methods(t *testing.T) {
	handler := Handler()

	methods := []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/metrics", nil)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			// Prometheus handler typically accepts GET and HEAD
			// Other methods might still return 200 or appropriate error
			if w.Code < 200 || w.Code >= 500 {
				t.Errorf("Handler with method %s returned unexpected status: %v", method, w.Code)
			}
		})
	}
}
