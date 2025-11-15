package testutil

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

var (
	loopbackOnce sync.Once
	loopbackErr  error
)

func ensureLoopback() error {
	loopbackOnce.Do(func() {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			loopbackErr = err
			return
		}
		_ = l.Close()
	})
	return loopbackErr
}

// RequireLoopback skips the test when loopback sockets are unavailable (common in sandboxes).
func RequireLoopback(t *testing.T) {
	t.Helper()
	if err := ensureLoopback(); err != nil {
		t.Skipf("loopback sockets unavailable: %v", err)
	}
}

// NewHTTPServer starts an httptest.Server pinned to IPv4 loopback so it works in restricted envs.
func NewHTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback sockets unavailable: %v", err)
	}

	server := httptest.NewUnstartedServer(handler)
	server.Listener = ln
	server.Start()
	return server
}
