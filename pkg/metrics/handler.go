package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler returns an HTTP handler for Prometheus metrics endpoint
func Handler() http.Handler {
	return promhttp.Handler()
}

// NewServeMux creates a new HTTP mux with metrics endpoint
func NewServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	return mux
}
