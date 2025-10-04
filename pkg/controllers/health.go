package controllers

import "sync/atomic"

// Health tracks the operator's connectivity and readiness state.
type Health struct {
	apiHealthy   atomic.Bool
	cachesSynced atomic.Bool
}

// NewHealth initialises a fresh health tracker.
func NewHealth() *Health {
	return &Health{}
}

// SetAPIHealthy records whether the operator can reach the Kubernetes API server.
func (h *Health) SetAPIHealthy(ok bool) {
	h.apiHealthy.Store(ok)
}

// SetCachesSynced records whether all informer caches have successfully synchronised.
func (h *Health) SetCachesSynced(ok bool) {
	h.cachesSynced.Store(ok)
}

// APIHealthy reports the last known API connectivity state.
func (h *Health) APIHealthy() bool {
	return h.apiHealthy.Load()
}

// CachesSynced reports whether informer caches finished their initial sync.
func (h *Health) CachesSynced() bool {
	return h.cachesSynced.Load()
}

// Ready is true when the operator is healthy and caches are in sync.
func (h *Health) Ready() bool {
	return h.APIHealthy() && h.CachesSynced()
}
