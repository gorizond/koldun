package controllers

import (
	"testing"
)

func TestNewHealth(t *testing.T) {
	h := NewHealth()
	if h == nil {
		t.Fatal("NewHealth() returned nil")
	}

	// New health should start as not ready
	if h.APIHealthy() {
		t.Error("New Health should not be API healthy initially")
	}
	if h.CachesSynced() {
		t.Error("New Health should not have caches synced initially")
	}
	if h.Ready() {
		t.Error("New Health should not be ready initially")
	}
}

func TestHealth_SetAPIHealthy(t *testing.T) {
	h := NewHealth()

	// Set to true
	h.SetAPIHealthy(true)
	if !h.APIHealthy() {
		t.Error("APIHealthy() should be true after SetAPIHealthy(true)")
	}

	// Set to false
	h.SetAPIHealthy(false)
	if h.APIHealthy() {
		t.Error("APIHealthy() should be false after SetAPIHealthy(false)")
	}

	// Set to true again
	h.SetAPIHealthy(true)
	if !h.APIHealthy() {
		t.Error("APIHealthy() should be true after second SetAPIHealthy(true)")
	}
}

func TestHealth_SetCachesSynced(t *testing.T) {
	h := NewHealth()

	// Set to true
	h.SetCachesSynced(true)
	if !h.CachesSynced() {
		t.Error("CachesSynced() should be true after SetCachesSynced(true)")
	}

	// Set to false
	h.SetCachesSynced(false)
	if h.CachesSynced() {
		t.Error("CachesSynced() should be false after SetCachesSynced(false)")
	}

	// Set to true again
	h.SetCachesSynced(true)
	if !h.CachesSynced() {
		t.Error("CachesSynced() should be true after second SetCachesSynced(true)")
	}
}

func TestHealth_Ready(t *testing.T) {
	tests := []struct {
		name         string
		apiHealthy   bool
		cachesSynced bool
		wantReady    bool
	}{
		{
			name:         "both false",
			apiHealthy:   false,
			cachesSynced: false,
			wantReady:    false,
		},
		{
			name:         "only API healthy",
			apiHealthy:   true,
			cachesSynced: false,
			wantReady:    false,
		},
		{
			name:         "only caches synced",
			apiHealthy:   false,
			cachesSynced: true,
			wantReady:    false,
		},
		{
			name:         "both true",
			apiHealthy:   true,
			cachesSynced: true,
			wantReady:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHealth()
			h.SetAPIHealthy(tt.apiHealthy)
			h.SetCachesSynced(tt.cachesSynced)

			if got := h.Ready(); got != tt.wantReady {
				t.Errorf("Ready() = %v, want %v", got, tt.wantReady)
			}
		})
	}
}

func TestHealth_ConcurrentAccess(t *testing.T) {
	// Test that concurrent reads/writes don't panic
	// This is a basic smoke test for thread safety
	h := NewHealth()

	done := make(chan bool)

	// Writer goroutines
	go func() {
		for i := 0; i < 100; i++ {
			h.SetAPIHealthy(i%2 == 0)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			h.SetCachesSynced(i%3 == 0)
		}
		done <- true
	}()

	// Reader goroutines
	go func() {
		for i := 0; i < 100; i++ {
			_ = h.APIHealthy()
			_ = h.CachesSynced()
			_ = h.Ready()
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 3; i++ {
		<-done
	}
}
