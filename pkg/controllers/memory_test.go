package controllers

import "testing"

func TestCalculateMemoryRequests(t *testing.T) {
	const tenGi = 10 * 1024 * 1024 * 1024

	tests := []struct {
		name         string
		bytes        int64
		workers      int32
		override     *float64
		ok           bool
		wantRootMi   string
		wantWorkerMi string
	}{
		{name: "three-workers", bytes: tenGi, workers: 3, ok: true, wantRootMi: "3584Mi", wantWorkerMi: "2816Mi"},
		{name: "single-worker", bytes: tenGi, workers: 1, ok: true, wantRootMi: "7680Mi", wantWorkerMi: "5632Mi"},
		{name: "override", bytes: tenGi, workers: 3, override: func() *float64 { v := 2.0; return &v }(), ok: true, wantRootMi: "5Gi", wantWorkerMi: "2816Mi"},
		{name: "seven-workers", bytes: tenGi, workers: 7, ok: true, wantRootMi: "1536Mi", wantWorkerMi: "1408Mi"},
		{name: "zero-size", bytes: 0, workers: 1, ok: false},
		{name: "tiny-size", bytes: 512 * 1024, workers: 0, ok: true, wantRootMi: "1Mi", wantWorkerMi: "1Mi"},
	}

	for _, tt := range tests {
		root, worker, ok := calculateMemoryRequests(tt.bytes, tt.workers, tt.override)
		if ok != tt.ok {
			if ok {
				t.Fatalf("%s: expected ok=false", tt.name)
			}
			if !ok {
				t.Fatalf("%s: expected ok=true", tt.name)
			}
		}
		if !ok {
			continue
		}
		if got := root.String(); got != tt.wantRootMi {
			t.Errorf("%s: root.String() = %q, want %q", tt.name, got, tt.wantRootMi)
		}
		if got := worker.String(); got != tt.wantWorkerMi {
			t.Errorf("%s: worker.String() = %q, want %q", tt.name, got, tt.wantWorkerMi)
		}
	}
}
