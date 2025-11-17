package controllers

import (
	"fmt"
	"math"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestRootMemoryOverheadRatio(t *testing.T) {
	tests := []struct {
		name           string
		workerReplicas int32
		override       *float64
		wantMin        float64
		wantMax        float64
	}{
		{
			name:           "single worker uses max ratio",
			workerReplicas: 1,
			override:       nil,
			wantMin:        rootOverheadMaxRatio,
			wantMax:        rootOverheadMaxRatio,
		},
		{
			name:           "zero workers uses max ratio",
			workerReplicas: 0,
			override:       nil,
			wantMin:        rootOverheadMaxRatio,
			wantMax:        rootOverheadMaxRatio,
		},
		{
			name:           "negative workers fall back to max ratio",
			workerReplicas: -4,
			override:       nil,
			wantMin:        rootOverheadMaxRatio,
			wantMax:        rootOverheadMaxRatio,
		},
		{
			name:           "two workers starts decay",
			workerReplicas: 2,
			override:       nil,
			wantMin:        rootOverheadMinRatio,
			wantMax:        rootOverheadMaxRatio,
		},
		{
			name:           "seven workers approaches min ratio",
			workerReplicas: 7,
			override:       nil,
			wantMin:        rootOverheadMinRatio,
			wantMax:        rootOverheadMinRatio + 0.01,
		},
		{
			name:           "many workers uses min ratio",
			workerReplicas: 100,
			override:       nil,
			wantMin:        rootOverheadMinRatio,
			wantMax:        rootOverheadMinRatio,
		},
		{
			name:           "max int workers stays at min ratio",
			workerReplicas: math.MaxInt32,
			override:       nil,
			wantMin:        rootOverheadMinRatio,
			wantMax:        rootOverheadMinRatio,
		},
		{
			name:           "override below min gets clamped",
			workerReplicas: 5,
			override:       floatPtr(1.0),
			wantMin:        rootOverheadMinRatio,
			wantMax:        rootOverheadMinRatio,
		},
		{
			name:           "override above min is used",
			workerReplicas: 5,
			override:       floatPtr(3.5),
			wantMin:        3.5,
			wantMax:        3.5,
		},
		{
			name:           "override high value is used",
			workerReplicas: 1,
			override:       floatPtr(4.0),
			wantMin:        4.0,
			wantMax:        4.0,
		},
		{
			name:           "override zero is ignored",
			workerReplicas: 1,
			override:       floatPtr(0.0),
			wantMin:        rootOverheadMaxRatio,
			wantMax:        rootOverheadMaxRatio,
		},
		{
			name:           "override negative is ignored",
			workerReplicas: 1,
			override:       floatPtr(-1.0),
			wantMin:        rootOverheadMaxRatio,
			wantMax:        rootOverheadMaxRatio,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio := rootMemoryOverheadRatio(tt.workerReplicas, tt.override)
			if ratio < tt.wantMin || ratio > tt.wantMax {
				t.Errorf("rootMemoryOverheadRatio(%d, %v) = %v, want between %v and %v",
					tt.workerReplicas, formatFloatPtr(tt.override), ratio, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestRootMemoryOverheadRatioDecayCurve(t *testing.T) {

	ratioTwo := rootMemoryOverheadRatio(2, nil)
	ratioFive := rootMemoryOverheadRatio(5, nil)
	ratioTen := rootMemoryOverheadRatio(10, nil)

	if ratioTwo < ratioFive {
		t.Fatalf("expected ratioTwo >= ratioFive, got %f < %f", ratioTwo, ratioFive)
	}
	if ratioFive < ratioTen {
		t.Fatalf("expected ratioFive >= ratioTen, got %f < %f", ratioFive, ratioTen)
	}
	if ratioTen < rootOverheadMinRatio {
		t.Fatalf("ratioTen %f should not fall below min ratio %f", ratioTen, rootOverheadMinRatio)
	}
}

func TestCalculateMemoryRequests(t *testing.T) {
	tests := []struct {
		name                string
		conversionSizeBytes int64
		workerReplicas      int32
		override            *float64
		wantRootMiMin       int64
		wantRootMiMax       int64
		wantWorkerMiMin     int64
		wantWorkerMiMax     int64
		wantOk              bool
	}{
		{
			name:                "typical model 7B size with 3 workers",
			conversionSizeBytes: 14 * 1024 * 1024 * 1024, // 14 GiB
			workerReplicas:      3,
			override:            nil,
			// 14GiB / 4 nodes = 3.5GiB per node
			// Root: 3.5GiB * 2.9167 (decay for 3 workers) ≈ 10.2GiB
			// Worker: 3.5GiB * 2.50 = 8.75GiB
			wantRootMiMin:       10000,
			wantRootMiMax:       10500,
			wantWorkerMiMin:     8700,
			wantWorkerMiMax:     9000,
			wantOk:              true,
		},
		{
			name:                "small model with 1 worker",
			conversionSizeBytes: 1 * 1024 * 1024 * 1024, // 1 GiB
			workerReplicas:      1,
			override:            nil,
			// 1GiB / 2 nodes = 512Mi per node
			// Root: 512Mi * 3.00 = 1536Mi
			// Worker: 512Mi * 2.50 = 1280Mi
			wantRootMiMin:       1500,
			wantRootMiMax:       1600,
			wantWorkerMiMin:     1250,
			wantWorkerMiMax:     1350,
			wantOk:              true,
		},
		{
			name:                "large model 70B with 7 workers",
			conversionSizeBytes: 140 * 1024 * 1024 * 1024, // 140 GiB
			workerReplicas:      7,
			override:            nil,
			// 140GiB / 8 nodes = 17.5GiB per node
			// Root: 17.5GiB * 2.50 (min ratio) = 43.75GiB
			// Worker: 17.5GiB * 2.50 = 43.75GiB
			wantRootMiMin:       44000,
			wantRootMiMax:       45000,
			wantWorkerMiMin:     44000,
			wantWorkerMiMax:     45000,
			wantOk:              true,
		},
		{
			name:                "zero size returns not ok",
			conversionSizeBytes: 0,
			workerReplicas:      3,
			override:            nil,
			wantOk:              false,
		},
		{
			name:                "negative size returns not ok",
			conversionSizeBytes: -1000,
			workerReplicas:      3,
			override:            nil,
			wantOk:              false,
		},
		{
			name:                "negative workers returns not ok",
			conversionSizeBytes: 1024 * 1024 * 1024,
			workerReplicas:      -1,
			override:            nil,
			wantOk:              false,
		},
		{
			name:                "extreme negative workers overflow guard",
			conversionSizeBytes: 1024,
			workerReplicas:      int32(math.MinInt32),
			override:            nil,
			wantOk:              false,
		},
		{
			name:                "zero workers with valid size",
			conversionSizeBytes: 1024 * 1024 * 1024,
			workerReplicas:      0,
			override:            nil,
			// Standalone mode: 1GiB / 1 node = 1GiB per node
			// Root: 1GiB * 3.00 = 3072Mi
			// Worker: 1GiB * 2.50 = 2560Mi
			wantRootMiMin:       3000,
			wantRootMiMax:       3200,
			wantWorkerMiMin:     2500,
			wantWorkerMiMax:     2700,
			wantOk:              true,
		},
		{
			name:                "tiny model rounds up to minimum",
			conversionSizeBytes: 100 * 1024, // 100 KiB
			workerReplicas:      1,
			override:            nil,
			wantRootMiMin:       1,
			wantRootMiMax:       2,
			wantWorkerMiMin:     1,
			wantWorkerMiMax:     2,
			wantOk:              true,
		},
		{
			name:                "with override ratio",
			conversionSizeBytes: 8 * 1024 * 1024 * 1024, // 8 GiB
			workerReplicas:      2,
			override:            floatPtr(5.0),
			// 8GiB / 3 nodes = 2.67GiB per node
			// Root: 2.67GiB * 5.0 (override) = 13.35GiB
			// Worker: 2.67GiB * 2.50 = 6.675GiB (but clamped to root)
			wantRootMiMin:       13000,
			wantRootMiMax:       14000,
			wantWorkerMiMin:     6600,
			wantWorkerMiMax:     6900,
			wantOk:              true,
		},
		{
			name:                "root memory not less than worker",
			conversionSizeBytes: 2 * 1024 * 1024 * 1024, // 2 GiB
			workerReplicas:      5,
			override:            floatPtr(1.1), // Low override gets clamped to min
			// 2GiB / 6 nodes = 341Mi per node
			// Override 1.1 < minRatio 2.50, so clamped to 2.50
			// Root: 341Mi * 2.50 = 853Mi
			// Worker: 341Mi * 2.50 = 853Mi
			wantRootMiMin:       850,
			wantRootMiMax:       860,
			wantWorkerMiMin:     850,
			wantWorkerMiMax:     860,
			wantOk:              true,
		},
		{
			name:                "override smaller than worker ratio clamps root",
			conversionSizeBytes: int64(1_992_294), // ~1.9 MiB total, forces rootMi < workerMi before clamp
			workerReplicas:      1,
			override:            floatPtr(1.01),
			// Override 1.01 < minRatio 2.50, so clamped to 2.50
			// 1.9Mi / 2 nodes = 0.95Mi per node
			// Root: 0.95Mi * 2.50 = 2.375Mi -> 3Mi
			// Worker: 0.95Mi * 2.50 = 2.375Mi -> 3Mi
			wantRootMiMin:       2,
			wantRootMiMax:       4,
			wantWorkerMiMin:     2,
			wantWorkerMiMax:     4,
			wantOk:              true,
		},
		{
			name:                "max worker replicas uses minimum allocations",
			conversionSizeBytes: 512 * 1024 * 1024 * 1024, // 512 GiB
			workerReplicas:      math.MaxInt32,
			override:            nil,
			wantRootMiMin:       1,
			wantRootMiMax:       2,
			wantWorkerMiMin:     1,
			wantWorkerMiMax:     2,
			wantOk:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, worker, ok := calculateMemoryRequests(tt.conversionSizeBytes, tt.workerReplicas, tt.override)

			if ok != tt.wantOk {
				t.Errorf("calculateMemoryRequests() ok = %v, want %v", ok, tt.wantOk)
				return
			}

			if !tt.wantOk {
				return
			}

			rootMi := root.Value() / (1024 * 1024)
			workerMi := worker.Value() / (1024 * 1024)

			if rootMi < tt.wantRootMiMin || rootMi > tt.wantRootMiMax {
				t.Errorf("calculateMemoryRequests() root = %dMi, want between %dMi and %dMi",
					rootMi, tt.wantRootMiMin, tt.wantRootMiMax)
			}

			if workerMi < tt.wantWorkerMiMin || workerMi > tt.wantWorkerMiMax {
				t.Errorf("calculateMemoryRequests() worker = %dMi, want between %dMi and %dMi",
					workerMi, tt.wantWorkerMiMin, tt.wantWorkerMiMax)
			}

			// Root should never be less than worker
			if rootMi < workerMi {
				t.Errorf("calculateMemoryRequests() root %dMi < worker %dMi, root should be >= worker",
					rootMi, workerMi)
			}
		})
	}
}

func TestCalculateMemoryRequestsRounding(t *testing.T) {
	conversionSizeBytes := int64(5 * 1024 * 1024) // 5 MiB model snapshot

	root, worker, ok := calculateMemoryRequests(conversionSizeBytes, 1, nil)
	if !ok {
		t.Fatal("calculateMemoryRequests() returned not ok for positive size")
	}

	rootMi := root.Value() / (1024 * 1024)
	workerMi := worker.Value() / (1024 * 1024)

	// 5Mi / 2 nodes = 2.5Mi per node
	// Worker: 2.5Mi * 2.50 = 6.25Mi -> 7Mi
	// Root: 2.5Mi * 3.00 = 7.5Mi -> 8Mi
	if workerMi != 7 {
		t.Fatalf("worker allocation should round up to 7Mi, got %dMi", workerMi)
	}
	if rootMi != 8 {
		t.Fatalf("root allocation should round up to 8Mi, got %dMi", rootMi)
	}
	if rootMi < workerMi {
		t.Fatalf("root allocation %dMi must be >= worker allocation %dMi", rootMi, workerMi)
	}
}

func TestCalculateMemoryRequests_Format(t *testing.T) {
	// Test that output is in correct Kubernetes resource format
	conversionSize := int64(4 * 1024 * 1024 * 1024) // 4 GiB
	workerReplicas := int32(2)

	root, worker, ok := calculateMemoryRequests(conversionSize, workerReplicas, nil)
	if !ok {
		t.Fatal("calculateMemoryRequests() returned not ok")
	}

	// Should be parseable as Kubernetes resource quantities
	rootStr := root.String()
	workerStr := worker.String()

	if rootStr == "" || workerStr == "" {
		t.Error("calculateMemoryRequests() returned empty quantity strings")
	}

	// Re-parse to ensure format is valid
	_, err := resource.ParseQuantity(rootStr)
	if err != nil {
		t.Errorf("root quantity %q is not valid: %v", rootStr, err)
	}

	_, err = resource.ParseQuantity(workerStr)
	if err != nil {
		t.Errorf("worker quantity %q is not valid: %v", workerStr, err)
	}
}

func TestFormatFloatPtr(t *testing.T) {

	if got := formatFloatPtr(nil); got != "nil" {
		t.Errorf("formatFloatPtr(nil) = %q, want %q", got, "nil")
	}

	value := 1.375
	if got := formatFloatPtr(&value); got != "1.375" {
		t.Errorf("formatFloatPtr() = %q, want %q", got, "1.375")
	}

	whole := 2.0
	if got := formatFloatPtr(&whole); got != "2" {
		t.Errorf("formatFloatPtr() = %q, want %q", got, "2")
	}
}

// Helper functions for tests

func floatPtr(f float64) *float64 {
	return &f
}

func formatFloatPtr(f *float64) string {
	if f == nil {
		return "nil"
	}
	return fmt.Sprintf("%g", *f)
}
