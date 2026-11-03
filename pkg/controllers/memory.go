package controllers

import (
	"fmt"
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	workerMemoryOverheadRatio = 2.50
	// workerOverheadMinRatio is the lower bound for the worker memory
	// multiplier: the limit must never be smaller than the per-node weight
	// share, otherwise the worker cannot start.
	workerOverheadMinRatio = 1.00
	rootOverheadMaxRatio   = 3.00
	rootOverheadMinRatio   = 2.50
)

// workerOverheadRatio resolves the worker memory multiplier. Unset or
// non-positive overrides fall back to the default; values below the minimum
// are clamped so a worker always gets at least its per-node weight share.
func workerOverheadRatio(override *float64) float64 {
	if override != nil && *override > 0 {
		val := *override
		if val < workerOverheadMinRatio {
			return workerOverheadMinRatio
		}
		return val
	}
	return workerMemoryOverheadRatio
}

func rootMemoryOverheadRatio(workerReplicas int32, override *float64) float64 {
	if override != nil && *override > 0 {
		val := *override
		if val < rootOverheadMinRatio {
			return rootOverheadMinRatio
		}
		return val
	}
	if workerReplicas <= 1 {
		return rootOverheadMaxRatio
	}
	decayWorkers := float64(workerReplicas-1) / 6.0
	if decayWorkers > 1 {
		decayWorkers = 1
	}
	ratio := rootOverheadMaxRatio - (rootOverheadMaxRatio-rootOverheadMinRatio)*decayWorkers
	return math.Max(ratio, rootOverheadMinRatio)
}

func calculateMemoryRequests(conversionSizeBytes int64, workerReplicas int32, override *float64, workerOverride *float64) (root resource.Quantity, worker resource.Quantity, ok bool) {
	if conversionSizeBytes <= 0 {
		return resource.Quantity{}, resource.Quantity{}, false
	}

	totalNodes := int64(workerReplicas) + 1
	if totalNodes <= 0 {
		return resource.Quantity{}, resource.Quantity{}, false
	}

	perNodeMi := (float64(conversionSizeBytes) / (1024 * 1024)) / float64(totalNodes)

	workerMi := int64(math.Ceil(perNodeMi * workerOverheadRatio(workerOverride)))
	rootMi := int64(math.Ceil(perNodeMi * rootMemoryOverheadRatio(workerReplicas, override)))

	return resource.MustParse(fmt.Sprintf("%dMi", rootMi)), resource.MustParse(fmt.Sprintf("%dMi", workerMi)), true
}
