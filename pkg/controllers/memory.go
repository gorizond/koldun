package controllers

import (
	"fmt"
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	workerMemoryOverheadRatio = 2.50
	rootOverheadMaxRatio      = 3.00
	rootOverheadMinRatio      = 2.50
)

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

func calculateMemoryRequests(conversionSizeBytes int64, workerReplicas int32, override *float64) (root resource.Quantity, worker resource.Quantity, ok bool) {
	if conversionSizeBytes <= 0 {
		return resource.Quantity{}, resource.Quantity{}, false
	}

	totalNodes := int64(workerReplicas) + 1
	if totalNodes <= 0 {
		return resource.Quantity{}, resource.Quantity{}, false
	}

	perNodeMi := (float64(conversionSizeBytes) / (1024 * 1024)) / float64(totalNodes)

	workerMi := int64(math.Ceil(perNodeMi * workerMemoryOverheadRatio))
	rootMi := int64(math.Ceil(perNodeMi * rootMemoryOverheadRatio(workerReplicas, override)))

	return resource.MustParse(fmt.Sprintf("%dMi", rootMi)), resource.MustParse(fmt.Sprintf("%dMi", workerMi)), true
}
