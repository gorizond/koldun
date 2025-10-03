package controllers

import (
	"fmt"
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	workerMemoryOverheadRatio = 1.10
	rootOverheadMaxRatio      = 1.50
	rootOverheadMinRatio      = 1.20
)

func rootMemoryOverheadRatio(workerReplicas int32) float64 {
	if workerReplicas <= 1 {
		return rootOverheadMaxRatio
	}
	decayWorkers := float64(workerReplicas-1) / 6.0
	if decayWorkers > 1 {
		decayWorkers = 1
	}
	ratio := rootOverheadMaxRatio - (rootOverheadMaxRatio-rootOverheadMinRatio)*decayWorkers
	if ratio < rootOverheadMinRatio {
		ratio = rootOverheadMinRatio
	}
	return ratio
}

func calculateMemoryRequests(conversionSizeBytes int64, workerReplicas int32) (root resource.Quantity, worker resource.Quantity, ok bool) {
	if conversionSizeBytes <= 0 {
		return resource.Quantity{}, resource.Quantity{}, false
	}

	totalNodes := int64(workerReplicas) + 1
	if totalNodes <= 0 {
		return resource.Quantity{}, resource.Quantity{}, false
	}

	perNodeMi := (float64(conversionSizeBytes) / (1024 * 1024)) / float64(totalNodes)

	workerMi := int64(math.Ceil(perNodeMi * workerMemoryOverheadRatio))
	if workerMi < 1 {
		workerMi = 1
	}

	rootMi := int64(math.Ceil(perNodeMi * rootMemoryOverheadRatio(workerReplicas)))
	if rootMi < workerMi {
		rootMi = workerMi
	}

	return resource.MustParse(fmt.Sprintf("%dMi", rootMi)), resource.MustParse(fmt.Sprintf("%dMi", workerMi)), true
}
