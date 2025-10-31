package controllers

import (
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	corev1 "k8s.io/api/core/v1"
)

// model_helpers.go contains utility functions for the model controller.
// These are pure functions with no side effects, making them easy to test and reuse.

// normalizeForceToken generates a unique token for force size rerun requests.
// If requested is true but value is empty, it generates a token based on resourceVersion.
func normalizeForceToken(value string, requested bool, resourceVersion string) string {
	token := strings.TrimSpace(value)
	if requested && token == "" {
		token = fmt.Sprintf("annotation-rv-%s", resourceVersion)
	}
	return token
}

// effectiveDownloadSpec returns a ModelDownloadSpec with defaults applied.
// If spec is nil, returns a spec with all default values.
// Otherwise, fills in missing fields with sensible defaults.
func effectiveDownloadSpec(spec *v1.ModelDownloadSpec) *v1.ModelDownloadSpec {
	if spec == nil {
		return &v1.ModelDownloadSpec{
			Image:       defaultDownloadImage,
			Memory:      "128Mi",
			ChunkMaxMiB: 64,
			Concurrency: 1,
		}
	}
	out := spec.DeepCopy()
	if out.Image == "" {
		out.Image = defaultDownloadImage
	}
	if out.Memory == "" {
		out.Memory = "128Mi"
	}
	if out.ChunkMaxMiB <= 0 {
		out.ChunkMaxMiB = 64
	}
	if out.Concurrency <= 0 {
		out.Concurrency = 1
	}
	return out
}

// effectiveConversionSpec returns a ModelConversionSpec with defaults applied.
// If spec is nil, returns a spec with all default values.
// Otherwise, fills in missing fields with sensible defaults.
func effectiveConversionSpec(spec *v1.ModelConversionSpec) *v1.ModelConversionSpec {
	if spec == nil {
		return &v1.ModelConversionSpec{
			Image:            defaultConversionImage,
			WeightsFloatType: defaultWeightsType,
			Memory:           "2Gi",
			ConvertWeights:   defaultWeightsType,
		}
	}
	out := spec.DeepCopy()
	if out.Image == "" {
		out.Image = defaultConversionImage
	}
	if out.WeightsFloatType == "" {
		out.WeightsFloatType = defaultWeightsType
	}
	if out.Memory == "" {
		out.Memory = "2Gi"
	}
	if out.ConverterVersion == "" {
		out.ConverterVersion = "v0.16.2"
	}
	if strings.TrimSpace(out.ConvertWeights) == "" {
		if wt := strings.TrimSpace(out.WeightsFloatType); wt != "" {
			out.ConvertWeights = wt
		} else {
			out.ConvertWeights = defaultWeightsType
		}
	}
	return out
}

// uniqueNonEmpty returns a deduplicated slice containing only non-empty trimmed values.
// Order is preserved for the first occurrence of each unique value.
func uniqueNonEmpty(values ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

// valueFromSecret extracts a non-empty value from a Secret by trying multiple keys.
// Keys are tried in order, and both original and lowercase versions are checked.
// Returns empty string if secret is nil or no matching key with non-empty value is found.
func valueFromSecret(secret *corev1.Secret, keys ...string) string {
	if secret == nil {
		return ""
	}
	for _, key := range keys {
		if data, ok := secret.Data[key]; ok {
			if v := strings.TrimSpace(string(data)); v != "" {
				return v
			}
		}
		lower := strings.ToLower(key)
		if data, ok := secret.Data[lower]; ok {
			if v := strings.TrimSpace(string(data)); v != "" {
				return v
			}
		}
	}
	return ""
}
