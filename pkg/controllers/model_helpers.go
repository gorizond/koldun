package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	corev1 "k8s.io/api/core/v1"
)

// model_helpers.go contains utility functions for the model controller.
// These are pure functions with no side effects, making them easy to test and reuse.

// modelArtifactSpec captures the ModelSpec fields that influence the artifact
// pipeline. pipProxy is included because it is injected into the download and
// conversion containers: fixing a broken proxy must allow a retry.
type modelArtifactSpec struct {
	SourceURL             string                     `json:"sourceUrl"`
	LocalPath             string                     `json:"localPath"`
	ObjectStorage         *v1.ModelObjectStorageSpec `json:"objectStorage,omitempty"`
	Download              *v1.ModelDownloadSpec      `json:"download,omitempty"`
	Conversion            *v1.ModelConversionSpec    `json:"conversion,omitempty"`
	PV                    *v1.ModelPVSpec            `json:"pv,omitempty"`
	PreConverted          bool                       `json:"preConverted,omitempty"`
	PreConvertedPVCName   string                     `json:"preConvertedPVCName,omitempty"`
	PreConvertedSizeBytes int64                      `json:"preConvertedSizeBytes,omitempty"`
	PipProxy              string                     `json:"pipProxy,omitempty"`
}

func modelSpecHash(obj *v1.Model, includeConversion bool) string {
	payload := modelArtifactSpec{
		SourceURL:             obj.Spec.SourceURL,
		LocalPath:             obj.Spec.LocalPath,
		ObjectStorage:         obj.Spec.ObjectStorage,
		Download:              obj.Spec.Download,
		PV:                    obj.Spec.PV,
		PreConverted:          obj.Spec.PreConverted,
		PreConvertedPVCName:   obj.Spec.PreConvertedPVCName,
		PreConvertedSizeBytes: obj.Spec.PreConvertedSizeBytes,
		PipProxy:              obj.Spec.PipProxy,
	}
	if includeConversion {
		payload.Conversion = obj.Spec.Conversion
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// modelDownloadHash covers the fields that determine the downloaded source
// artifacts. Conversion-only changes keep this hash stable, so a finished
// download job is not recreated (and the source bucket is not rewritten).
func modelDownloadHash(obj *v1.Model) string {
	return modelSpecHash(obj, false)
}

// modelArtifactHash covers the downloaded artifacts plus conversion settings.
// Runtime-only fields (maxSeqLen, replicaPower, launchOptions) are deliberately
// excluded so tuning them does not restart multi-hour jobs.
func modelArtifactHash(obj *v1.Model) string {
	return modelSpecHash(obj, true)
}

func modelStageCurrent(recorded string, obj *v1.Model, hash string) bool {
	if recorded != "" {
		return recorded == hash
	}
	// Missing hash means legacy status written before hashes existed: fall
	// back to the generation gate so the first rollout does not restart jobs.
	return obj.Status.ObservedGeneration == obj.Generation
}

// modelDownloadCurrent reports whether Status.DownloadHash matches hash.
func modelDownloadCurrent(obj *v1.Model, hash string) bool {
	return modelStageCurrent(obj.Status.DownloadHash, obj, hash)
}

// modelArtifactsCurrent reports whether Status.ArtifactHash matches hash.
func modelArtifactsCurrent(obj *v1.Model, hash string) bool {
	return modelStageCurrent(obj.Status.ArtifactHash, obj, hash)
}

// normalizeForceToken generates a unique token for force size rerun requests.
// If requested is true but value is empty, it generates a token based on resourceVersion.
func normalizeForceToken(value string, requested bool, resourceVersion string) string {
	token := strings.TrimSpace(value)
	if requested && token == "" {
		token = fmt.Sprintf("annotation-rv-%s", resourceVersion)
	}
	return token
}

// forceTokenMatches reports whether the processed sizing token satisfies the requested token.
// It treats tokens as equal when they match exactly or when the processed token preserves the
// requested value with a "-<resourceVersion>" suffix produced by older runs.
func forceTokenMatches(requested, processed, resourceVersion string) bool {
	req := strings.TrimSpace(requested)
	proc := strings.TrimSpace(processed)
	if req == "" {
		return proc == ""
	}
	if req == proc {
		return true
	}
	if resourceVersion != "" && proc == req+"-"+resourceVersion {
		return true
	}
	return false
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
	if out.WeightsFloatType == "" {
		out.WeightsFloatType = defaultWeightsType
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
