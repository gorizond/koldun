package tokens

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gorizond/koldun/pkg/registry"
	corev1 "k8s.io/api/core/v1"
)

const (
	// LabelToken marks Secrets that should be treated as Koldun API tokens.
	LabelToken = "koldun.gorizond.io/token"
	// DataHashKey is the Secret data key that carries the SHA-256 token hash.
	DataHashKey = "hash"
	// DataDisabledKey optionally toggles whether the token is disabled.
	DataDisabledKey = "disabled"
	// DataMetadataKey optionally embeds a JSON object with arbitrary metadata.
	DataMetadataKey = "metadata"
	// AnnotationDisabled optionally toggles whether the token is disabled.
	AnnotationDisabled = "koldun.gorizond.io/token-disabled"
	// AnnotationMetadataPrefix adds arbitrary metadata entries sourced from annotations.
	AnnotationMetadataPrefix = "koldun.gorizond.io/token-metadata-"
)

var truthyValues = map[string]bool{
	"1":    true,
	"true": true,
	"yes":  true,
	"y":    true,
	"on":   true,
}

var falsyValues = map[string]bool{
	"0":     true,
	"false": true,
	"no":    true,
	"n":     true,
	"off":   true,
}

// IsTokenSecret returns true when the Secret carries the token label set to a truthy value.
func IsTokenSecret(secret *corev1.Secret) bool {
	if secret == nil {
		return false
	}
	return parseLabelBool(secret.Labels[LabelToken])
}

// Hash extracts the normalised token hash from the Secret data, if present.
func Hash(secret *corev1.Secret) string {
	if secret == nil {
		return ""
	}
	raw := string(secret.Data[DataHashKey])
	return strings.ToLower(strings.TrimSpace(raw))
}

// ExtractRegistryToken converts a Secret into the registry Token payload.
func ExtractRegistryToken(secret *corev1.Secret) (*registry.Token, error) {
	if secret == nil {
		return nil, fmt.Errorf("secret is nil")
	}

	hash := Hash(secret)
	if hash == "" {
		return nil, fmt.Errorf("secret %s/%s missing token hash", secret.Namespace, secret.Name)
	}

	disabled, err := disabledFromSecret(secret)
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s disabled flag: %w", secret.Namespace, secret.Name, err)
	}

	metadata, err := metadataFromSecret(secret)
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s metadata: %w", secret.Namespace, secret.Name, err)
	}

	return &registry.Token{
		Hash:      hash,
		Disabled:  disabled,
		Namespace: secret.Namespace,
		Metadata:  metadata,
	}, nil
}

func disabledFromSecret(secret *corev1.Secret) (bool, error) {
	if secret == nil {
		return false, nil
	}
	raw := string(secret.Data[DataDisabledKey])
	if strings.TrimSpace(raw) == "" {
		raw = secret.Annotations[AnnotationDisabled]
	}
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	value := normalise(raw)
	if truthyValues[value] {
		return true, nil
	}
	if falsyValues[value] {
		return false, nil
	}
	return false, fmt.Errorf("invalid boolean value %q", raw)
}

func metadataFromSecret(secret *corev1.Secret) (map[string]string, error) {
	if secret == nil {
		return nil, nil
	}

	var metadata map[string]string
	if raw, ok := secret.Data[DataMetadataKey]; ok {
		if strings.TrimSpace(string(raw)) != "" {
			if err := json.Unmarshal(raw, &metadata); err != nil {
				return nil, fmt.Errorf("parse json: %w", err)
			}
		}
	}

	for key, value := range secret.Annotations {
		if !strings.HasPrefix(key, AnnotationMetadataPrefix) {
			continue
		}
		trimmed := strings.TrimPrefix(key, AnnotationMetadataPrefix)
		if trimmed == "" {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[trimmed] = value
	}

	if len(metadata) == 0 {
		return nil, nil
	}
	return metadata, nil
}

func parseLabelBool(value string) bool {
	value = normalise(value)
	if value == "" {
		return false
	}
	if truthyValues[value] {
		return true
	}
	if falsyValues[value] {
		return false
	}
	return false
}

func normalise(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// IsHashJetStreamSafe reports whether the provided token hash can be used as a JetStream KeyValue key segment.
// JetStream rejects keys containing path separators or control characters. Koldun hashes should be lowercase
// hex strings, so we only allow [a-z0-9_-] to ensure compatibility while still protecting against
// unexpected secret contents (e.g. cluster bootstrap secrets).
func IsHashJetStreamSafe(hash string) bool {
	if strings.TrimSpace(hash) == "" {
		return false
	}
	for _, r := range hash {
		switch {
		case r >= 'a' && r <= 'z':
			continue
		case r >= '0' && r <= '9':
			continue
		case r == '-' || r == '_':
			continue
		default:
			return false
		}
	}
	return true
}
