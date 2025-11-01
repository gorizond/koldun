package controllers

import (
	"reflect"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	corev1 "k8s.io/api/core/v1"
)

// TestNormalizeForceToken is in model_token_test.go

func TestEffectiveDownloadSpec(t *testing.T) {
	tests := []struct {
		name     string
		spec     *v1.ModelDownloadSpec
		expected *v1.ModelDownloadSpec
	}{
		{
			name: "nil spec returns defaults",
			spec: nil,
			expected: &v1.ModelDownloadSpec{
				Image:       defaultDownloadImage,
				Memory:      "128Mi",
				ChunkMaxMiB: 64,
				Concurrency: 1,
			},
		},
		{
			name: "empty spec fills defaults",
			spec: &v1.ModelDownloadSpec{},
			expected: &v1.ModelDownloadSpec{
				Image:       defaultDownloadImage,
				Memory:      "128Mi",
				ChunkMaxMiB: 64,
				Concurrency: 1,
			},
		},
		{
			name: "partial spec fills missing fields",
			spec: &v1.ModelDownloadSpec{
				Image:  "custom-image",
				Memory: "256Mi",
			},
			expected: &v1.ModelDownloadSpec{
				Image:       "custom-image",
				Memory:      "256Mi",
				ChunkMaxMiB: 64,
				Concurrency: 1,
			},
		},
		{
			name: "full spec preserved",
			spec: &v1.ModelDownloadSpec{
				Image:       "my-image",
				Memory:      "512Mi",
				ChunkMaxMiB: 128,
				Concurrency: 4,
			},
			expected: &v1.ModelDownloadSpec{
				Image:       "my-image",
				Memory:      "512Mi",
				ChunkMaxMiB: 128,
				Concurrency: 4,
			},
		},
		{
			name: "negative values reset to defaults",
			spec: &v1.ModelDownloadSpec{
				Image:       "my-image",
				ChunkMaxMiB: -10,
				Concurrency: -5,
			},
			expected: &v1.ModelDownloadSpec{
				Image:       "my-image",
				Memory:      "128Mi",
				ChunkMaxMiB: 64,
				Concurrency: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := effectiveDownloadSpec(tt.spec)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("effectiveDownloadSpec() = %+v, want %+v", result, tt.expected)
			}
		})
	}
}

func TestEffectiveConversionSpec(t *testing.T) {
	tests := []struct {
		name     string
		spec     *v1.ModelConversionSpec
		expected *v1.ModelConversionSpec
	}{
		{
			name: "nil spec returns defaults",
			spec: nil,
			expected: &v1.ModelConversionSpec{
				Image:            defaultConversionImage,
				WeightsFloatType: defaultWeightsType,
				Memory:           "2Gi",
				ConvertWeights:   defaultWeightsType,
			},
		},
		{
			name: "empty spec fills defaults",
			spec: &v1.ModelConversionSpec{},
			expected: &v1.ModelConversionSpec{
				Image:            defaultConversionImage,
				WeightsFloatType: defaultWeightsType,
				Memory:           "2Gi",
				ConverterVersion: "v0.16.2",
				ConvertWeights:   defaultWeightsType,
			},
		},
		{
			name: "partial spec fills missing fields",
			spec: &v1.ModelConversionSpec{
				Image:  "custom-converter",
				Memory: "4Gi",
			},
			expected: &v1.ModelConversionSpec{
				Image:            "custom-converter",
				WeightsFloatType: defaultWeightsType,
				Memory:           "4Gi",
				ConverterVersion: "v0.16.2",
				ConvertWeights:   defaultWeightsType,
			},
		},
		{
			name: "ConvertWeights inherits from WeightsFloatType",
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
			},
			expected: &v1.ModelConversionSpec{
				Image:            defaultConversionImage,
				WeightsFloatType: "Q4_0",
				Memory:           "2Gi",
				ConverterVersion: "v0.16.2",
				ConvertWeights:   "Q4_0",
			},
		},
		{
			name: "explicit ConvertWeights preserved",
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
				ConvertWeights:   "Q8_0",
			},
			expected: &v1.ModelConversionSpec{
				Image:            defaultConversionImage,
				WeightsFloatType: "Q4_0",
				Memory:           "2Gi",
				ConverterVersion: "v0.16.2",
				ConvertWeights:   "Q8_0",
			},
		},
		{
			name: "whitespace ConvertWeights uses WeightsFloatType",
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
				ConvertWeights:   "  ",
			},
			expected: &v1.ModelConversionSpec{
				Image:            defaultConversionImage,
				WeightsFloatType: "Q4_0",
				Memory:           "2Gi",
				ConverterVersion: "v0.16.2",
				ConvertWeights:   "Q4_0",
			},
		},
		{
			name: "whitespace ConvertWeights and empty WeightsFloatType uses default",
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "",
				ConvertWeights:   "  ",
			},
			expected: &v1.ModelConversionSpec{
				Image:            defaultConversionImage,
				WeightsFloatType: defaultWeightsType,
				Memory:           "2Gi",
				ConverterVersion: "v0.16.2",
				ConvertWeights:   defaultWeightsType,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := effectiveConversionSpec(tt.spec)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("effectiveConversionSpec() = %+v, want %+v", result, tt.expected)
			}
		})
	}
}

func TestUniqueNonEmpty(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected []string
	}{
		{
			name:     "empty input",
			values:   []string{},
			expected: []string{},
		},
		{
			name:     "all empty strings",
			values:   []string{"", "  ", "\t"},
			expected: []string{},
		},
		{
			name:     "unique values",
			values:   []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "duplicate values",
			values:   []string{"a", "b", "a", "c", "b"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "mixed empty and non-empty",
			values:   []string{"", "a", "  ", "b", "", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "values with whitespace",
			values:   []string{" a ", "a", " b", "b ", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "duplicate after trim",
			values:   []string{" foo ", "foo", "  foo  "},
			expected: []string{"foo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := uniqueNonEmpty(tt.values...)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("uniqueNonEmpty() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestValueFromSecret(t *testing.T) {
	tests := []struct {
		name     string
		secret   *corev1.Secret
		keys     []string
		expected string
	}{
		{
			name:     "nil secret",
			secret:   nil,
			keys:     []string{"key1"},
			expected: "",
		},
		{
			name: "key exists",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"api_key": []byte("secret-value"),
				},
			},
			keys:     []string{"api_key"},
			expected: "secret-value",
		},
		{
			name: "key not found",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"other_key": []byte("value"),
				},
			},
			keys:     []string{"api_key"},
			expected: "",
		},
		{
			name: "lowercase fallback",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"api_key": []byte("secret-value"),
				},
			},
			keys:     []string{"API_KEY"},
			expected: "secret-value",
		},
		{
			name: "multiple keys, first match wins",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"key1": []byte("value1"),
					"key2": []byte("value2"),
				},
			},
			keys:     []string{"key2", "key1"},
			expected: "value2",
		},
		{
			name: "empty value skipped",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"key1": []byte(""),
					"key2": []byte("value2"),
				},
			},
			keys:     []string{"key1", "key2"},
			expected: "value2",
		},
		{
			name: "whitespace value skipped",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"key1": []byte("   "),
					"key2": []byte("value2"),
				},
			},
			keys:     []string{"key1", "key2"},
			expected: "value2",
		},
		{
			name: "value with whitespace trimmed",
			secret: &corev1.Secret{
				Data: map[string][]byte{
					"key1": []byte("  value1  "),
				},
			},
			keys:     []string{"key1"},
			expected: "value1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueFromSecret(tt.secret, tt.keys...)
			if result != tt.expected {
				t.Errorf("valueFromSecret() = %q, want %q", result, tt.expected)
			}
		})
	}
}
