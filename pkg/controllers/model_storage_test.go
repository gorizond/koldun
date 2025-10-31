package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name           string
		value          string
		expectedBucket string
		expectedKey    string
		expectedOk     bool
	}{
		{
			name:           "valid s3 URI with key",
			value:          "s3://my-bucket/models/llama",
			expectedBucket: "my-bucket",
			expectedKey:    "models/llama",
			expectedOk:     true,
		},
		{
			name:           "valid s3 URI without key",
			value:          "s3://my-bucket",
			expectedBucket: "my-bucket",
			expectedKey:    "",
			expectedOk:     true,
		},
		{
			name:           "valid s3 URI with trailing slash",
			value:          "s3://my-bucket/",
			expectedBucket: "my-bucket",
			expectedKey:    "",
			expectedOk:     true,
		},
		{
			name:           "valid s3 URI with deep key path",
			value:          "s3://bucket/path/to/models/file.gguf",
			expectedBucket: "bucket",
			expectedKey:    "path/to/models/file.gguf",
			expectedOk:     true,
		},
		{
			name:           "valid s3 URI with leading slash in key",
			value:          "s3://bucket//models/llama",
			expectedBucket: "bucket",
			expectedKey:    "models/llama",
			expectedOk:     true,
		},
		{
			name:           "not an s3 URI (http)",
			value:          "http://example.com/path",
			expectedBucket: "",
			expectedKey:    "",
			expectedOk:     false,
		},
		{
			name:           "not an s3 URI (plain path)",
			value:          "/local/path/to/model",
			expectedBucket: "",
			expectedKey:    "",
			expectedOk:     false,
		},
		{
			name:           "invalid s3 URI (no bucket)",
			value:          "s3://",
			expectedBucket: "",
			expectedKey:    "",
			expectedOk:     false,
		},
		{
			name:           "invalid s3 URI (empty bucket)",
			value:          "s3:///models/llama",
			expectedBucket: "",
			expectedKey:    "",
			expectedOk:     false,
		},
		{
			name:           "empty string",
			value:          "",
			expectedBucket: "",
			expectedKey:    "",
			expectedOk:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, ok := parseS3Path(tt.value)
			if ok != tt.expectedOk {
				t.Errorf("parseS3Path(%q) ok = %v, want %v", tt.value, ok, tt.expectedOk)
			}
			if bucket != tt.expectedBucket {
				t.Errorf("parseS3Path(%q) bucket = %q, want %q", tt.value, bucket, tt.expectedBucket)
			}
			if key != tt.expectedKey {
				t.Errorf("parseS3Path(%q) key = %q, want %q", tt.value, key, tt.expectedKey)
			}
		})
	}
}

func TestModelObjectKey(t *testing.T) {
	tests := []struct {
		name        string
		model       *v1.Model
		expectedKey string
	}{
		{
			name: "s3 URI in LocalPath",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "s3://my-bucket/models/llama",
				},
			},
			expectedKey: "models/llama",
		},
		{
			name: "relative path in LocalPath",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "models/llama/weights",
				},
			},
			expectedKey: "models/llama/weights",
		},
		{
			name: "absolute path in LocalPath",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "/models/llama/weights",
				},
			},
			expectedKey: "models/llama/weights",
		},
		{
			name: "LocalPath with leading slash trimmed",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "///models/llama",
				},
			},
			expectedKey: "models/llama",
		},
		{
			name: "empty LocalPath",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "",
				},
			},
			expectedKey: "",
		},
		{
			name: "whitespace only LocalPath",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "   ",
				},
			},
			expectedKey: "",
		},
		{
			name: "s3 URI with bucket only",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: "s3://my-bucket",
				},
			},
			expectedKey: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := modelObjectKey(tt.model)
			if result != tt.expectedKey {
				t.Errorf("modelObjectKey() = %q, want %q", result, tt.expectedKey)
			}
		})
	}
}

func TestConversionPaths(t *testing.T) {
	tests := []struct {
		name            string
		model           *v1.Model
		spec            *v1.ModelConversionSpec
		defaultInputKey string
		expectedWorkDir string
		expectedBucket  string
		expectedKey     string
		expectedURI     string
	}{
		{
			name: "default conversion with BucketForConvert",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForSource:  "source-bucket",
						BucketForConvert: "convert-bucket",
					},
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/hf",
			expectedBucket:  "convert-bucket",
			expectedKey:     "models/llama/converted/Q4_0/llama-model",
			expectedURI:     "s3://convert-bucket/models/llama/converted/Q4_0/llama-model",
		},
		{
			name: "fallback to BucketForSource when BucketForConvert is empty",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForSource:  "source-bucket",
						BucketForConvert: "",
					},
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/hf",
			expectedBucket:  "source-bucket",
			expectedKey:     "models/llama/converted/Q4_0/llama-model",
			expectedURI:     "s3://source-bucket/models/llama/converted/Q4_0/llama-model",
		},
		{
			name: "custom OutputPath as s3 URI",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForConvert: "convert-bucket",
					},
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q8_0",
				OutputPath:       "s3://custom-bucket/custom/path",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/hf",
			expectedBucket:  "custom-bucket",
			expectedKey:     "custom/path/llama-model",
			expectedURI:     "s3://custom-bucket/custom/path/llama-model",
		},
		{
			name: "custom OutputPath as local filesystem path",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForConvert: "convert-bucket",
					},
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
				OutputPath:       "custom/local/path",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/custom/local/path",
			expectedBucket:  "convert-bucket",
			expectedKey:     "models/llama/converted/Q4_0/llama-model",
			expectedURI:     "s3://convert-bucket/models/llama/converted/Q4_0/llama-model",
		},
		{
			name: "empty WeightsFloatType uses default",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForConvert: "convert-bucket",
					},
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/hf",
			expectedBucket:  "convert-bucket",
			expectedKey:     "models/llama/converted/q40/llama-model",
			expectedURI:     "s3://convert-bucket/models/llama/converted/q40/llama-model",
		},
		{
			name: "no object storage configured",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: nil,
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/hf",
			expectedBucket:  "",
			expectedKey:     "models/llama/converted/Q4_0/llama-model",
			expectedURI:     "",
		},
		{
			name: "s3 URI with bucket only in OutputPath",
			model: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "llama-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForConvert: "convert-bucket",
					},
				},
			},
			spec: &v1.ModelConversionSpec{
				WeightsFloatType: "Q4_0",
				OutputPath:       "s3://custom-bucket",
			},
			defaultInputKey: "models/llama",
			expectedWorkDir: "/workspace/hf",
			expectedBucket:  "custom-bucket",
			expectedKey:     "models/llama/converted/llama-model",
			expectedURI:     "s3://custom-bucket/models/llama/converted/llama-model",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workDir, bucket, key, uri := conversionPaths(tt.model, tt.spec, tt.defaultInputKey)

			if workDir != tt.expectedWorkDir {
				t.Errorf("conversionPaths() workDir = %q, want %q", workDir, tt.expectedWorkDir)
			}
			if bucket != tt.expectedBucket {
				t.Errorf("conversionPaths() bucket = %q, want %q", bucket, tt.expectedBucket)
			}
			if key != tt.expectedKey {
				t.Errorf("conversionPaths() key = %q, want %q", key, tt.expectedKey)
			}
			if uri != tt.expectedURI {
				t.Errorf("conversionPaths() uri = %q, want %q", uri, tt.expectedURI)
			}
		})
	}
}

// TestSplitKey is already implemented in dllama_test.go
