package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantBucket string
		wantKey    string
		wantOk     bool
	}{
		{
			name:       "valid s3 uri with bucket and key",
			value:      "s3://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantKey:    "path/to/object",
			wantOk:     true,
		},
		{
			name:       "valid s3 uri with bucket only",
			value:      "s3://my-bucket",
			wantBucket: "my-bucket",
			wantKey:    "",
			wantOk:     true,
		},
		{
			name:       "valid s3 uri with bucket and trailing slash",
			value:      "s3://my-bucket/",
			wantBucket: "my-bucket",
			wantKey:    "",
			wantOk:     true,
		},
		{
			name:       "valid s3 uri with multiple path segments",
			value:      "s3://bucket/models/llama/weights.bin",
			wantBucket: "bucket",
			wantKey:    "models/llama/weights.bin",
			wantOk:     true,
		},
		{
			name:       "not an s3 uri - http",
			value:      "http://example.com/file",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
		},
		{
			name:       "not an s3 uri - plain path",
			value:      "/path/to/file",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
		},
		{
			name:       "empty value",
			value:      "",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
		},
		{
			name:       "s3 prefix only",
			value:      "s3://",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
		},
		{
			name:       "s3 with empty bucket name",
			value:      "s3:///path",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
		},
		{
			name:       "s3 with whitespace bucket",
			value:      "s3://   /path",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
		},
		{
			name:       "s3 with leading slashes in key",
			value:      "s3://bucket///path/to/object",
			wantBucket: "bucket",
			wantKey:    "path/to/object",
			wantOk:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, ok := parseS3Path(tt.value)
			if ok != tt.wantOk {
				t.Errorf("parseS3Path() ok = %v, want %v", ok, tt.wantOk)
			}
			if bucket != tt.wantBucket {
				t.Errorf("parseS3Path() bucket = %v, want %v", bucket, tt.wantBucket)
			}
			if key != tt.wantKey {
				t.Errorf("parseS3Path() key = %v, want %v", key, tt.wantKey)
			}
		})
	}
}

// TestSplitKey is already defined in dllama_test.go

func TestModelObjectKey(t *testing.T) {
	tests := []struct {
		name      string
		localPath string
		wantKey   string
	}{
		{
			name:      "s3 uri with key",
			localPath: "s3://bucket/models/llama",
			wantKey:   "models/llama",
		},
		{
			name:      "s3 uri with leading slash in key",
			localPath: "s3://bucket//models/llama",
			wantKey:   "models/llama",
		},
		{
			name:      "plain path",
			localPath: "/models/llama",
			wantKey:   "models/llama",
		},
		{
			name:      "plain path without leading slash",
			localPath: "models/llama",
			wantKey:   "models/llama",
		},
		{
			name:      "empty path",
			localPath: "",
			wantKey:   "",
		},
		{
			name:      "whitespace path",
			localPath: "   ",
			wantKey:   "",
		},
		{
			name:      "s3 uri bucket only",
			localPath: "s3://bucket",
			wantKey:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1.ModelSpec{
					LocalPath: tt.localPath,
				},
			}
			key := modelObjectKey(model)
			if key != tt.wantKey {
				t.Errorf("modelObjectKey() = %v, want %v", key, tt.wantKey)
			}
		})
	}
}

func TestConversionPaths(t *testing.T) {
	tests := []struct {
		name             string
		modelName        string
		objectStorage    *v1.ModelObjectStorageSpec
		conversionSpec   *v1.ModelConversionSpec
		defaultInputKey  string
		wantWorkDir      string
		wantBucket       string
		wantKey          string
		wantURISubstring string // Check if URI contains this substring
	}{
		{
			name:      "default conversion paths",
			modelName: "llama-7b",
			objectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.example.com",
				BucketForSource:  "models",
				BucketForConvert: "converted-models",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "f16",
			},
			defaultInputKey:  "downloads/llama",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "converted-models",
			wantKey:          "downloads/llama/converted/f16/llama-7b",
			wantURISubstring: "s3://converted-models/downloads/llama/converted/f16/llama-7b",
		},
		{
			name:      "empty weights type defaults to q40",
			modelName: "llama-7b",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "converted",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "",
			},
			defaultInputKey:  "models/llama",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "converted",
			wantKey:          "models/llama/converted/q40/llama-7b",
			wantURISubstring: "s3://converted/models/llama/converted/q40/llama-7b",
		},
		{
			name:      "custom s3 output path",
			modelName: "llama-7b",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "default-bucket",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "f16",
				OutputPath:       "s3://custom-bucket/custom/path",
			},
			defaultInputKey:  "downloads/llama",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "custom-bucket",
			wantKey:          "custom/path/llama-7b",
			wantURISubstring: "s3://custom-bucket/custom/path/llama-7b",
		},
		{
			name:      "custom local output path",
			modelName: "llama-7b",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "bucket",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "f16",
				OutputPath:       "/custom/local/path",
			},
			defaultInputKey:  "downloads",
			wantWorkDir:      "/custom/local/path",
			wantBucket:       "bucket",
			wantKey:          "downloads/converted/f16/llama-7b",
			wantURISubstring: "s3://bucket/downloads/converted/f16/llama-7b",
		},
		{
			name:      "relative output path becomes absolute",
			modelName: "model",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "bucket",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "f32",
				OutputPath:       "relative/path",
			},
			defaultInputKey:  "input",
			wantWorkDir:      "/workspace/relative/path",
			wantBucket:       "bucket",
			wantKey:          "input/converted/f32/model",
			wantURISubstring: "s3://bucket/input/converted/f32/model",
		},
		{
			name:      "no bucket for convert falls back to source bucket",
			modelName: "llama",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "source-bucket",
				BucketForConvert: "",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "q8",
			},
			defaultInputKey:  "models",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "source-bucket",
			wantKey:          "models/converted/q8/llama",
			wantURISubstring: "s3://source-bucket/models/converted/q8/llama",
		},
		{
			name:      "s3 output path with bucket override only",
			modelName: "model",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "default",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "f16",
				OutputPath:       "s3://override-bucket",
			},
			defaultInputKey:  "input",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "override-bucket",
			wantKey:          "input/converted/model",
			wantURISubstring: "s3://override-bucket/input/converted/model",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.modelName,
				},
				Spec: v1.ModelSpec{
					ObjectStorage: tt.objectStorage,
				},
			}

			workDir, bucket, key, uri := conversionPaths(model, tt.conversionSpec, tt.defaultInputKey)

			if workDir != tt.wantWorkDir {
				t.Errorf("conversionPaths() workDir = %v, want %v", workDir, tt.wantWorkDir)
			}
			if bucket != tt.wantBucket {
				t.Errorf("conversionPaths() bucket = %v, want %v", bucket, tt.wantBucket)
			}
			if key != tt.wantKey {
				t.Errorf("conversionPaths() key = %v, want %v", key, tt.wantKey)
			}
			if tt.wantURISubstring != "" && uri != tt.wantURISubstring {
				t.Errorf("conversionPaths() uri = %v, want %v", uri, tt.wantURISubstring)
			}
		})
	}
}
