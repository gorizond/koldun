package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestParseS3Path tests the parseS3Path function which parses S3 URIs
func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantBucket string
		wantKey    string
		wantOK     bool
	}{
		{
			name:       "valid S3 URI with bucket and key",
			value:      "s3://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantKey:    "path/to/object",
			wantOK:     true,
		},
		{
			name:       "valid S3 URI with bucket only",
			value:      "s3://my-bucket",
			wantBucket: "my-bucket",
			wantKey:    "",
			wantOK:     true,
		},
		{
			name:       "valid S3 URI with bucket and trailing slash",
			value:      "s3://my-bucket/",
			wantBucket: "my-bucket",
			wantKey:    "",
			wantOK:     true,
		},
		{
			name:       "valid S3 URI with leading slashes in key",
			value:      "s3://my-bucket///path/to/object",
			wantBucket: "my-bucket",
			wantKey:    "path/to/object",
			wantOK:     true,
		},
		{
			name:       "invalid - not S3 URI",
			value:      "http://example.com/bucket/key",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "invalid - no bucket",
			value:      "s3://",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "invalid - only slashes after s3://",
			value:      "s3:///",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "invalid - empty string",
			value:      "",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "valid - complex path",
			value:      "s3://models/llama-2-7b/fp16/model.safetensors",
			wantBucket: "models",
			wantKey:    "llama-2-7b/fp16/model.safetensors",
			wantOK:     true,
		},
		{
			name:       "valid - bucket with dashes",
			value:      "s3://my-model-bucket-01/weights",
			wantBucket: "my-model-bucket-01",
			wantKey:    "weights",
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotKey, gotOK := parseS3Path(tt.value)
			if gotBucket != tt.wantBucket {
				t.Errorf("parseS3Path(%q) bucket = %q, want %q", tt.value, gotBucket, tt.wantBucket)
			}
			if gotKey != tt.wantKey {
				t.Errorf("parseS3Path(%q) key = %q, want %q", tt.value, gotKey, tt.wantKey)
			}
			if gotOK != tt.wantOK {
				t.Errorf("parseS3Path(%q) ok = %v, want %v", tt.value, gotOK, tt.wantOK)
			}
		})
	}
}

// TestModelObjectKey tests the modelObjectKey function which extracts and normalizes object keys
func TestModelObjectKey(t *testing.T) {
	tests := []struct {
		name      string
		localPath string
		want      string
	}{
		{
			name:      "empty local path",
			localPath: "",
			want:      "",
		},
		{
			name:      "whitespace only",
			localPath: "   ",
			want:      "",
		},
		{
			name:      "S3 URI with bucket and key",
			localPath: "s3://my-bucket/models/llama",
			want:      "models/llama",
		},
		{
			name:      "S3 URI with leading slashes in key",
			localPath: "s3://my-bucket///models/llama",
			want:      "models/llama",
		},
		{
			name:      "S3 URI with bucket only",
			localPath: "s3://my-bucket",
			want:      "",
		},
		{
			name:      "regular path with leading slash",
			localPath: "/models/llama",
			want:      "models/llama",
		},
		{
			name:      "regular path without leading slash",
			localPath: "models/llama",
			want:      "models/llama",
		},
		{
			name:      "regular path with multiple leading slashes",
			localPath: "///models/llama",
			want:      "models/llama",
		},
		{
			name:      "path with spaces (trimmed)",
			localPath: "  models/llama  ",
			want:      "models/llama",
		},
		{
			name:      "invalid S3 URI (falls back to path parsing)",
			localPath: "http://example.com/models/llama",
			want:      "http://example.com/models/llama",
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
			got := modelObjectKey(model)
			if got != tt.want {
				t.Errorf("modelObjectKey() = %q, want %q (localPath=%q)", got, tt.want, tt.localPath)
			}
		})
	}
}
