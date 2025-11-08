package controllers

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/minio/minio-go/v7"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
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
			name:      "local absolute path",
			localPath: "/models/llama",
			wantKey:   "models/llama",
		},
		{
			name:      "local relative path",
			localPath: "models/llama",
			wantKey:   "models/llama",
		},
		{
			name:      "empty path",
			localPath: " ",
			wantKey:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &v1.Model{
				Spec: v1.ModelSpec{LocalPath: tt.localPath},
			}
			if got := modelObjectKey(model); got != tt.wantKey {
				t.Errorf("modelObjectKey() = %v, want %v", got, tt.wantKey)
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
		wantURISubstring string
	}{
		{
			name:      "default settings",
			modelName: "llama-7b",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "models-source",
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
			name:      "custom s3 output path with nonstandard float type",
			modelName: "falcon-40b",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "default-bucket",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "bf16-mixed",
				OutputPath:       "s3://alt-bucket/exports/falcon/",
			},
			defaultInputKey:  "downloads/falcon",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "alt-bucket",
			wantKey:          "exports/falcon/falcon-40b",
			wantURISubstring: "s3://alt-bucket/exports/falcon/falcon-40b",
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
		{
			name:      "leading slash in model name is sanitized",
			modelName: "/bad-model",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "converted-bucket",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "f16",
				OutputPath:       "s3://converted-bucket/edge",
			},
			defaultInputKey:  "downloads",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "converted-bucket",
			wantKey:          "edge/bad-model",
			wantURISubstring: "s3://converted-bucket/edge/bad-model",
		},
		{
			name:      "default output with exotic float type",
			modelName: "phoenix",
			objectStorage: &v1.ModelObjectStorageSpec{
				BucketForConvert: "converted",
			},
			conversionSpec: &v1.ModelConversionSpec{
				WeightsFloatType: "fp8-e4m3",
			},
			defaultInputKey:  "models/phoenix",
			wantWorkDir:      "/workspace/hf",
			wantBucket:       "converted",
			wantKey:          "models/phoenix/converted/fp8-e4m3/phoenix",
			wantURISubstring: "s3://converted/models/phoenix/converted/fp8-e4m3/phoenix",
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

func TestModelObjectKeyHandlesBucketOnlyURI(t *testing.T) {

	model := &v1.Model{
		Spec: v1.ModelSpec{
			LocalPath: " \ts3://weights-cache \n",
		},
	}

	require.Equal(t, "", modelObjectKey(model), "bucket-only URIs should yield an empty key")
}

func TestConversionPathsWithoutObjectStorageDefaults(t *testing.T) {

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "tiny-llm"},
	}
	spec := &v1.ModelConversionSpec{
		WeightsFloatType: "q4",
		OutputPath:       "relative/output",
	}

	workDir, bucket, key, uri := conversionPaths(model, spec, "inputs/tiny")

	require.Equal(t, "/workspace/relative/output", workDir)
	require.Equal(t, "", bucket, "bucket should remain empty when object storage is not configured")
	require.Equal(t, "inputs/tiny/converted/q4/tiny-llm", key)
	require.Equal(t, "", uri, "uri should remain empty without a bucket")
}

func TestSplitKeyEdgeCases(t *testing.T) {

	tests := []struct {
		name  string
		key   string
		first string
		rest  string
	}{
		{
			name:  "leading slash is ignored",
			key:   "/namespace/resource",
			first: "",
			rest:  "namespace/resource",
		},
		{
			name:  "multiple segments return remainder intact",
			key:   "models/llama/weights",
			first: "models",
			rest:  "llama/weights",
		},
		{
			name:  "missing separator returns empty parts",
			key:   "singleton",
			first: "",
			rest:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, rest := splitKey(tt.key)
			require.Equal(t, tt.first, first)
			require.Equal(t, tt.rest, rest)
		})
	}
}

type fakeMinioClient struct {
	existing   map[string]bool
	existsErr  error
	makeErrors map[string]error
	made       []string
}

func (f *fakeMinioClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	if f.existsErr != nil {
		return false, f.existsErr
	}
	return f.existing[bucket], nil
}

func (f *fakeMinioClient) MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error {
	if f.makeErrors != nil {
		if err, ok := f.makeErrors[bucket]; ok {
			return err
		}
	}
	if f.existing == nil {
		f.existing = map[string]bool{}
	}
	f.existing[bucket] = true
	f.made = append(f.made, bucket)
	return nil
}

func TestEnsureObjectStorageBuckets_EarlyExit(t *testing.T) {

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
	}

	t.Run("ensure disabled", func(t *testing.T) {
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: false,
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(model))
	})

	t.Run("no storage", func(t *testing.T) {
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(model))
	})

	t.Run("missing endpoint", func(t *testing.T) {
		modelWithStorage := model.DeepCopy()
		modelWithStorage.Spec.ObjectStorage = &v1.ModelObjectStorageSpec{
			BucketForSource: "source",
		}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(modelWithStorage))
	})

	t.Run("missing secret ref", func(t *testing.T) {
		modelWithStorage := model.DeepCopy()
		modelWithStorage.Spec.SourceURL = "https://example.com/model"
		modelWithStorage.Spec.ObjectStorage = &v1.ModelObjectStorageSpec{
			Endpoint:        "https://minio.local",
			BucketForSource: "source",
		}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(modelWithStorage))
	})
}

func TestEnsureObjectStorageBuckets_SecretErrors(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	secrets := genericfake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	secretCache := genericfake.NewMockCacheInterface[*corev1.Secret](ctrl)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForSource:  "source",
				BucketForConvert: "convert",
				SecretRef:        &v1.SecretReference{Name: "storage"},
			},
		},
	}

	secrets.EXPECT().Cache().Return(secretCache).AnyTimes()

	t.Run("secret fetch error", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(nil, errors.New("boom"))
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
		}
		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "fetch object storage secret")
	})

	t.Run("missing credentials", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(&corev1.Secret{}, nil)
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
		}
		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "missing AWS credentials")
	})
}

func TestEnsureObjectStorageBuckets_SecretNamespaceFallbackAndInsecureEndpoint(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	secrets := genericfake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	secretCache := genericfake.NewMockCacheInterface[*corev1.Secret](ctrl)
	secrets.EXPECT().Cache().Return(secretCache).AnyTimes()

	secret := &corev1.Secret{
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte("access"),
			"AWS_SECRET_ACCESS_KEY": []byte("secret"),
		},
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:        "http://minio.local:9000",
				BucketForSource: "source",
				SecretRef:       &v1.SecretReference{Name: "storage"},
			},
		},
	}

	secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
	fake := &fakeMinioClient{}
	handler := &modelHandler{
		ctx:           context.Background(),
		ensureBuckets: true,
		secrets:       secrets,
		minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
			require.Equal(t, "minio.local:9000", host)
			require.False(t, opts.Secure, "http endpoint should disable TLS")
			return fake, nil
		},
	}

	require.NoError(t, handler.ensureObjectStorageBuckets(model))
	require.Equal(t, []string{"source"}, fake.made)
}

func TestEnsureObjectStorageBuckets_RegionPropagatedToClient(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	secrets := genericfake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	secretCache := genericfake.NewMockCacheInterface[*corev1.Secret](ctrl)
	secrets.EXPECT().Cache().Return(secretCache).AnyTimes()

	secret := &corev1.Secret{
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte("access"),
			"AWS_SECRET_ACCESS_KEY": []byte("secret"),
			"AWS_REGION":            []byte("us-east-2"),
		},
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForConvert: "convert",
				SecretRef:        &v1.SecretReference{Name: "storage", Namespace: "custom-ns"},
			},
		},
	}

	secretCache.EXPECT().Get("custom-ns", "storage").Return(secret, nil)
	fake := &fakeMinioClient{}
	handler := &modelHandler{
		ctx:           context.Background(),
		ensureBuckets: true,
		secrets:       secrets,
		minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
			require.Equal(t, "minio.local", host)
			require.True(t, opts.Secure, "https endpoint should enable TLS")
			require.Equal(t, "us-east-2", opts.Region)
			return fake, nil
		},
	}

	require.NoError(t, handler.ensureObjectStorageBuckets(model))
	require.Equal(t, []string{"convert"}, fake.made)
}

func TestEnsureObjectStorageBuckets_ClientInteractions(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	secrets := genericfake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	secretCache := genericfake.NewMockCacheInterface[*corev1.Secret](ctrl)
	secrets.EXPECT().Cache().Return(secretCache).AnyTimes()

	secret := &corev1.Secret{
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte("access"),
			"AWS_SECRET_ACCESS_KEY": []byte("secret"),
		},
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForSource:  "source",
				BucketForConvert: "source",
				SecretRef:        &v1.SecretReference{Name: "storage"},
			},
		},
	}

	t.Run("bucket exists", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
		fake := &fakeMinioClient{existing: map[string]bool{"source": true}}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
			minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
				require.Equal(t, "minio.local", host)
				require.True(t, opts.Secure)
				return fake, nil
			},
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(model))
		require.Empty(t, fake.made, "no buckets should be created when already present")
	})

	t.Run("create bucket", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
		fake := &fakeMinioClient{existing: map[string]bool{}}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
			minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
				return fake, nil
			},
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(model))
		require.Equal(t, []string{"source"}, fake.made)
	})

	t.Run("bucket already exists error", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
		fake := &fakeMinioClient{makeErrors: map[string]error{"source": minio.ErrorResponse{Code: "BucketAlreadyOwnedByYou"}}}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
			minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
				return fake, nil
			},
		}
		require.NoError(t, handler.ensureObjectStorageBuckets(model))
	})

	t.Run("bucket creation failure", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
		fake := &fakeMinioClient{makeErrors: map[string]error{"source": errors.New("nope")}}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
			minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
				return fake, nil
			},
		}
		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "create bucket")
	})

	t.Run("bucket exists check failure", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
		fake := &fakeMinioClient{existsErr: errors.New("dial failed")}
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
			minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
				return fake, nil
			},
		}
		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "check bucket source existence")
	})

	t.Run("factory creation failure", func(t *testing.T) {
		secretCache.EXPECT().Get("models", "storage").Return(secret, nil)
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
			minioFactory: func(host string, opts *minio.Options) (minioClient, error) {
				return nil, errors.New("connect refused")
			},
		}
		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "initialise object storage client")
	})
}

func TestEnsureObjectStorageBuckets_EndpointErrors(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	secrets := genericfake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	secretCache := genericfake.NewMockCacheInterface[*corev1.Secret](ctrl)
	secrets.EXPECT().Cache().Return(secretCache).AnyTimes()
	secret := &corev1.Secret{
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte("access"),
			"AWS_SECRET_ACCESS_KEY": []byte("secret"),
		},
	}
	secretCache.EXPECT().Get("models", "storage").Return(secret, nil).AnyTimes()

	base := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForSource:  "source",
				BucketForConvert: "convert",
				SecretRef:        &v1.SecretReference{Name: "storage"},
			},
		},
	}

	t.Run("invalid endpoint parse", func(t *testing.T) {
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
		}
		model := base.DeepCopy()
		model.Spec.ObjectStorage.Endpoint = "://bad-endpoint"

		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "parse object storage endpoint")
	})

	t.Run("endpoint missing host", func(t *testing.T) {
		handler := &modelHandler{
			ctx:           context.Background(),
			ensureBuckets: true,
			secrets:       secrets,
		}
		model := base.DeepCopy()
		model.Spec.ObjectStorage.Endpoint = "https://"

		err := handler.ensureObjectStorageBuckets(model)
		require.ErrorContains(t, err, "missing host")
	})
}

func TestDefaultMinioFactory(t *testing.T) {

	client, err := defaultMinioFactory("play.min.io", &minio.Options{Secure: true})
	require.NoError(t, err)
	require.NotNil(t, client)
}
