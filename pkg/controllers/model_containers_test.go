package controllers

import (
	"strings"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestBuildDependencyInstallScript tests the buildDependencyInstallScript function
// which generates shell scripts for installing Python dependencies with specific versions.
func TestBuildDependencyInstallScript(t *testing.T) {
	tests := []struct {
		name string
		deps map[string]string
		want string
	}{
		{
			name: "empty dependencies",
			deps: map[string]string{},
			want: "",
		},
		{
			name: "nil dependencies",
			deps: nil,
			want: "",
		},
		{
			name: "single package with version",
			deps: map[string]string{
				"torch": "2.0.0",
			},
			want: "for dep in 'torch==2.0.0'; do \\\n  pip install --no-cache-dir \"$dep\"; \\\n done",
		},
		{
			name: "single package without version",
			deps: map[string]string{
				"boto3": "",
			},
			want: "for dep in 'boto3'; do \\\n  pip install --no-cache-dir \"$dep\"; \\\n done",
		},
		{
			name: "multiple packages sorted alphabetically",
			deps: map[string]string{
				"transformers": "4.30.0",
				"boto3":        "1.28.0",
				"torch":        "2.0.0",
			},
			want: "for dep in 'boto3==1.28.0' 'torch==2.0.0' 'transformers==4.30.0'; do \\\n  pip install --no-cache-dir \"$dep\"; \\\n done",
		},
		{
			name: "packages with and without versions",
			deps: map[string]string{
				"torch":  "2.0.0",
				"boto3":  "",
				"numpy":  "1.24.0",
				"pandas": "",
			},
			want: "for dep in 'boto3' 'numpy==1.24.0' 'pandas' 'torch==2.0.0'; do \\\n  pip install --no-cache-dir \"$dep\"; \\\n done",
		},
		{
			name: "packages with whitespace in names and versions",
			deps: map[string]string{
				" torch ":        " 2.0.0 ",
				"  boto3":        "1.28.0  ",
				"transformers  ": "",
			},
			want: "for dep in 'boto3==1.28.0' 'torch==2.0.0' 'transformers'; do \\\n  pip install --no-cache-dir \"$dep\"; \\\n done",
		},
		{
			name: "empty package names should be skipped",
			deps: map[string]string{
				"":      "1.0.0",
				"  ":    "2.0.0",
				"torch": "2.0.0",
			},
			want: "for dep in 'torch==2.0.0'; do \\\n  pip install --no-cache-dir \"$dep\"; \\\n done",
		},
		{
			name: "all empty package names returns empty",
			deps: map[string]string{
				"":   "1.0.0",
				" ":  "2.0.0",
				"  ": "",
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDependencyInstallScript(tt.deps)
			if got != tt.want {
				t.Errorf("buildDependencyInstallScript() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestModelHandler_DownloadCommand tests the downloadCommand method
// which returns the command to execute for download containers.
func TestModelHandler_DownloadCommand(t *testing.T) {
	h := &modelHandler{}

	tests := []struct {
		name string
		spec *v1.ModelDownloadSpec
		want []string
	}{
		{
			name: "default command when spec.Command is empty",
			spec: &v1.ModelDownloadSpec{},
			want: []string{"/bin/sh", "-c"},
		},
		{
			name: "custom command from spec",
			spec: &v1.ModelDownloadSpec{
				Command: []string{"/bin/bash", "-x"},
			},
			want: []string{"/bin/bash", "-x"},
		},
		{
			name: "single custom command",
			spec: &v1.ModelDownloadSpec{
				Command: []string{"python"},
			},
			want: []string{"python"},
		},
		{
			name: "complex custom command",
			spec: &v1.ModelDownloadSpec{
				Command: []string{"/usr/bin/env", "python3", "-u"},
			},
			want: []string{"/usr/bin/env", "python3", "-u"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.downloadCommand(tt.spec)
			if len(got) != len(tt.want) {
				t.Errorf("downloadCommand() returned %d elements, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("downloadCommand()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestModelHandler_DownloadArgs tests the downloadArgs method
// which returns the arguments for download containers.
func TestModelHandler_DownloadArgs(t *testing.T) {
	h := &modelHandler{}

	tests := []struct {
		name         string
		model        *v1.Model
		spec         *v1.ModelDownloadSpec
		sourceURL    string
		objectKey    string
		generation   string
		wantCustom   bool     // true if spec.Args is used
		checkContent []string // substrings to check in generated script
	}{
		{
			name:  "custom args from spec",
			model: &v1.Model{},
			spec: &v1.ModelDownloadSpec{
				Args: []string{"echo", "custom"},
			},
			wantCustom: true,
		},
		{
			name:  "default script generation",
			model: &v1.Model{},
			spec:  &v1.ModelDownloadSpec{},
			checkContent: []string{
				"set -euo pipefail",
				"python -m pip install",
				"huggingface_hub",
				"boto3",
				"python -u /opt/script/download.py",
			},
		},
		{
			name:  "script with PIP_PROXY check",
			model: &v1.Model{},
			spec:  &v1.ModelDownloadSpec{},
			checkContent: []string{
				"if [ -n \"${PIP_PROXY:-}\" ]",
				"mkdir -p ~/.pip",
				"cat > ~/.pip/pip.conf",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.downloadArgs(tt.model, tt.spec, tt.sourceURL, tt.objectKey, tt.generation)

			if tt.wantCustom {
				// Should return spec.Args directly
				if len(got) != len(tt.spec.Args) {
					t.Errorf("downloadArgs() returned %d elements, want %d", len(got), len(tt.spec.Args))
					return
				}
				for i := range got {
					if got[i] != tt.spec.Args[i] {
						t.Errorf("downloadArgs()[%d] = %q, want %q", i, got[i], tt.spec.Args[i])
					}
				}
				return
			}

			// For generated scripts, check content
			if len(got) != 1 {
				t.Errorf("downloadArgs() should return single script element, got %d", len(got))
				return
			}

			script := got[0]
			for _, substring := range tt.checkContent {
				if !strings.Contains(script, substring) {
					t.Errorf("downloadArgs() script missing expected content: %q\nGot: %s", substring, script)
				}
			}
		})
	}
}

// TestModelHandler_ConversionArgs tests the conversionArgs method
// which returns the arguments for conversion containers.
func TestModelHandler_ConversionArgs(t *testing.T) {
	h := &modelHandler{}

	tests := []struct {
		name         string
		model        *v1.Model
		spec         *v1.ModelConversionSpec
		sourceURL    string
		inputKey     string
		outputKey    string
		wantCustom   bool
		checkContent []string
	}{
		{
			name:  "custom args from spec",
			model: &v1.Model{},
			spec: &v1.ModelConversionSpec{
				Args: []string{"custom", "conversion"},
			},
			wantCustom: true,
		},
		{
			name:  "default script with no custom dependencies",
			model: &v1.Model{},
			spec:  &v1.ModelConversionSpec{},
			checkContent: []string{
				"set -euo pipefail",
				"pip install --no-cache-dir torch safetensors sentencepiece transformers datasets huggingface_hub boto3 requests gitpython",
				"python -u /workspace/converter/convert-hf.py",
				"python -u /workspace/converter/convert-tokenizer-hf.py",
			},
		},
		{
			name:  "script with custom dependencies",
			model: &v1.Model{},
			spec: &v1.ModelConversionSpec{
				Dependencies: map[string]string{
					"torch": "2.0.0",
					"boto3": "1.28.0",
				},
			},
			checkContent: []string{
				"set -euo pipefail",
				"for dep in 'boto3==1.28.0' 'torch==2.0.0'",
				"pip install --no-cache-dir",
				"python -u /workspace/converter/convert-hf.py",
			},
		},
		{
			name:  "script with PIP_PROXY check",
			model: &v1.Model{},
			spec:  &v1.ModelConversionSpec{},
			checkContent: []string{
				"if [ -n \"${PIP_PROXY:-}\" ]",
				"mkdir -p ~/.pip",
				"cat > ~/.pip/pip.conf",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.conversionArgs(tt.model, tt.spec, tt.sourceURL, tt.inputKey, tt.outputKey)

			if tt.wantCustom {
				if len(got) != len(tt.spec.Args) {
					t.Errorf("conversionArgs() returned %d elements, want %d", len(got), len(tt.spec.Args))
					return
				}
				for i := range got {
					if got[i] != tt.spec.Args[i] {
						t.Errorf("conversionArgs()[%d] = %q, want %q", i, got[i], tt.spec.Args[i])
					}
				}
				return
			}

			if len(got) != 1 {
				t.Errorf("conversionArgs() should return single script element, got %d", len(got))
				return
			}

			script := got[0]
			for _, substring := range tt.checkContent {
				if !strings.Contains(script, substring) {
					t.Errorf("conversionArgs() script missing expected content: %q\nGot: %s", substring, script)
				}
			}
		})
	}
}

func TestModelHandlerBuildDownloadContainerWithSecrets(t *testing.T) {
	t.Parallel()

	h := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			PipProxy: "http://proxy.internal:8080",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:        "https://s3.internal",
				BucketForSource: "models-src",
				SecretRef:       &v1.SecretReference{Name: "storage-creds"},
			},
		},
	}
	spec := &v1.ModelDownloadSpec{
		Image:                     "downloader:1.0",
		Memory:                    "256Mi",
		ChunkMaxMiB:               32,
		Concurrency:               4,
		HuggingFaceTokenSecretRef: &v1.SecretReference{Name: "hf-token"},
	}

	container := h.buildDownloadContainer(model, spec, "https://huggingface.co/gpt", "models/llama", "5")
	require.Equal(t, "model-downloader", container.Name)
	require.Equal(t, spec.Image, container.Image)
	require.Len(t, container.EnvFrom, 1)
	require.Equal(t, "storage-creds", container.EnvFrom[0].SecretRef.Name)

	env := toEnvMap(container.Env)
	require.Equal(t, "models-src", env["CACHE_BUCKET"].Value)
	require.Equal(t, "https://s3.internal", env["CACHE_ENDPOINT"].Value)
	require.Equal(t, "models/llama", env["CACHE_OBJECT_KEY"].Value)
	require.Equal(t, "5", env["MODEL_GENERATION"].Value)
	require.Equal(t, "256Mi", env["MEMORY_LIMIT"].Value)
	require.Equal(t, "268435456", env["MEMORY_LIMIT_BYTES"].Value)
	require.Equal(t, "32", env["CHUNK_MAX_MIB"].Value)
	require.Equal(t, "4", env["CONCURRENCY"].Value)
	require.Equal(t, "http://proxy.internal:8080", env["PIP_PROXY"].Value)
	require.Equal(t, "hf-token", env["HF_TOKEN"].ValueFrom.SecretKeyRef.Name)

	limit := container.Resources.Limits[corev1.ResourceMemory]
	require.True(t, limit.Equal(resource.MustParse("256Mi")))
	request := container.Resources.Requests[corev1.ResourceMemory]
	require.True(t, request.Equal(resource.MustParse("256Mi")))
}

func TestModelHandlerBuildDownloadContainerSkipsCrossNamespaceSecrets(t *testing.T) {
	t.Parallel()

	h := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:        "https://minio.internal",
				BucketForSource: "models-src",
				SecretRef:       &v1.SecretReference{Name: "storage-creds", Namespace: "other"},
			},
		},
	}
	spec := &v1.ModelDownloadSpec{
		Image:                     "downloader:latest",
		Memory:                    "not-a-quantity",
		HuggingFaceTokenSecretRef: &v1.SecretReference{Name: "hf-token", Namespace: "other"},
	}

	container := h.buildDownloadContainer(model, spec, "https://hf.co/private", "models/llama", "7")
	env := toEnvMap(container.Env)
	require.NotContains(t, env, "HF_TOKEN", "cross-namespace HF secret must be ignored")
	require.Equal(t, "https://minio.internal", env["CACHE_ENDPOINT"].Value)
	require.Equal(t, "models-src", env["CACHE_BUCKET"].Value)
	require.Empty(t, container.EnvFrom, "storage secret from another namespace should be skipped")

	limit := container.Resources.Limits[corev1.ResourceMemory]
	require.True(t, limit.Equal(resource.MustParse("128Mi")), "invalid memory should fall back to default")
	request := container.Resources.Requests[corev1.ResourceMemory]
	require.True(t, request.Equal(resource.MustParse("128Mi")))
}

func TestModelHandlerBuildConversionContainerDefaults(t *testing.T) {
	t.Parallel()

	h := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mixtral", Namespace: "models"},
		Spec: v1.ModelSpec{
			PipProxy: "http://proxy.internal",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://s3.internal",
				BucketForSource:  "models-src",
				BucketForConvert: "models-conv",
				SecretRef:        &v1.SecretReference{Name: "storage-creds"},
			},
		},
	}
	spec := &v1.ModelConversionSpec{
		WeightsFloatType: "fp16",
		ConvertWeights:   "q4_0",
		Memory:           "4Gi",
	}

	container := h.buildConversionContainer(model, spec, "/workspace/hf", "input/key", "bucket-out", "key-out", "s3://bucket/key", "fp16", "9")
	require.Equal(t, "model-converter", container.Name)
	require.Equal(t, defaultConversionImage, container.Image)
	require.Equal(t, []string{"/bin/sh", "-c"}, container.Command)
	require.Len(t, container.Args, 1)
	require.Contains(t, container.Args[0], "/workspace/converter/convert-hf.py")
	require.Len(t, container.EnvFrom, 1)
	require.Equal(t, "storage-creds", container.EnvFrom[0].SecretRef.Name)

	env := toEnvMap(container.Env)
	require.Equal(t, "models-src", env["CACHE_BUCKET"].Value)
	require.Equal(t, "input/key", env["CACHE_OBJECT_KEY"].Value)
	require.Equal(t, "bucket-out", env["CONVERSION_BUCKET"].Value)
	require.Equal(t, "key-out", env["CONVERSION_OBJECT_KEY"].Value)
	require.Equal(t, "s3://bucket/key", env["CONVERSION_OUTPUT_URI"].Value)
	require.Equal(t, "q4_0", env["CONVERSION_WEIGHTS_TYPE"].Value)
	require.Equal(t, "http://proxy.internal", env["PIP_PROXY"].Value)

	limit := container.Resources.Limits[corev1.ResourceMemory]
	require.True(t, limit.Equal(resource.MustParse("4Gi")))
}

func TestModelHandlerBuildConversionContainerCustomCommand(t *testing.T) {
	t.Parallel()

	h := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mixtral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://s3.internal",
				BucketForSource:  "models-src",
				BucketForConvert: "models-conv",
				SecretRef:        &v1.SecretReference{Name: "storage-creds", Namespace: "other"},
			},
		},
	}
	spec := &v1.ModelConversionSpec{
		Image:   "converter:1.2",
		Command: []string{"python"},
		Args:    []string{"custom", "script.py"},
		Memory:  "not-a-number",
	}

	container := h.buildConversionContainer(model, spec, "/workspace/custom", "input/key", "bucket", "key", "s3://bucket/key", "", "2")
	require.Equal(t, "converter:1.2", container.Image)
	require.Equal(t, spec.Command, container.Command)
	require.Equal(t, spec.Args, container.Args)
	require.Empty(t, container.EnvFrom, "cross-namespace storage secret should not be mounted")

	env := toEnvMap(container.Env)
	require.Equal(t, defaultWeightsType, env["CONVERSION_WEIGHTS_TYPE"].Value, "missing weights should fall back to default")

	limit := container.Resources.Limits[corev1.ResourceMemory]
	require.True(t, limit.Equal(resource.MustParse("2Gi")), "invalid memory should fall back to default")
}

func toEnvMap(env []corev1.EnvVar) map[string]corev1.EnvVar {
	result := make(map[string]corev1.EnvVar, len(env))
	for _, e := range env {
		result[e.Name] = e
	}
	return result
}
