package controllers

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
)

// model_containers.go
// Contains functions for building Kubernetes container specifications for Model Jobs.
// This includes download containers, conversion containers, and their command/args generation.

// buildDownloadContainer creates a container specification for downloading model files
// from various sources (HTTP, HuggingFace, etc.) and uploading them to object storage.
func (h *modelHandler) buildDownloadContainer(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey, generation string) corev1.Container {
	storage := model.Spec.ObjectStorage
	sourceBucket := ""
	cacheEndpoint := ""
	if storage != nil {
		sourceBucket = storage.BucketForSource
		cacheEndpoint = storage.Endpoint
	}

	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: sourceBucket},
		{Name: "CACHE_OBJECT_KEY", Value: objectKey},
		{Name: "MODEL_GENERATION", Value: generation},
	}
	if strings.TrimSpace(cacheEndpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: cacheEndpoint})
	}
	env = append(env, corev1.EnvVar{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"})

	// Determine memory limit from spec (fallback to 128Mi) and expose to container
	memoryQuantity := resource.MustParse("128Mi")
	if spec.Memory != "" {
		if q, err := resource.ParseQuantity(spec.Memory); err == nil {
			memoryQuantity = q
		} else {
			klog.Warningf("Model %s/%s: invalid download.memory '%s', falling back to 128Mi: %v", model.Namespace, model.Name, spec.Memory, err)
		}
	}
	env = append(env,
		corev1.EnvVar{Name: "MEMORY_LIMIT", Value: memoryQuantity.String()},
		corev1.EnvVar{Name: "MEMORY_LIMIT_BYTES", Value: strconv.FormatInt(memoryQuantity.Value(), 10)},
		corev1.EnvVar{Name: "CHUNK_MAX_MIB", Value: strconv.Itoa(int(spec.ChunkMaxMiB))},
		corev1.EnvVar{Name: "CONCURRENCY", Value: strconv.Itoa(int(spec.Concurrency))},
	)

	if spec.HuggingFaceTokenSecretRef != nil {
		if spec.HuggingFaceTokenSecretRef.Namespace != "" && spec.HuggingFaceTokenSecretRef.Namespace != model.Namespace {
			// cross-namespace secret mounts are not permitted; skip with warning via env comment
		} else {
			env = append(env, corev1.EnvVar{
				Name: "HF_TOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: spec.HuggingFaceTokenSecretRef.Name},
						Key:                  "token",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

	// Pass SOURCE_URL and optional PIP proxy to container
	env = append(env, corev1.EnvVar{Name: "SOURCE_URL", Value: sourceURL})
	// Pass PIP_PROXY if set in spec
	if strings.TrimSpace(model.Spec.PipProxy) != "" {
		env = append(env, corev1.EnvVar{Name: "PIP_PROXY", Value: model.Spec.PipProxy})
	}

	if storage != nil && storage.SecretRef != nil {
		if storage.SecretRef.Namespace == "" || storage.SecretRef.Namespace == model.Namespace {
			envFrom := corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: storage.SecretRef.Name},
					Optional:             pointer.Bool(true),
				},
			}
			return corev1.Container{
				Name:    "model-downloader",
				Image:   spec.Image,
				Command: h.downloadCommand(spec),
				Args:    h.downloadArgs(model, spec, sourceURL, objectKey, generation),
				Env:     env,
				EnvFrom: []corev1.EnvFromSource{envFrom},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: memoryQuantity,
					},
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: memoryQuantity,
					},
				},
				ImagePullPolicy: corev1.PullIfNotPresent,
			}
		}
	}

	return corev1.Container{
		Name:    "model-downloader",
		Image:   spec.Image,
		Command: h.downloadCommand(spec),
		Args:    h.downloadArgs(model, spec, sourceURL, objectKey, generation),
		Env:     env,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
		},
		ImagePullPolicy: corev1.PullIfNotPresent,
	}
}

// downloadCommand returns the command to execute in the download container.
// If a custom command is specified in the spec, it is used; otherwise, a shell is used.
func (h *modelHandler) downloadCommand(spec *v1.ModelDownloadSpec) []string {
	if len(spec.Command) > 0 {
		return spec.Command
	}
	// run a shell to install deps and execute a small Python + boto3 upload workflow
	return []string{"/bin/sh", "-c"}
}

// downloadArgs returns the arguments for the download container.
// If custom args are specified in the spec, they are used; otherwise, a script
// is generated that installs dependencies and executes the download.py script.
func (h *modelHandler) downloadArgs(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey, generation string) []string {
	if len(spec.Args) > 0 {
		return spec.Args
	}

	// Construct a script that:
	// 1) installs huggingface_hub and boto3,
	// 3) snapshots HF repo to /tmp/model, 4) uploads files directly to S3 via boto3
	_ = generation

	lines := []string{
		"set -euo pipefail",
		// Create pip.conf if PIP_PROXY set
		"if [ -n \"${PIP_PROXY:-}\" ]; then mkdir -p ~/.pip; cat > ~/.pip/pip.conf <<CONF\n[global]\nproxy = ${PIP_PROXY}\nindex-url = https://pypi.org/simple/\n\n[install]\ndefault-timeout = 500\ntrusted-host = pypi.python.org\n               pypi.org\n               files.pythonhosted.org\nCONF\nfi",
		"python -m pip install --no-cache-dir huggingface_hub boto3 botocore requests",
		"python -u /opt/script/download.py",
	}
	return []string{strings.Join(lines, "\n")}
}

// buildConversionContainer creates a container specification for converting model files
// from one format to another (e.g., HuggingFace to GGUF) and uploading the result to object storage.
func (h *modelHandler) buildConversionContainer(model *v1.Model, spec *v1.ModelConversionSpec, workDir, inputKey, outputBucket, outputKey, outputURI, weightsType, generation string) corev1.Container {
	storage := model.Spec.ObjectStorage
	sourceBucket := ""
	cacheEndpoint := ""
	if storage != nil {
		sourceBucket = storage.BucketForSource
		cacheEndpoint = storage.Endpoint
	}
	quantType := strings.TrimSpace(spec.ConvertWeights)
	if quantType == "" {
		quantType = strings.TrimSpace(spec.WeightsFloatType)
	}
	if quantType == "" {
		quantType = defaultWeightsType
	}

	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: sourceBucket},
		{Name: "CACHE_OBJECT_KEY", Value: inputKey},
		{Name: "CONVERSION_BUCKET", Value: outputBucket},
		{Name: "CONVERSION_OBJECT_KEY", Value: outputKey},
		{Name: "CONVERSION_OUTPUT_URI", Value: outputURI},
		{Name: "CONVERSION_WEIGHTS_TYPE", Value: quantType},
		{Name: "MODEL_GENERATION", Value: generation},
		{Name: "CONVERSION_WORK_DIR", Value: workDir},
		{Name: "PYTHONPATH", Value: "/workspace/converter"},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
	}
	if strings.TrimSpace(cacheEndpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: cacheEndpoint})
	}
	if strings.TrimSpace(model.Spec.PipProxy) != "" {
		env = append(env, corev1.EnvVar{Name: "PIP_PROXY", Value: model.Spec.PipProxy})
	}

	memoryQuantity := resource.MustParse("2Gi")
	if spec.Memory != "" {
		if q, err := resource.ParseQuantity(spec.Memory); err == nil {
			memoryQuantity = q
		} else {
			klog.Warningf("Model %s/%s: invalid conversion.memory '%s', falling back to 2Gi: %v", model.Namespace, model.Name, spec.Memory, err)
		}
	}

	image := spec.Image
	if image == "" {
		image = defaultConversionImage
	}

	container := corev1.Container{
		Name:            "model-converter",
		Image:           image,
		Command:         spec.Command,
		Args:            spec.Args,
		WorkingDir:      "/mnt/s3-output",
		Env:             env,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
	}

	if len(container.Command) == 0 {
		container.Command = []string{"/bin/sh", "-c"}
	}
	if len(container.Args) == 0 {
		container.Args = h.conversionArgs(model, spec, model.Spec.SourceURL, inputKey, outputKey)
	}

	if storage != nil && storage.SecretRef != nil {
		if storage.SecretRef.Namespace == "" || storage.SecretRef.Namespace == model.Namespace {
			container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: storage.SecretRef.Name},
					Optional:             pointer.Bool(true),
				},
			})
		}
	}

	return container
}

// conversionArgs returns the arguments for the conversion container.
// If custom args are specified in the spec, they are used; otherwise, a script
// is generated that installs dependencies and executes the conversion scripts.
func (h *modelHandler) conversionArgs(model *v1.Model, spec *v1.ModelConversionSpec, sourceURL, inputKey, outputKey string) []string {
	if len(spec.Args) > 0 {
		return spec.Args
	}

	_ = sourceURL
	_ = inputKey
	_ = outputKey

	cmdLines := []string{
		"set -euo pipefail",
		// create pip.conf if proxy provided (PIP_PROXY comes from spec.pipProxy via envFrom later if needed)
		"if [ -n \"${PIP_PROXY:-}\" ]; then mkdir -p ~/.pip; cat > ~/.pip/pip.conf <<CONF\n[global]\nproxy = ${PIP_PROXY}\nindex-url = https://pypi.org/simple/\n\n[install]\ndefault-timeout = 500\ntrusted-host = pypi.python.org\n               pypi.org\n               files.pythonhosted.org\nCONF\nfi",
	}

	if len(spec.Dependencies) > 0 {
		if depScript := buildDependencyInstallScript(spec.Dependencies); depScript != "" {
			cmdLines = append(cmdLines, depScript)
		}
	} else {
		cmdLines = append(cmdLines, "pip install --no-cache-dir torch safetensors sentencepiece transformers datasets huggingface_hub boto3 requests gitpython")
	}
	cmdLines = append(cmdLines,
		"python -u /workspace/converter/convert-hf.py /mnt/s3 ${CONVERSION_WEIGHTS_TYPE} ${MODEL_NAME}",
		"python -u /workspace/converter/convert-tokenizer-hf.py /mnt/s3 ${MODEL_NAME}",
	)

	return []string{strings.Join(cmdLines, "\n")}
}

// buildDependencyInstallScript generates a shell script that installs Python packages
// with specific versions. The packages are sorted for deterministic output.
func buildDependencyInstallScript(deps map[string]string) string {
	if len(deps) == 0 {
		return ""
	}
	packages := make([]string, 0, len(deps))
	for name, version := range deps {
		cleanName := strings.TrimSpace(name)
		if cleanName == "" {
			continue
		}
		cleanVersion := strings.TrimSpace(version)
		if cleanVersion != "" {
			packages = append(packages, fmt.Sprintf("%s==%s", cleanName, cleanVersion))
		} else {
			packages = append(packages, cleanName)
		}
	}
	if len(packages) == 0 {
		return ""
	}
	sort.Strings(packages)
	var b strings.Builder
	b.WriteString("for dep in")
	for _, pkg := range packages {
		b.WriteString(" '")
		b.WriteString(pkg)
		b.WriteString("'")
	}
	b.WriteString("; do \\")
	b.WriteString("\n  pip install --no-cache-dir \"$dep\"; \\")
	b.WriteString("\n done")
	return b.String()
}
