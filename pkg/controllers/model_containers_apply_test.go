package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func envMap(env []corev1.EnvVar) map[string]string {
	result := make(map[string]string, len(env))
	for _, e := range env {
		if e.Value != "" {
			result[e.Name] = e.Value
		}
	}
	return result
}

func TestBuildDownloadContainerWithSecretRef(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForSource:  "models-src",
				BucketForConvert: "models-out",
				SecretRef:        &v1.SecretReference{Name: "minio-credentials"},
			},
			PipProxy: "https://pip.proxy",
		},
	}
	spec := &v1.ModelDownloadSpec{
		Image:       "ghcr.io/gorizond/downloader:latest",
		Memory:      "256Mi",
		ChunkMaxMiB: 32,
		Concurrency: 4,
		Command:     []string{"/bin/launcher"},
		Args:        []string{"--serve"},
	}

	container := handler.buildDownloadContainer(model, spec, "https://example.com/model", "models/example", "5")

	require.Equal(t, "model-downloader", container.Name)
	require.Equal(t, spec.Image, container.Image)
	require.NotEmpty(t, container.EnvFrom, "secret env should be mounted when secret namespace matches")

	env := envMap(container.Env)
	require.Equal(t, "example", env["MODEL_NAME"])
	require.Equal(t, "models-src", env["CACHE_BUCKET"])
	require.Equal(t, "models/example", env["CACHE_OBJECT_KEY"])
	require.Equal(t, "https://minio.local", env["CACHE_ENDPOINT"])
	require.Equal(t, "https://example.com/model", env["SOURCE_URL"])
	require.Equal(t, "https://pip.proxy", env["PIP_PROXY"])
	require.Equal(t, "256Mi", env["MEMORY_LIMIT"])
	require.Equal(t, "32", env["CHUNK_MAX_MIB"])
	require.Equal(t, "4", env["CONCURRENCY"])
	require.Contains(t, container.Args, "--serve")
	require.Equal(t, []string{"/bin/launcher"}, container.Command)
}

func TestBuildDownloadContainerSkipsCrossNamespaceSecret(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-src",
				SecretRef:       &v1.SecretReference{Name: "minio-credentials", Namespace: "other"},
			},
		},
	}
	spec := &v1.ModelDownloadSpec{Image: "downloader:latest"}

	container := handler.buildDownloadContainer(model, spec, "http://source", "obj", "1")
	require.Empty(t, container.EnvFrom, "secret from another namespace must not be mounted")
}

func TestDownloadArgsDefaults(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "models"}}
	spec := &v1.ModelDownloadSpec{}

	args := handler.downloadArgs(model, spec, "https://source", "object", "2")
	require.Len(t, args, 1)
	require.Contains(t, args[0], "pip install")
	require.Contains(t, args[0], "download.py")
}

func TestDownloadArgsCustom(t *testing.T) {
	handler := &modelHandler{}
	spec := &v1.ModelDownloadSpec{Args: []string{"--custom"}}
	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "models"}}

	args := handler.downloadArgs(model, spec, "https://source", "object", "3")
	require.Equal(t, []string{"--custom"}, args)
}
