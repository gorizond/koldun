package controllers

import (
	"strings"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

func TestRootStatefulSetName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "demo-root", rootStatefulSetName("demo-root"))

	base := strings.Repeat("b", 60)
	trimmed := rootStatefulSetName(base)
	require.True(t, strings.HasSuffix(trimmed, "-root"))
	require.LessOrEqual(t, len(trimmed), validation.LabelValueMaxLength-statefulSetRevisionSuffixLength)

	name := strings.Repeat("a", 45) + "-segment-root"
	result := rootStatefulSetName(name)
	require.True(t, strings.HasSuffix(result, "-segment-root"))
	require.LessOrEqual(t, len(result), validation.LabelValueMaxLength-statefulSetRevisionSuffixLength)
}

func TestBuildLLMNATSEnv(t *testing.T) {
	t.Parallel()

	require.Nil(t, buildLLMNATSEnv(nil))

	vars := buildLLMNATSEnv(&v1.RootNATSConfig{URL: "nats://demo:4222"})
	require.Len(t, vars, 1)
	require.Equal(t, "NATS_URL", vars[0].Name)
	require.Equal(t, "nats://demo:4222", vars[0].Value)

	vars = buildLLMNATSEnv(&v1.RootNATSConfig{
		URL:               "nats://secure",
		CredentialsSecret: &v1.SecretReference{Name: "nats-creds"},
	})
	require.Len(t, vars, 2)
	require.Equal(t, "NATS_URL", vars[0].Name)
	require.Equal(t, "NATS_CREDS", vars[1].Name)
	require.Equal(t, "nats-creds", vars[1].ValueFrom.SecretKeyRef.Name)
	require.NotNil(t, vars[1].ValueFrom.SecretKeyRef.Optional)
	require.True(t, *vars[1].ValueFrom.SecretKeyRef.Optional)
}

func TestRootContainerBuildsExpectedValues(t *testing.T) {
	t.Parallel()

	h := &rootHandler{}
	root := &v1.Root{
		Spec: v1.RootSpec{
			Image: "ghcr.io/koldun/root:latest",
			Args:  []string{"--custom-flag"},
			CacheSpec: &v1.CacheSpec{
				Endpoint:  "https://cache",
				Bucket:    "models",
				SecretRef: &v1.SecretReference{Name: "cache-secret"},
			},
			NATS: &v1.RootNATSConfig{
				URL:               "nats://demo",
				CredentialsSecret: &v1.SecretReference{Name: "nats-creds"},
			},
		},
	}

	resources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}

	workers := []string{"worker-a", "worker-b"}
	container := h.rootContainer(root, "/models/model.gguf", "/models/tokenizer.json", "q4_0", 6, workers, resources)

	require.Equal(t, "root", container.Name)
	require.Equal(t, []string{"dllama-api"}, container.Command)
	require.Equal(t, root.Spec.Image, container.Image)
	require.Equal(t, resources, container.Resources)
	require.Contains(t, container.Args, "--workers")
	require.Subset(t, container.Args, []string{"--custom-flag"})
	require.Contains(t, container.Args, "worker-a")
	require.Contains(t, container.Args, "worker-b")

	envMap := map[string]corev1.EnvVar{}
	for _, env := range container.Env {
		envMap[env.Name] = env
	}
	require.Equal(t, "root", envMap["DLLAMA_ROLE"].Value)
	require.Equal(t, "https://cache", envMap["CACHE_ENDPOINT"].Value)
	require.Equal(t, "models", envMap["CACHE_BUCKET"].Value)
	require.Equal(t, "cache-secret", envMap["CACHE_SECRET"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "nats://demo", envMap["NATS_URL"].Value)
	require.Equal(t, "nats-creds", envMap["NATS_CREDS"].ValueFrom.SecretKeyRef.Name)

	require.Len(t, container.VolumeMounts, 1)
	require.Equal(t, "/model", container.VolumeMounts[0].MountPath)
}

func TestLLMSidecarContainer(t *testing.T) {
	t.Parallel()

	h := &rootHandler{}
	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sessions",
			Name:      "demo-root",
			Labels: map[string]string{
				labelConversationHash: "hash-123",
				labelDllamaName:       "demo",
			},
			Annotations: map[string]string{
				annotationSessionQueuePrefix: "sessions.demo",
			},
		},
		Spec: v1.RootSpec{
			Image: "ghcr.io/koldun/root:latest",
			NATS: &v1.RootNATSConfig{
				URL:               "nats://demo",
				CredentialsSecret: &v1.SecretReference{Name: "nats-creds"},
			},
		},
	}

	container := h.llmSidecarContainer(root)
	require.Equal(t, "llm", container.Name)
	require.Equal(t, []string{"/koldun"}, container.Command)
	require.Equal(t, root.Spec.Image, container.Image)
	require.Contains(t, container.Args, "--llm-hash")
	require.Contains(t, container.Args, "hash-123")
	require.Contains(t, container.Args, "--llm-nats-url")
	require.Contains(t, container.Args, "nats://demo")
	require.Contains(t, container.Args, "--llm-in-prefix")
	require.Contains(t, container.Args, "sessions.demo.")
	require.Contains(t, container.Args, "--llm-request-subject")
	require.Contains(t, container.Args, "sessions.demo.demo.in")
	require.Contains(t, container.Args, "--llm-state-subject")
	require.Contains(t, container.Args, "sessions.demo.demo.state")
	require.Contains(t, container.Args, "--llm-dllama-name")
	require.Contains(t, container.Args, "demo")

	envMap := map[string]corev1.EnvVar{}
	for _, env := range container.Env {
		envMap[env.Name] = env
	}
	require.Equal(t, "hash-123", envMap["HASH_KOLDUN"].Value)
	require.Equal(t, "nats://demo", envMap["NATS_URL"].Value)
	require.Equal(t, "nats-creds", envMap["NATS_CREDS"].ValueFrom.SecretKeyRef.Name)
}
