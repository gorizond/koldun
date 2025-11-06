package controllers

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

	longTail := fmt.Sprintf("edge-%s-root", strings.Repeat("q", 80))
	edgeResult := rootStatefulSetName(longTail)
	require.True(t, strings.HasSuffix(edgeResult, "-root"))
	require.LessOrEqual(t, len(edgeResult), validation.LabelValueMaxLength-statefulSetRevisionSuffixLength)
	require.NotContains(t, edgeResult, "--", "truncation should not introduce double dashes")
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

func TestRootHandlerWorkerStatus(t *testing.T) {
	t.Parallel()

	t.Run("nil root returns defaults", func(t *testing.T) {
		t.Parallel()

		h := &rootHandler{}
		ready, count, endpoints, err := h.workerStatus(nil)
		require.NoError(t, err)
		require.False(t, ready)
		require.Zero(t, count)
		require.Nil(t, endpoints)
	})

	t.Run("missing selector short circuits", func(t *testing.T) {
		t.Parallel()

		h := &rootHandler{}
		ready, count, endpoints, err := h.workerStatus(&v1.Root{})
		require.NoError(t, err)
		require.False(t, ready)
		require.Zero(t, count)
		require.Nil(t, endpoints)
	})

	t.Run("dllama not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		h := &rootHandler{dllamas: dllamas}

		dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		dllamas.EXPECT().Cache().Return(dllamaCache)
		dllamaCache.EXPECT().Get("models", "mistral").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "dllamas"}, "mistral"))

		root := &v1.Root{
			ObjectMeta: metav1.ObjectMeta{Namespace: "models", Labels: map[string]string{labelDllamaName: "mistral"}},
			Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		}

		ready, count, endpoints, err := h.workerStatus(root)
		require.NoError(t, err)
		require.False(t, ready)
		require.Zero(t, count)
		require.Nil(t, endpoints)
	})

	t.Run("dllama lookup error bubbles up", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		h := &rootHandler{dllamas: dllamas}

		dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		dllamas.EXPECT().Cache().Return(dllamaCache)
		expected := errors.New("dllama boom")
		dllamaCache.EXPECT().Get("models", "mistral").Return(nil, expected)

		root := &v1.Root{
			ObjectMeta: metav1.ObjectMeta{Namespace: "models", Labels: map[string]string{labelDllamaName: "mistral"}},
			Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		}

		_, _, _, err := h.workerStatus(root)
		require.ErrorIs(t, err, expected)
	})

	t.Run("worker missing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		h := &rootHandler{dllamas: dllamas, workers: workers}

		dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
		dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1}}
		dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil)

		workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
		workers.EXPECT().Cache().Return(workerCache)
		workerName := workerResourceName("mistral")
		workerCache.EXPECT().Get("models", workerName).Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "workers"}, workerName))

		root := &v1.Root{
			ObjectMeta: metav1.ObjectMeta{Namespace: "models", Labels: map[string]string{labelDllamaName: "mistral"}},
			Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		}

		ready, count, endpoints, err := h.workerStatus(root)
		require.NoError(t, err)
		require.False(t, ready)
		require.Zero(t, count)
		require.Nil(t, endpoints)
	})

	t.Run("statefulset missing returns endpoints", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
		h := &rootHandler{dllamas: dllamas, workers: workers, statefulsets: statefulsets}

		dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
		dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1}}
		dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil)

		workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
		workers.EXPECT().Cache().Return(workerCache).AnyTimes()
		workerName := workerResourceName("mistral")
		worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
		workerCache.EXPECT().Get("models", workerName).Return(worker, nil)

		stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
		statefulsets.EXPECT().Cache().Return(stsCache)
		stsCache.EXPECT().Get("models", workerName).Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, workerName))

		root := &v1.Root{
			ObjectMeta: metav1.ObjectMeta{Namespace: "models", Labels: map[string]string{labelDllamaName: "mistral"}},
			Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		}

		ready, count, endpoints, err := h.workerStatus(root)
		require.NoError(t, err)
		require.False(t, ready)
		require.Zero(t, count)
		require.Len(t, endpoints, 1)
		require.Contains(t, endpoints[0], workerName)
	})

	t.Run("statefulset lookup error bubbles up", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
		h := &rootHandler{dllamas: dllamas, workers: workers, statefulsets: statefulsets}

		dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
		dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1}}
		dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil)

		workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
		workers.EXPECT().Cache().Return(workerCache).AnyTimes()
		workerName := workerResourceName("mistral")
		worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
		workerCache.EXPECT().Get("models", workerName).Return(worker, nil)

		stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
		statefulsets.EXPECT().Cache().Return(stsCache)
		expected := errors.New("statefulset boom")
		stsCache.EXPECT().Get("models", workerName).Return(nil, expected)

		root := &v1.Root{
			ObjectMeta: metav1.ObjectMeta{Namespace: "models", Labels: map[string]string{labelDllamaName: "mistral"}},
			Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		}

		_, _, _, err := h.workerStatus(root)
		require.ErrorIs(t, err, expected)
	})
}

func TestRootHandlerEnsureServiceAppliesHeadlessService(t *testing.T) {
	t.Parallel()

	fakeApply := newFakeApply()
	h := &rootHandler{apply: fakeApply}
	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-root",
			Labels: map[string]string{
				labelDllamaName:       "mistral",
				labelConversationHash: "hash-1",
			},
		},
		Spec: v1.RootSpec{ModelRef: "mistral-model", Image: "ghcr.io/koldun/root"},
	}

	require.NoError(t, h.ensureService(root))
	require.Len(t, fakeApply.appliedObjects, 1)
	svc, ok := fakeApply.appliedObjects[0].(*corev1.Service)
	require.True(t, ok)
	require.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
	require.Equal(t, map[string]string{
		labelComponent:        componentRoot,
		labelRootName:         sanitizeLabelValue(root.Name),
		labelConversationHash: "hash-1",
		labelDllamaName:       "mistral",
	}, svc.ObjectMeta.Labels)
	require.Equal(t, sanitizeLabelValue(root.Name), svc.Spec.Selector[labelRootName])
	require.Equal(t, "mistral", svc.Spec.Selector[labelDllamaName])
}

func TestRootHandlerEnsureStatefulSetWaitsForWorkers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)

	h := &rootHandler{
		apply:   fakeApply,
		dllamas: dllamas,
		workers: workers,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1}}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil)

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache)
	workerName := workerResourceName("mistral")
	workerCache.EXPECT().Get("models", workerName).Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "workers"}, workerName))

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
	}

	require.NoError(t, h.ensureStatefulSet(root))
	require.Empty(t, fakeApply.appliedObjects, "statefulset should not be applied while workers converge")
}

func TestRootHandlerEnsureStatefulSetCreatesResources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.DllamaSpec{
			ReplicaPower: 1,
			ModelRef:     v1.ModelReference{Name: "mistral"},
		},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4_0"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 8 << 30, ConversionSizeHuman: "8Gi"},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil)

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-root",
			Labels: map[string]string{
				labelDllamaName:       "mistral",
				labelConversationHash: "hash-1",
			},
		},
		Spec: v1.RootSpec{
			ModelRef:       "mistral-model",
			Image:          "ghcr.io/gorizond/root:latest",
			Args:           []string{"--custom"},
			WorkerSelector: map[string]string{"app": "dllama"},
		},
	}

	require.NoError(t, h.ensureStatefulSet(root))
	require.Len(t, fakeApply.appliedObjects, 1)
	sts, ok := fakeApply.appliedObjects[0].(*appsv1.StatefulSet)
	require.True(t, ok)
	require.Equal(t, rootStatefulSetName(root.Name), sts.Name)
	require.Equal(t, root.Spec.ModelRef, sts.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
	require.Len(t, sts.Spec.Template.Spec.Containers, 2)
	rootContainer := sts.Spec.Template.Spec.Containers[0]
	require.Equal(t, "root", rootContainer.Name)
	require.Contains(t, rootContainer.Args, "--workers")
	require.Contains(t, rootContainer.Args, "mistral-workers-0.mistral-workers.models.svc.cluster.local:9999")
	require.Equal(t, "llm", sts.Spec.Template.Spec.Containers[1].Name)
	require.Equal(t, fmt.Sprintf("root-%s-statefulset", root.Name), fakeApply.setID)
}

func TestRootHandlerEnsureStatefulSetRequiresConversionWeights(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{ModelRef: "mistral-pvc", WorkerSelector: map[string]string{"app": "dllama"}},
	}

	err := h.ensureStatefulSet(root)
	require.EqualError(t, err, "model models/mistral conversion.weightsFloatType is required")
	require.Empty(t, fakeApply.appliedObjects)
}

func TestRootHandlerEnsureStatefulSetRequiresModelRef(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 1 << 30},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{ModelRef: "", WorkerSelector: map[string]string{"app": "dllama"}},
	}

	err := h.ensureStatefulSet(root)
	require.EqualError(t, err, "root models/mistral-root missing spec.modelRef")
	require.Empty(t, fakeApply.appliedObjects)
}

func TestRootHandlerEnsureStatefulSetHandlesZeroReplicaPower(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.DllamaSpec{
			ReplicaPower: 0,
			ModelRef:     v1.ModelReference{Name: "mistral"},
		},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4_k_m"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 12 << 30, ConversionSizeHuman: "12Gi"},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil)

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	maxRatio := 1.8
	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-root",
			Labels: map[string]string{
				labelDllamaName: "mistral",
			},
		},
		Spec: v1.RootSpec{
			ModelRef:       "mistral-model-pvc",
			WorkerSelector: map[string]string{"app": "dllama"},
			Memory:         &v1.RootMemorySpec{OverheadMaxRatio: &maxRatio},
		},
	}

	require.NoError(t, h.ensureStatefulSet(root))
	require.Len(t, fakeApply.appliedObjects, 1)

	sts, ok := fakeApply.appliedObjects[0].(*appsv1.StatefulSet)
	require.True(t, ok)

	rootArgs := sts.Spec.Template.Spec.Containers[0].Args
	var threadsArg string
	for i := 0; i < len(rootArgs)-1; i++ {
		if rootArgs[i] == "--nthreads" {
			threadsArg = rootArgs[i+1]
			break
		}
	}
	require.Equal(t, "2", threadsArg, "zero replica power should default to two root threads")

	annotations := sts.Spec.Template.ObjectMeta.Annotations
	require.Contains(t, annotations, annotationMemoryPlan)
	require.Contains(t, annotations[annotationMemoryPlan], "nodes=2", "worker replica fallback should influence memory plan annotation")
}

func TestRootHandlerEnsureStatefulSetDeletesLegacyDeployment(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
		deployments:  deployments,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 1 << 30},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	deploymentCache := genericfake.NewMockCacheInterface[*appsv1.Deployment](ctrl)
	deployments.EXPECT().Cache().Return(deploymentCache)
	deploymentCache.EXPECT().Get("models", "mistral-root").Return(&appsv1.Deployment{}, nil)
	deployments.EXPECT().Delete("models", "mistral-root", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).Return(nil)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-root",
			Labels:    map[string]string{labelDllamaName: "mistral"},
		},
		Spec: v1.RootSpec{ModelRef: "mistral-pvc", WorkerSelector: map[string]string{"app": "dllama"}},
	}

	require.NoError(t, h.ensureStatefulSet(root))
	require.Len(t, fakeApply.appliedObjects, 1)
}

func TestRootHandlerEnsureStatefulSetHandlesDeploymentErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
		deployments:  deployments,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 1 << 30},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	deploymentCache := genericfake.NewMockCacheInterface[*appsv1.Deployment](ctrl)
	deployments.EXPECT().Cache().Return(deploymentCache)
	deploymentCache.EXPECT().Get("models", "mistral-root").Return(&appsv1.Deployment{}, nil)
	expected := apierrors.NewInternalError(fmt.Errorf("delete legacy deployment"))
	deployments.EXPECT().Delete("models", "mistral-root", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).Return(expected)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{ModelRef: "mistral-pvc", WorkerSelector: map[string]string{"app": "dllama"}},
	}

	err := h.ensureStatefulSet(root)
	require.ErrorContains(t, err, "delete legacy root deployment")
	require.ErrorIs(t, err, expected)
}

func TestRootHandlerEnsureStatefulSetHandlesDeploymentLookupErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
		deployments:  deployments,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}},
	}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 1 << 30},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	deploymentCache := genericfake.NewMockCacheInterface[*appsv1.Deployment](ctrl)
	deployments.EXPECT().Cache().Return(deploymentCache)
	expected := apierrors.NewInternalError(fmt.Errorf("lookup error"))
	deploymentCache.EXPECT().Get("models", "mistral-root").Return(nil, expected)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{ModelRef: "mistral-pvc", WorkerSelector: map[string]string{"app": "dllama"}},
	}

	err := h.ensureStatefulSet(root)
	require.ErrorContains(t, err, "lookup legacy root deployment")
	require.ErrorIs(t, err, expected)
}

func TestRootHandlerEnsureStatusUpdatesReadyCondition(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	services := genericfake.NewMockControllerInterface[*corev1.Service, *corev1.ServiceList](ctrl)
	roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)

	h := &rootHandler{
		dllamas:      dllamas,
		workers:      workers,
		statefulsets: statefulsets,
		services:     services,
		roots:        roots,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}}}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil)

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ns, name string) (*appsv1.StatefulSet, error) {
		switch name {
		case workerName:
			return &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil
		case rootStatefulSetName("mistral-root"):
			return &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil
		default:
			return nil, apierrors.NewNotFound(appsv1.Resource("statefulsets"), name)
		}
	}).AnyTimes()

	serviceCache := genericfake.NewMockCacheInterface[*corev1.Service](ctrl)
	services.EXPECT().Cache().Return(serviceCache).AnyTimes()
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "mistral-root", Namespace: "models"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 9999}}}}
	serviceCache.EXPECT().Get("models", "mistral-root").Return(svc, nil)

	var updated *v1.Root
	roots.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Root{})).DoAndReturn(func(obj *v1.Root) (*v1.Root, error) {
		updated = obj
		return obj, nil
	})

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-root",
			Labels:    map[string]string{labelDllamaName: "mistral"},
		},
		Spec:   v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		Status: v1.RootStatus{ObservedGeneration: 0},
	}
	root.Generation = 2

	result, err := h.ensureStatus(root)
	require.NoError(t, err)
	require.Same(t, updated, result)
	require.Equal(t, "mistral-root.models.svc.cluster.local:9999", updated.Status.Endpoint)
	require.NotEmpty(t, updated.Status.Conditions)
	require.Equal(t, metav1.ConditionTrue, updated.Status.Conditions[0].Status)
}

func TestRootHandlerEnsureStatusHandlesWorkerErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	services := genericfake.NewMockControllerInterface[*corev1.Service, *corev1.ServiceList](ctrl)
	roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)

	h := &rootHandler{
		dllamas:      dllamas,
		workers:      workers,
		statefulsets: statefulsets,
		services:     services,
		roots:        roots,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}}}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	expected := errors.New("statefulset cache unavailable")
	stsCache.EXPECT().Get("models", workerName).Return(nil, expected)

	serviceCache := genericfake.NewMockCacheInterface[*corev1.Service](ctrl)
	services.EXPECT().Cache().Return(serviceCache).AnyTimes()
	serviceCache.EXPECT().Get("models", "mistral-root").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "core", Resource: "services"}, "mistral-root"))

	var updated *v1.Root
	roots.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Root{})).DoAndReturn(func(obj *v1.Root) (*v1.Root, error) {
		updated = obj
		return obj, nil
	})

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		Status:     v1.RootStatus{},
	}
	root.Generation = 5

	result, err := h.ensureStatus(root)
	require.NoError(t, err)
	require.Same(t, updated, result)
	require.Len(t, updated.Status.Conditions, 1)
	cond := updated.Status.Conditions[0]
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "WorkersLookupFailed", cond.Reason)
	require.Contains(t, cond.Message, expected.Error())
	require.Equal(t, root.Generation, updated.Status.ObservedGeneration)
}

func TestRootHandlerEnsureStatusWaitsForWorkers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	services := genericfake.NewMockControllerInterface[*corev1.Service, *corev1.ServiceList](ctrl)
	roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)

	h := &rootHandler{
		dllamas:      dllamas,
		workers:      workers,
		statefulsets: statefulsets,
		services:     services,
		roots:        roots,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 2, ModelRef: v1.ModelReference{Name: "mistral"}}}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 0}}, nil).AnyTimes()

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "mistral-root", Namespace: "models"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 9999}}}}
	serviceCache := genericfake.NewMockCacheInterface[*corev1.Service](ctrl)
	services.EXPECT().Cache().Return(serviceCache).AnyTimes()
	serviceCache.EXPECT().Get("models", "mistral-root").Return(svc, nil)

	var updated *v1.Root
	roots.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Root{})).DoAndReturn(func(obj *v1.Root) (*v1.Root, error) {
		updated = obj
		return obj, nil
	})

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		Status:     v1.RootStatus{},
	}
	root.Generation = 3

	result, err := h.ensureStatus(root)
	require.NoError(t, err)
	require.Same(t, updated, result)
	require.Equal(t, "mistral-root.models.svc.cluster.local:9999", updated.Status.Endpoint)
	require.NotEmpty(t, updated.Status.Conditions)
	cond := updated.Status.Conditions[0]
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "WorkersNotReady", cond.Reason)
	require.Equal(t, root.Generation, updated.Status.ObservedGeneration)
}

func TestRootHandlerEnsureStatusHandlesRootStatefulSetLookupErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	services := genericfake.NewMockControllerInterface[*corev1.Service, *corev1.ServiceList](ctrl)
	roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)

	h := &rootHandler{
		dllamas:      dllamas,
		workers:      workers,
		statefulsets: statefulsets,
		services:     services,
		roots:        roots,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}}}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	expected := errors.New("root statefulset unavailable")
	gomock.InOrder(
		stsCache.EXPECT().Get("models", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil),
		stsCache.EXPECT().Get("models", rootStatefulSetName("mistral-root")).Return(nil, expected),
	)

	serviceCache := genericfake.NewMockCacheInterface[*corev1.Service](ctrl)
	services.EXPECT().Cache().Return(serviceCache).AnyTimes()
	serviceCache.EXPECT().Get("models", "mistral-root").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "core", Resource: "services"}, "mistral-root"))

	var updated *v1.Root
	roots.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Root{})).DoAndReturn(func(obj *v1.Root) (*v1.Root, error) {
		updated = obj
		return obj, nil
	})

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "mistral-root", Labels: map[string]string{labelDllamaName: "mistral"}},
		Spec:       v1.RootSpec{WorkerSelector: map[string]string{"app": "dllama"}},
		Status:     v1.RootStatus{},
	}
	root.Generation = 7

	result, err := h.ensureStatus(root)
	require.NoError(t, err)
	require.Same(t, updated, result)
	require.Len(t, updated.Status.Conditions, 1)
	cond := updated.Status.Conditions[0]
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "StatefulSetLookupFailed", cond.Reason)
	require.Contains(t, cond.Message, expected.Error())
	require.Equal(t, root.Generation, updated.Status.ObservedGeneration)
}

func TestRootHandlerOnChangeReconcilesResources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	fakeApply := newFakeApply()
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	services := genericfake.NewMockControllerInterface[*corev1.Service, *corev1.ServiceList](ctrl)
	roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)

	h := &rootHandler{
		apply:        fakeApply,
		dllamas:      dllamas,
		models:       models,
		workers:      workers,
		statefulsets: statefulsets,
		services:     services,
		roots:        roots,
	}

	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamas.EXPECT().Cache().Return(dllamaCache).AnyTimes()
	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}, Spec: v1.DllamaSpec{ReplicaPower: 1, ModelRef: v1.ModelReference{Name: "mistral"}}}
	dllamaCache.EXPECT().Get("models", "mistral").Return(dllama, nil).AnyTimes()

	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	models.EXPECT().Cache().Return(modelCache).AnyTimes()
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{Conversion: &v1.ModelConversionSpec{WeightsFloatType: "fp16", ConvertWeights: "q4_0"}},
		Status:     v1.ModelStatus{ConversionSizeBytes: 8 << 30, ConversionSizeHuman: "8Gi"},
	}
	modelCache.EXPECT().Get("models", "mistral").Return(model, nil).AnyTimes()

	workerCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workers.EXPECT().Cache().Return(workerCache).AnyTimes()
	workerName := workerResourceName("mistral")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "models"}}
	workerCache.EXPECT().Get("models", workerName).Return(worker, nil).AnyTimes()

	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	statefulsets.EXPECT().Cache().Return(stsCache).AnyTimes()
	stsCache.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ns, name string) (*appsv1.StatefulSet, error) {
		switch name {
		case workerName:
			return &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil
		case rootStatefulSetName("mistral-root"):
			return &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil
		default:
			return nil, apierrors.NewNotFound(appsv1.Resource("statefulsets"), name)
		}
	}).AnyTimes()

	serviceCache := genericfake.NewMockCacheInterface[*corev1.Service](ctrl)
	services.EXPECT().Cache().Return(serviceCache).AnyTimes()
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "mistral-root", Namespace: "models"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 9999}}}}
	serviceCache.EXPECT().Get("models", "mistral-root").Return(svc, nil).AnyTimes()

	var updated *v1.Root
	roots.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Root{})).DoAndReturn(func(obj *v1.Root) (*v1.Root, error) {
		updated = obj
		return obj, nil
	})

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-root",
			Labels: map[string]string{
				labelDllamaName:       "mistral",
				labelConversationHash: "hash-1",
			},
		},
		Spec: v1.RootSpec{
			ModelRef:       "mistral-model",
			Image:          "ghcr.io/gorizond/root",
			WorkerSelector: map[string]string{"app": "dllama"},
		},
	}

	result, err := h.onChange("models/mistral-root", root)
	require.NoError(t, err)
	require.Same(t, updated, result)
	require.Len(t, fakeApply.appliedObjects, 2)
	require.IsType(t, &corev1.Service{}, fakeApply.appliedObjects[0])
	require.IsType(t, &appsv1.StatefulSet{}, fakeApply.appliedObjects[1])
}

func TestRootHandlerOnChangeHandlesNilAndDeletion(t *testing.T) {
	t.Parallel()

	h := &rootHandler{}
	result, err := h.onChange("", nil)
	require.NoError(t, err)
	require.Nil(t, result)

	obj := &v1.Root{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{}}}
	result, err = h.onChange("models/demo", obj)
	require.NoError(t, err)
	require.Same(t, obj, result)
}

func TestRootHandlerOnChangePropagatesEnsureServiceError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	applyMock := newGomockApply(ctrl)
	h := &rootHandler{apply: applyMock}
	root := &v1.Root{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "models"}, Spec: v1.RootSpec{ModelRef: "demo-model"}}
	sentinel := errors.New("apply failure")
	applyMock.EXPECT().ApplyObjects(gomock.Any()).Return(sentinel)

	result, err := h.onChange("models/demo", root)
	require.ErrorIs(t, err, sentinel)
	require.Same(t, root, result)
}

func TestRootHandlerResolveDllamaRequiresLabel(t *testing.T) {
	t.Parallel()

	h := &rootHandler{}
	_, err := h.resolveDllama(&v1.Root{ObjectMeta: metav1.ObjectMeta{Name: "demo"}})
	require.Error(t, err)
}

func TestRootHandlerResolveModelValidatesName(t *testing.T) {
	t.Parallel()

	h := &rootHandler{}
	_, err := h.resolveModel(nil)
	require.Error(t, err)

	dllama := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "models"}}
	_, err = h.resolveModel(dllama)
	require.Error(t, err)
}
