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
)

func TestWorkerHandlerEnsureStatefulSetAppliesObjects(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "mistral").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "koldun.gorizond.io", Resource: "dllamas"}, "mistral"))

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
			Labels: map[string]string{
				labelComponent:  componentWorker,
				labelWorkerName: "mistral-workers",
				labelDllamaName: "mistral",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Args:  []string{"--foo", "bar"},
			Slot:  7,
			CacheSpec: &v1.CacheSpec{
				Endpoint: "https://minio:9000",
				Bucket:   "models",
				SecretRef: &v1.SecretReference{
					Name: "cache-creds",
				},
			},
			NATS: &v1.WorkerNATSConfig{
				URL: "nats://nats:4222",
				CredentialsSecret: &v1.SecretReference{
					Name: "nats-creds",
				},
			},
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		if len(objs) != 2 {
			t.Fatalf("expected two objects, got %d", len(objs))
		}

		svc, ok := objs[0].(*corev1.Service)
		if !ok {
			t.Fatalf("expected first object to be Service, got %T", objs[0])
		}
		require.Equal(t, worker.Name, svc.Name)
		require.Equal(t, worker.Namespace, svc.Namespace)
		require.Equal(t, componentWorker, svc.Labels[labelComponent])
		require.Equal(t, sanitizeLabelValue(worker.Name), svc.Labels[labelWorkerName])
		require.Equal(t, "7", svc.Annotations[annotationSlotKey])
		require.True(t, svc.Spec.PublishNotReadyAddresses)

		sts, ok := objs[1].(*appsv1.StatefulSet)
		if !ok {
			t.Fatalf("expected second object to be StatefulSet, got %T", objs[1])
		}
		require.Equal(t, worker.Name, sts.Name)
		require.Equal(t, worker.Namespace, sts.Namespace)
		require.EqualValues(t, 1, *sts.Spec.Replicas)
		require.Equal(t, "mistral-workers", sts.Spec.ServiceName)
		require.NotNil(t, sts.Spec.Selector)
		require.Equal(t, sanitizeLabelValue(worker.Name), sts.Spec.Template.Labels[labelWorkerName])
		require.Equal(t, "7", sts.Annotations[annotationSlotKey])
		require.Equal(t, "7", sts.Spec.Template.Annotations[annotationSlotKey])

		pod := sts.Spec.Template.Spec
		require.Len(t, pod.Containers, 1)
		container := pod.Containers[0]
		require.Equal(t, "dllama", strings.Join(container.Command, " "))
		require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "1", "--foo", "bar"}, container.Args)
		require.Equal(t, int32(9999), container.Ports[0].ContainerPort)

		roleEnv := envVarValue(container.Env, "DLLAMA_ROLE")
		require.NotNil(t, roleEnv)
		require.Equal(t, "worker", roleEnv.Value)

		cacheEndpoint := envVarValue(container.Env, "CACHE_ENDPOINT")
		require.NotNil(t, cacheEndpoint)
		require.Equal(t, "https://minio:9000", cacheEndpoint.Value)

		cacheSecret := envVarValue(container.Env, "CACHE_SECRET")
		require.NotNil(t, cacheSecret)
		require.NotNil(t, cacheSecret.ValueFrom)
		require.NotNil(t, cacheSecret.ValueFrom.SecretKeyRef)
		require.Equal(t, "cache-creds", cacheSecret.ValueFrom.SecretKeyRef.Name)
		require.Equal(t, "credentials", cacheSecret.ValueFrom.SecretKeyRef.Key)
		if cacheSecret.ValueFrom.SecretKeyRef.Optional == nil || !*cacheSecret.ValueFrom.SecretKeyRef.Optional {
			t.Fatalf("CACHE_SECRET optional flag not set")
		}

		natsURL := envVarValue(container.Env, "NATS_URL")
		require.NotNil(t, natsURL)
		require.Equal(t, "nats://nats:4222", natsURL.Value)

		natsCreds := envVarValue(container.Env, "NATS_CREDS")
		require.NotNil(t, natsCreds)
		require.NotNil(t, natsCreds.ValueFrom)
		require.Equal(t, "nats-creds", natsCreds.ValueFrom.SecretKeyRef.Name)
		require.Equal(t, "nats.creds", natsCreds.ValueFrom.SecretKeyRef.Key)
		if natsCreds.ValueFrom.SecretKeyRef.Optional == nil || !*natsCreds.ValueFrom.SecretKeyRef.Optional {
			t.Fatalf("NATS_CREDS optional flag not set")
		}

		require.Empty(t, container.Resources.Requests)
		require.Empty(t, container.Resources.Limits)

		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatusInitializesNilConditions(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("models", "test-worker").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(1),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 0,
		},
	}, nil)

	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		require.NotNil(t, updated.Status.Conditions)
		require.Len(t, updated.Status.Conditions, 1)
		requireCondition(t, updated.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  "StatefulSetNotReady",
			Message: "Worker statefulset is not yet ready",
		})
		return updated, nil
	})

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			Conditions: nil, // Explicitly nil conditions
		},
	}
	worker.Generation = 1

	result, err := handler.ensureStatus(worker)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestWorkerHandlerEnsureStatusStatefulSetNotFound(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "test-worker")
	stsCache.EXPECT().Get("models", "test-worker").Return(nil, notFoundErr)

	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		require.EqualValues(t, 2, updated.Status.ObservedGeneration)
		requireCondition(t, updated.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  "StatefulSetNotReady",
			Message: "Worker statefulset is not yet ready",
		})
		return updated, nil
	})

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			Conditions: []metav1.Condition{},
		},
	}
	worker.Generation = 2

	result, err := handler.ensureStatus(worker)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestWorkerHandlerEnsureStatusPartiallyReadyReplicas(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("models", "test-worker").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(5),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 3, // Only 3 out of 5 ready
		},
	}, nil)

	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		requireCondition(t, updated.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  "StatefulSetNotReady",
			Message: "Worker statefulset is not yet ready",
		})
		return updated, nil
	})

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			Conditions: []metav1.Condition{},
		},
	}
	worker.Generation = 1

	result, err := handler.ensureStatus(worker)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestWorkerHandlerEnsureStatusNilReplicas(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("models", "test-worker").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: nil, // Should default to 1
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 1,
		},
	}, nil)

	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		requireCondition(t, updated.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "StatefulSetReady",
			Message: "Worker statefulset is ready",
		})
		return updated, nil
	})

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			Conditions: []metav1.Condition{},
		},
	}
	worker.Generation = 1

	result, err := handler.ensureStatus(worker)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestWorkerHandlerEnsureStatefulSetUsesModelMemoryPlan(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
		models:  models,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "mistral").Return(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ReplicaPower: 2,
			ModelRef: v1.ModelReference{
				Name: "mistral",
				Kind: "Model",
			},
		},
	}, nil)

	models.EXPECT().Cache().Return(modelCache)
	modelCache.EXPECT().Get("models", "mistral").Return(&v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral",
			Namespace: "models",
		},
		Status: v1.ModelStatus{
			ConversionSizeBytes: 64 * 1024 * 1024,
			ConversionSizeHuman: "64Mi",
		},
	}, nil)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
			Labels: map[string]string{
				labelComponent:  componentWorker,
				labelWorkerName: "mistral-workers",
				labelDllamaName: "mistral",
			},
		},
		Spec: v1.WorkerSpec{
			Image:    "ghcr.io/gorizond/dllama:latest",
			Slot:     3,
			NThreads: 4,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		sts := objs[1].(*appsv1.StatefulSet)
		require.NotNil(t, sts.Spec.Replicas)
		require.EqualValues(t, 3, *sts.Spec.Replicas)
		require.Contains(t, sts.Spec.Template.Annotations, annotationMemoryPlan)
		// 64Mi model / 4 nodes = 16Mi per worker * 2.50 overhead = 40Mi
		require.Contains(t, sts.Spec.Template.Annotations[annotationMemoryPlan], "worker=40Mi")
		require.Equal(t, "64Mi", sts.Spec.Template.Annotations[annotationConversionSizeHuman])

		container := sts.Spec.Template.Spec.Containers[0]
		require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "4"}, container.Args)
		requestMem := container.Resources.Requests[corev1.ResourceMemory]
		limitMem := container.Resources.Limits[corev1.ResourceMemory]
		// Memory request matches the calculated worker memory with overhead
		require.Equal(t, "40Mi", requestMem.String())
		require.Equal(t, "40Mi", limitMem.String())

		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatefulSetPropagatesDllamaErrors(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	expectedErr := errors.New("cache failure")
	dllamaCache.EXPECT().Get("models", "mistral").Return(nil, expectedErr)
	mockApply.EXPECT().ApplyObjects(gomock.Any()).Times(0)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "mistral",
			},
		},
	}

	err := handler.ensureStatefulSet(worker)
	require.ErrorIs(t, err, expectedErr)
}

func TestWorkerHandlerEnsureStatusMarksReady(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("models", "mistral-workers").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(3),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 3,
		},
	}, nil)

	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		require.EqualValues(t, 3, updated.Status.ObservedGeneration)
		requireCondition(t, updated.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "StatefulSetReady",
			Message: "Worker statefulset is ready",
		})
		return updated, nil
	})

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			Conditions: []metav1.Condition{},
		},
	}
	worker.Generation = 3

	result, err := handler.ensureStatus(worker)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotSame(t, worker, result)
}

func TestWorkerHandlerEnsureStatusNoopWhenUnchanged(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("models", "mistral-workers").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(1),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 0,
		},
	}, nil)

	workers.EXPECT().UpdateStatus(gomock.Any()).Times(0)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			ObservedGeneration: 5,
			Conditions: []metav1.Condition{
				{
					Type:    conditionReady,
					Status:  metav1.ConditionFalse,
					Reason:  "StatefulSetNotReady",
					Message: "Worker statefulset is not yet ready",
				},
			},
		},
	}
	worker.Generation = 5

	result, err := handler.ensureStatus(worker)
	require.NoError(t, err)
	require.Equal(t, worker, result)
}

func TestWorkerHandlerEnsureStatusPropagatesUpdateErrors(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		workers:      workers,
		statefulsets: statefulsets,
	}

	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("models", "mistral-workers").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(2),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 0,
		},
	}, nil)

	updateErr := errors.New("status update failed")
	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		require.EqualValues(t, 6, updated.Status.ObservedGeneration)
		return nil, updateErr
	})

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
		},
		Status: v1.WorkerStatus{
			ObservedGeneration: 5,
			Conditions: []metav1.Condition{
				{
					Type:    conditionReady,
					Status:  metav1.ConditionFalse,
					Reason:  "StatefulSetNotReady",
					Message: "Worker statefulset is not yet ready",
				},
			},
		},
	}
	worker.Generation = 6

	_, err := handler.ensureStatus(worker)
	require.ErrorIs(t, err, updateErr)
}

func envVarValue(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func int32Ptr(val int32) *int32 {
	return &val
}

func requireCondition(t *testing.T, conditions []metav1.Condition, expected metav1.Condition) {
	t.Helper()
	for _, cond := range conditions {
		if cond.Type == expected.Type {
			require.Equal(t, expected.Status, cond.Status)
			require.Equal(t, expected.Reason, cond.Reason)
			require.Equal(t, expected.Message, cond.Message)
			return
		}
	}
	t.Fatalf("condition %q not found in %#v", expected.Type, conditions)
}

func TestWorkerHandlerOnChangeSuccess(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		apply:        mockApply,
		workers:      workers,
		dllamas:      dllamas,
		statefulsets: statefulsets,
	}

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "default",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}
	worker.Generation = 1

	// ensureStatefulSet expectations
	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("default", "test-dllama").Return(nil, apierrors.NewNotFound(schema.GroupResource{}, "test-dllama"))
	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).Return(nil)

	// ensureStatus expectations
	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("default", "test-worker").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(1),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 1,
		},
	}, nil)
	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		requireCondition(t, updated.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "StatefulSetReady",
			Message: "Worker statefulset is ready",
		})
		return updated, nil
	})

	result, err := handler.onChange("default/test-worker", worker)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestWorkerHandlerOnChangeNilWorker(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	handler := &workerHandler{}

	result, err := handler.onChange("default/test-worker", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestWorkerHandlerOnChangeDeletionTimestamp(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	handler := &workerHandler{}

	now := metav1.Now()
	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-worker",
			Namespace:         "default",
			DeletionTimestamp: &now,
		},
	}

	result, err := handler.onChange("default/test-worker", worker)
	require.NoError(t, err)
	require.Equal(t, worker, result)
}

func TestWorkerHandlerOnChangeEnsureStatefulSetError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)

	handler := &workerHandler{
		apply:        mockApply,
		dllamas:      dllamas,
		statefulsets: statefulsets,
	}

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "default",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	expectedErr := errors.New("apply failed")
	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("default", "test-dllama").Return(nil, apierrors.NewNotFound(schema.GroupResource{}, "test-dllama"))
	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).Return(expectedErr)

	result, err := handler.onChange("default/test-worker", worker)
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, worker, result)
}

func TestWorkerHandlerOnChangeEnsureStatusError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	statefulsets := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)

	handler := &workerHandler{
		apply:        mockApply,
		workers:      workers,
		dllamas:      dllamas,
		statefulsets: statefulsets,
	}

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "default",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}
	worker.Generation = 2

	// ensureStatefulSet expectations
	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("default", "test-dllama").Return(nil, apierrors.NewNotFound(schema.GroupResource{}, "test-dllama"))
	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).Return(nil)

	// ensureStatus expectations
	expectedErr := errors.New("status update failed")
	statefulsets.EXPECT().Cache().Return(stsCache)
	stsCache.EXPECT().Get("default", "test-worker").Return(&appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(1),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 0,
		},
	}, nil)
	workers.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Worker{})).DoAndReturn(func(updated *v1.Worker) (*v1.Worker, error) {
		return nil, expectedErr
	})

	result, err := handler.onChange("default/test-worker", worker)
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, result)
}

func TestWorkerHandlerEnsureStatefulSetWithConversationHash(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "mistral").Return(nil, apierrors.NewNotFound(schema.GroupResource{}, "mistral"))

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-workers",
			Namespace: "models",
			Labels: map[string]string{
				labelComponent:        componentWorker,
				labelWorkerName:       "mistral-workers",
				labelDllamaName:       "mistral",
				labelConversationHash: "abc123def",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  5,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		svc := objs[0].(*corev1.Service)
		require.Equal(t, "abc123def", svc.Labels[labelConversationHash])

		sts := objs[1].(*appsv1.StatefulSet)
		require.Equal(t, "abc123def", sts.Labels[labelConversationHash])
		require.Equal(t, "abc123def", sts.Spec.Template.Labels[labelConversationHash])

		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatefulSetWithoutDllamaLabel(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
	}

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone-worker",
			Namespace: "default",
			Labels: map[string]string{
				labelComponent:  componentWorker,
				labelWorkerName: "standalone-worker",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		sts := objs[1].(*appsv1.StatefulSet)
		require.EqualValues(t, 1, *sts.Spec.Replicas)
		container := sts.Spec.Template.Spec.Containers[0]
		require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "1"}, container.Args)
		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatefulSetReplicasZeroOrNegative(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "test-dllama").Return(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dllama",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ReplicaPower: -5,
		},
	}, nil)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		sts := objs[1].(*appsv1.StatefulSet)
		require.EqualValues(t, 1, *sts.Spec.Replicas)
		container := sts.Spec.Template.Spec.Containers[0]
		require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "1"}, container.Args)
		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatefulSetThreadsZeroOrNegative(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "test-dllama").Return(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dllama",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ReplicaPower: -3,
		},
	}, nil)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		sts := objs[1].(*appsv1.StatefulSet)
		container := sts.Spec.Template.Spec.Containers[0]
		require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "1"}, container.Args)
		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatefulSetModelCacheError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
		models:  models,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "test-dllama").Return(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dllama",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ReplicaPower: 2,
			ModelRef: v1.ModelReference{
				Name: "test-model",
				Kind: "Model",
			},
		},
	}, nil)

	expectedErr := errors.New("model cache error")
	models.EXPECT().Cache().Return(modelCache)
	modelCache.EXPECT().Get("models", "test-model").Return(nil, expectedErr)
	mockApply.EXPECT().ApplyObjects(gomock.Any()).Times(0)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	err := handler.ensureStatefulSet(worker)
	require.ErrorIs(t, err, expectedErr)
}

func TestWorkerHandlerEnsureStatefulSetEmptyModelName(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
		models:  models,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "test-dllama").Return(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dllama",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ReplicaPower: 2,
			ModelRef: v1.ModelReference{
				Name: "  ",
				Kind: "Model",
			},
		},
	}, nil)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		sts := objs[1].(*appsv1.StatefulSet)
		require.EqualValues(t, 3, *sts.Spec.Replicas)
		container := sts.Spec.Template.Spec.Containers[0]
		require.Empty(t, container.Resources.Requests)
		require.Empty(t, container.Resources.Limits)
		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

func TestWorkerHandlerEnsureStatefulSetEmptyConversionSizeHuman(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	dllamaCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	modelCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)

	handler := &workerHandler{
		apply:   mockApply,
		dllamas: dllamas,
		models:  models,
	}

	dllamas.EXPECT().Cache().Return(dllamaCache)
	dllamaCache.EXPECT().Get("models", "test-dllama").Return(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dllama",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ReplicaPower: 2,
			ModelRef: v1.ModelReference{
				Name: "test-model",
				Kind: "Model",
			},
		},
	}, nil)

	models.EXPECT().Cache().Return(modelCache)
	modelCache.EXPECT().Get("models", "test-model").Return(&v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "models",
		},
		Status: v1.ModelStatus{
			ConversionSizeBytes: 128 * 1024 * 1024,
			ConversionSizeHuman: "  ",
		},
	}, nil)

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "test-dllama",
			},
		},
		Spec: v1.WorkerSpec{
			Image: "ghcr.io/gorizond/dllama:latest",
			Slot:  1,
		},
	}

	mockApply.EXPECT().ApplyObjects(
		gomock.AssignableToTypeOf(&corev1.Service{}),
		gomock.AssignableToTypeOf(&appsv1.StatefulSet{}),
	).DoAndReturn(func(objs ...interface{}) error {
		sts := objs[1].(*appsv1.StatefulSet)
		conversionSize := sts.Spec.Template.Annotations[annotationConversionSizeHuman]
		require.Equal(t, "134217728B", conversionSize)
		memPlan := sts.Spec.Template.Annotations[annotationMemoryPlan]
		require.Contains(t, memPlan, "model=134217728B")
		return nil
	})

	err := handler.ensureStatefulSet(worker)
	require.NoError(t, err)
}

// TestWorkerContainer tests the workerContainer function
func TestWorkerContainer(t *testing.T) {

	tests := []struct {
		name      string
		worker    *v1.Worker
		threads   int32
		resources corev1.ResourceRequirements
		validate  func(t *testing.T, container corev1.Container)
	}{
		{
			name: "basic worker with minimal config",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:v1.0.0",
				},
			},
			threads:   4,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Equal(t, "worker", container.Name)
				require.Equal(t, "ghcr.io/gorizond/dllama:v1.0.0", container.Image)
				require.Equal(t, corev1.PullIfNotPresent, container.ImagePullPolicy)
				require.Equal(t, []string{"dllama"}, container.Command)
				require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "4"}, container.Args)
				require.Len(t, container.Env, 1)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
				require.Equal(t, "worker", container.Env[0].Value)
				require.Len(t, container.Ports, 1)
				require.Equal(t, int32(9999), container.Ports[0].ContainerPort)
				require.Empty(t, container.Resources.Requests)
				require.Empty(t, container.Resources.Limits)
			},
		},
		{
			name: "worker with custom args",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					Args:  []string{"--foo", "bar", "--verbose"},
				},
			},
			threads:   2,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "2", "--foo", "bar", "--verbose"}, container.Args)
			},
		},
		{
			name: "worker with cache config",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					CacheSpec: &v1.CacheSpec{
						Endpoint: "https://minio:9000",
						Bucket:   "models",
					},
				},
			},
			threads:   8,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 3)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
				require.Equal(t, "worker", container.Env[0].Value)
				require.Equal(t, "CACHE_ENDPOINT", container.Env[1].Name)
				require.Equal(t, "https://minio:9000", container.Env[1].Value)
				require.Equal(t, "CACHE_BUCKET", container.Env[2].Name)
				require.Equal(t, "models", container.Env[2].Value)
			},
		},
		{
			name: "worker with cache and secret",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					CacheSpec: &v1.CacheSpec{
						Endpoint: "https://s3.amazonaws.com",
						Bucket:   "my-bucket",
						SecretRef: &v1.SecretReference{
							Name: "s3-credentials",
						},
					},
				},
			},
			threads:   1,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 4)
				require.Equal(t, "CACHE_ENDPOINT", container.Env[1].Name)
				require.Equal(t, "https://s3.amazonaws.com", container.Env[1].Value)
				require.Equal(t, "CACHE_BUCKET", container.Env[2].Name)
				require.Equal(t, "my-bucket", container.Env[2].Value)
				require.Equal(t, "CACHE_SECRET", container.Env[3].Name)
				require.Empty(t, container.Env[3].Value)
				require.NotNil(t, container.Env[3].ValueFrom)
				require.NotNil(t, container.Env[3].ValueFrom.SecretKeyRef)
				require.Equal(t, "s3-credentials", container.Env[3].ValueFrom.SecretKeyRef.Name)
				require.Equal(t, "credentials", container.Env[3].ValueFrom.SecretKeyRef.Key)
				require.NotNil(t, container.Env[3].ValueFrom.SecretKeyRef.Optional)
				require.True(t, *container.Env[3].ValueFrom.SecretKeyRef.Optional)
			},
		},
		{
			name: "worker with NATS config",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					NATS: &v1.WorkerNATSConfig{
						URL: "nats://nats.example.com:4222",
					},
				},
			},
			threads:   16,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 2)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
				require.Equal(t, "NATS_URL", container.Env[1].Name)
				require.Equal(t, "nats://nats.example.com:4222", container.Env[1].Value)
			},
		},
		{
			name: "worker with NATS and credentials secret",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					NATS: &v1.WorkerNATSConfig{
						URL: "nats://secured.example.com:4222",
						CredentialsSecret: &v1.SecretReference{
							Name: "nats-creds-secret",
						},
					},
				},
			},
			threads:   3,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 3)
				require.Equal(t, "NATS_URL", container.Env[1].Name)
				require.Equal(t, "nats://secured.example.com:4222", container.Env[1].Value)
				require.Equal(t, "NATS_CREDS", container.Env[2].Name)
				require.Empty(t, container.Env[2].Value)
				require.NotNil(t, container.Env[2].ValueFrom)
				require.NotNil(t, container.Env[2].ValueFrom.SecretKeyRef)
				require.Equal(t, "nats-creds-secret", container.Env[2].ValueFrom.SecretKeyRef.Name)
				require.Equal(t, "nats.creds", container.Env[2].ValueFrom.SecretKeyRef.Key)
				require.NotNil(t, container.Env[2].ValueFrom.SecretKeyRef.Optional)
				require.True(t, *container.Env[2].ValueFrom.SecretKeyRef.Optional)
			},
		},
		{
			name: "worker with all configs (cache + NATS)",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					Args:  []string{"--debug"},
					CacheSpec: &v1.CacheSpec{
						Endpoint: "https://minio:9000",
						Bucket:   "models",
						SecretRef: &v1.SecretReference{
							Name: "cache-creds",
						},
					},
					NATS: &v1.WorkerNATSConfig{
						URL: "nats://nats:4222",
						CredentialsSecret: &v1.SecretReference{
							Name: "nats-creds",
						},
					},
				},
			},
			threads:   6,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "6", "--debug"}, container.Args)
				require.Len(t, container.Env, 6)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
				require.Equal(t, "CACHE_ENDPOINT", container.Env[1].Name)
				require.Equal(t, "CACHE_BUCKET", container.Env[2].Name)
				require.Equal(t, "CACHE_SECRET", container.Env[3].Name)
				require.Equal(t, "NATS_URL", container.Env[4].Name)
				require.Equal(t, "NATS_CREDS", container.Env[5].Name)
			},
		},
		{
			name: "worker with resource requirements",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
				},
			},
			threads: 4,
			resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: mustParseQuantity("1Gi"),
					corev1.ResourceCPU:    mustParseQuantity("500m"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: mustParseQuantity("2Gi"),
					corev1.ResourceCPU:    mustParseQuantity("1000m"),
				},
			},
			validate: func(t *testing.T, container corev1.Container) {
				require.NotEmpty(t, container.Resources.Requests)
				require.NotEmpty(t, container.Resources.Limits)
				requestMem := container.Resources.Requests[corev1.ResourceMemory]
				requestCPU := container.Resources.Requests[corev1.ResourceCPU]
				limitMem := container.Resources.Limits[corev1.ResourceMemory]
				limitCPU := container.Resources.Limits[corev1.ResourceCPU]
				require.Equal(t, "1Gi", requestMem.String())
				require.Equal(t, "500m", requestCPU.String())
				require.Equal(t, "2Gi", limitMem.String())
				require.Equal(t, "1", limitCPU.String())
			},
		},
		{
			name: "worker with zero threads",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
				},
			},
			threads:   0,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "0"}, container.Args)
			},
		},
		{
			name: "worker with negative threads",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
				},
			},
			threads:   -5,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "-5"}, container.Args)
			},
		},
		{
			name: "worker with empty args slice",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					Args:  []string{},
				},
			},
			threads:   4,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Equal(t, []string{"worker", "--port", "9999", "--nthreads", "4"}, container.Args)
			},
		},
		{
			name: "worker with nil cache spec",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image:     "ghcr.io/gorizond/dllama:latest",
					CacheSpec: nil,
				},
			},
			threads:   4,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 1)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
			},
		},
		{
			name: "worker with nil NATS config",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					NATS:  nil,
				},
			},
			threads:   4,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 1)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
			},
		},
		{
			name: "worker with cache but nil secret ref",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					CacheSpec: &v1.CacheSpec{
						Endpoint:  "https://minio:9000",
						Bucket:    "models",
						SecretRef: nil,
					},
				},
			},
			threads:   4,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 3)
				require.Equal(t, "CACHE_ENDPOINT", container.Env[1].Name)
				require.Equal(t, "CACHE_BUCKET", container.Env[2].Name)
			},
		},
		{
			name: "worker with NATS but nil credentials secret",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
					NATS: &v1.WorkerNATSConfig{
						URL:               "nats://nats:4222",
						CredentialsSecret: nil,
					},
				},
			},
			threads:   4,
			resources: corev1.ResourceRequirements{},
			validate: func(t *testing.T, container corev1.Container) {
				require.Len(t, container.Env, 2)
				require.Equal(t, "DLLAMA_ROLE", container.Env[0].Name)
				require.Equal(t, "NATS_URL", container.Env[1].Name)
			},
		},
		{
			name: "worker with empty resource requirements",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
				},
			},
			threads: 4,
			resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{},
				Limits:   corev1.ResourceList{},
			},
			validate: func(t *testing.T, container corev1.Container) {
				require.Empty(t, container.Resources.Requests)
				require.Empty(t, container.Resources.Limits)
			},
		},
		{
			name: "worker with partial resource requirements (only requests)",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
				},
			},
			threads: 4,
			resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: mustParseQuantity("512Mi"),
				},
			},
			validate: func(t *testing.T, container corev1.Container) {
				require.NotEmpty(t, container.Resources.Requests)
				require.Empty(t, container.Resources.Limits)
				requestMem := container.Resources.Requests[corev1.ResourceMemory]
				require.Equal(t, "512Mi", requestMem.String())
			},
		},
		{
			name: "worker with partial resource requirements (only limits)",
			worker: &v1.Worker{
				Spec: v1.WorkerSpec{
					Image: "ghcr.io/gorizond/dllama:latest",
				},
			},
			threads: 4,
			resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: mustParseQuantity("1Gi"),
				},
			},
			validate: func(t *testing.T, container corev1.Container) {
				require.Empty(t, container.Resources.Requests)
				require.NotEmpty(t, container.Resources.Limits)
				limitMem := container.Resources.Limits[corev1.ResourceMemory]
				require.Equal(t, "1Gi", limitMem.String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			handler := &workerHandler{}
			container := handler.workerContainer(tt.worker, tt.threads, tt.resources)
			tt.validate(t, container)
		})
	}
}

func mustParseQuantity(s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		panic(fmt.Sprintf("failed to parse quantity %q: %v", s, err))
	}
	return q
}
