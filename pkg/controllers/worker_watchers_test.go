package controllers

import (
	"errors"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerHandlerOnRelatedStatefulSet(t *testing.T) {
	t.Parallel()

	t.Run("enqueues worker by label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent:  componentWorker,
					labelWorkerName: "mistral-workers",
				},
			},
		}

		workers.EXPECT().Enqueue("models", "mistral-workers")

		result, err := handler.onRelatedStatefulSet("models/mistral-workers", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("ignores non-worker statefulset", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels:    map[string]string{},
			},
		}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedStatefulSet("models/mistral-workers", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("ignores worker statefulset without name label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentWorker,
				},
			},
		}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedStatefulSet("models/mistral-workers", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("returns nil for nil object", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedStatefulSet("models/mistral-workers", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestWorkerHandlerOnRelatedService(t *testing.T) {
	t.Parallel()

	t.Run("enqueues worker by label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent:  componentWorker,
					labelWorkerName: "mistral-workers",
				},
			},
		}

		workers.EXPECT().Enqueue("models", "mistral-workers")

		result, err := handler.onRelatedService("models/mistral-workers", service)
		require.NoError(t, err)
		require.Equal(t, service, result)
	})

	t.Run("ignores non-worker service", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels:    map[string]string{},
			},
		}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedService("models/mistral-workers", service)
		require.NoError(t, err)
		require.Equal(t, service, result)
	})

	t.Run("ignores worker service without name label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentWorker,
				},
			},
		}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedService("models/mistral-workers", service)
		require.NoError(t, err)
		require.Equal(t, service, result)
	})

	t.Run("returns nil for nil object", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedService("models/mistral-workers", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestWorkerHandlerOnRelatedDllama(t *testing.T) {
	t.Parallel()

	t.Run("enqueues all workers sorted by name", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)

		handler := &workerHandler{workers: workers}

		workers.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("models", gomock.Any()).Return([]*v1.Worker{
			{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "beta"}},
			{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "alpha"}},
		}, nil)

		gomock.InOrder(
			workers.EXPECT().Enqueue("models", "alpha"),
			workers.EXPECT().Enqueue("models", "beta"),
		)

		dllama := &v1.Dllama{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Name:      "mistral",
			},
		}

		result, err := handler.onRelatedDllama("models/mistral", dllama)
		require.NoError(t, err)
		require.Equal(t, dllama, result)
	})

	t.Run("propagates list error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)

		handler := &workerHandler{workers: workers}

		workers.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("models", gomock.Any()).Return(nil, errors.New("boom"))

		dllama := &v1.Dllama{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Name:      "mistral",
			},
		}

		result, err := handler.onRelatedDllama("models/mistral", dllama)
		require.Error(t, err)
		require.Equal(t, dllama, result)
	})

	t.Run("returns nil when object missing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		handler := &workerHandler{workers: workers}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedDllama("models/mistral", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestWorkerHandlerOnRelatedModel(t *testing.T) {
	t.Parallel()

	t.Run("enqueues workers for referencing dllamas", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

		handler := &workerHandler{
			workers: workers,
			dllamas: dllamas,
		}

		dllamas.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("", gomock.Any()).Return([]*v1.Dllama{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "models",
					Name:      "mistral",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "mistral",
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "models",
					Name:      "other",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "unrelated",
					},
				},
			},
		}, nil)

		workers.EXPECT().Enqueue("models", "mistral-workers")

		model := &v1.Model{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Name:      "mistral",
			},
		}

		result, err := handler.onRelatedModel("models/mistral", model)
		require.NoError(t, err)
		require.Equal(t, model, result)
	})

	t.Run("propagates listing errors", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

		handler := &workerHandler{
			workers: workers,
			dllamas: dllamas,
		}

		dllamas.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("", gomock.Any()).Return(nil, errors.New("boom"))

		result, err := handler.onRelatedModel("models/mistral", nil)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("enqueues via key when object missing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

		handler := &workerHandler{
			workers: workers,
			dllamas: dllamas,
		}

		dllamas.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("", gomock.Any()).Return([]*v1.Dllama{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "models",
					Name:      "mistral",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "mistral",
					},
				},
			},
		}, nil)

		workers.EXPECT().Enqueue("models", "mistral-workers")

		result, err := handler.onRelatedModel("models/mistral", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("ignores invalid key", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		workers := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

		handler := &workerHandler{
			workers: workers,
			dllamas: dllamas,
		}

		workers.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedModel("invalid-key", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}
