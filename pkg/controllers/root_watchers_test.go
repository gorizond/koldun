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

func TestRootHandlerOnRelatedService(t *testing.T) {
	t.Parallel()

	t.Run("enqueues root service", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentRoot,
					labelRootName:  "mistral-root",
				},
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedService("models/mistral-root", service)
		require.NoError(t, err)
		require.Equal(t, service, result)
	})

	t.Run("ignores unrelated service", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentWorker,
				},
			},
		}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedService("models/random", service)
		require.NoError(t, err)
		require.Equal(t, service, result)
	})

	t.Run("nil service returns nil", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedService("models/mistral-root", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("root component without name label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentRoot,
				},
			},
		}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedService("models/mistral-root", service)
		require.NoError(t, err)
		require.Same(t, service, result)
	})
}

func TestRootHandlerOnRelatedStatefulSet(t *testing.T) {
	t.Parallel()

	t.Run("root statefulset enqueues by root label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentRoot,
					labelRootName:  "mistral-root",
				},
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedStatefulSet("models/mistral-root", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("worker statefulset enqueues derived root", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent:  componentWorker,
					labelDllamaName: "mistral",
				},
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedStatefulSet("models/mistral-worker", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("root statefulset falls back to object name", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Name:      "mistral-root",
				Labels: map[string]string{
					labelComponent: componentRoot,
				},
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedStatefulSet("models/mistral-root", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("worker statefulset without dllama label ignored", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentWorker,
				},
			},
		}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedStatefulSet("models/mistral-worker", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("ignores unrelated component", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelComponent: componentBackend,
				},
			},
		}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedStatefulSet("models/backend", sts)
		require.NoError(t, err)
		require.Equal(t, sts, result)
	})

	t.Run("nil object returns nil", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedStatefulSet("models/mistral-root", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestRootHandlerOnRelatedWorker(t *testing.T) {
	t.Parallel()

	t.Run("prefers explicit root reference", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		worker := &v1.Worker{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
			},
			Spec: v1.WorkerSpec{
				RootRef: "  mistral-root  ",
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedWorker("models/mistral-worker", worker)
		require.NoError(t, err)
		require.Equal(t, worker, result)
	})

	t.Run("falls back to dllama label", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		worker := &v1.Worker{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Labels: map[string]string{
					labelDllamaName: "mistral",
				},
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedWorker("models/mistral-worker", worker)
		require.NoError(t, err)
		require.Equal(t, worker, result)
	})

	t.Run("ignores worker without references", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		worker := &v1.Worker{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
			},
		}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedWorker("models/mistral-worker", worker)
		require.NoError(t, err)
		require.Equal(t, worker, result)
	})

	t.Run("nil worker returns nil", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedWorker("models/mistral-worker", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestRootHandlerOnRelatedDllama(t *testing.T) {
	t.Parallel()

	t.Run("enqueues derived root name", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		dllama := &v1.Dllama{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "models",
				Name:      "mistral",
			},
		}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedDllama("ignored", dllama)
		require.NoError(t, err)
		require.Equal(t, dllama, result)
	})

	t.Run("falls back to key when object nil", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		roots.EXPECT().Enqueue("models", "mistral-root")

		result, err := handler.onRelatedDllama("models/mistral", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("ignores invalid key", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		handler := &rootHandler{roots: roots}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedDllama("invalid-key", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestRootHandlerOnRelatedModel(t *testing.T) {
	t.Parallel()

	t.Run("enqueues roots for referencing dllamas", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

		handler := &rootHandler{
			roots:   roots,
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
					Name:      "ignored",
				},
				Spec: v1.DllamaSpec{
					ModelRef: v1.ModelReference{
						Kind: "Model",
						Name: "other",
					},
				},
			},
		}, nil)

		roots.EXPECT().Enqueue("models", "mistral-root")

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

	t.Run("returns error when listing dllamas fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

		handler := &rootHandler{
			roots:   roots,
			dllamas: dllamas,
		}

		dllamas.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("", gomock.Any()).Return(nil, errors.New("boom"))

		result, err := handler.onRelatedModel("models/mistral", nil)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("ignores invalid keys", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

		handler := &rootHandler{
			roots:   roots,
			dllamas: dllamas,
		}

		roots.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

		result, err := handler.onRelatedModel("invalid", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}
