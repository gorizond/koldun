package controllers

import (
	"errors"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDllamaHandlerOnRelatedRoot(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	handler := &dllamaHandler{dllamas: dllamas}

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "mistral",
			},
		},
	}

	dllamas.EXPECT().Enqueue("models", "mistral")

	result, err := handler.onRelatedRoot("models/mistral-root", root)
	require.NoError(t, err)
	require.Equal(t, root, result)
}

func TestDllamaHandlerOnRelatedRootNil(t *testing.T) {
	t.Parallel()

	handler := &dllamaHandler{}
	result, err := handler.onRelatedRoot("models/mistral-root", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestDllamaHandlerOnRelatedWorker(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	handler := &dllamaHandler{dllamas: dllamas}

	worker := &v1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "mistral",
			},
		},
	}

	dllamas.EXPECT().Enqueue("models", "mistral")

	result, err := handler.onRelatedWorker("models/mistral-worker", worker)
	require.NoError(t, err)
	require.Equal(t, worker, result)
}

func TestDllamaHandlerOnRelatedWorkerNil(t *testing.T) {
	t.Parallel()

	handler := &dllamaHandler{}
	result, err := handler.onRelatedWorker("models/mistral-worker", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestDllamaHandlerOnRelatedStatefulSet(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	handler := &dllamaHandler{dllamas: dllamas}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Labels: map[string]string{
				labelDllamaName: "mistral",
			},
		},
	}

	dllamas.EXPECT().Enqueue("models", "mistral")

	result, err := handler.onRelatedStatefulSet("models/mistral-root", sts)
	require.NoError(t, err)
	require.Equal(t, sts, result)
}

func TestDllamaHandlerOnRelatedStatefulSetMissingLabel(t *testing.T) {
	t.Parallel()

	handler := &dllamaHandler{}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
		},
	}

	result, err := handler.onRelatedStatefulSet("models/mistral", sts)
	require.NoError(t, err)
	require.Equal(t, sts, result)
}

func TestDllamaHandlerOnRelatedStatefulSetNil(t *testing.T) {
	t.Parallel()

	handler := &dllamaHandler{}
	result, err := handler.onRelatedStatefulSet("models/mistral", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestDllamaHandlerOnRelatedModel(t *testing.T) {
	t.Parallel()

	t.Run("enqueues referenced dllamas", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		handler := &dllamaHandler{dllamas: dllamas}

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
		dllamas.EXPECT().Enqueue("models", "mistral")

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

	t.Run("handles nil object via key when list fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
		cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
		handler := &dllamaHandler{dllamas: dllamas}

		dllamas.EXPECT().Cache().Return(cache)
		cache.EXPECT().List("", gomock.Any()).Return(nil, errors.New("boom"))

		result, err := handler.onRelatedModel("models/mistral", nil)
		require.Error(t, err)
		require.Nil(t, result)
	})
}

func TestDllamaHandlerOnRelatedModelIgnoresInvalidKey(t *testing.T) {
	t.Parallel()

	handler := &dllamaHandler{}
	result, err := handler.onRelatedModel("garbage-key", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestDllamaHandlerOnRelatedIngress(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	handler := &dllamaHandler{dllamas: dllamas}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("", gomock.Any()).Return([]*v1.Dllama{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "alpha"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "beta"}},
	}, nil)
	dllamas.EXPECT().Enqueue("models", "alpha")
	dllamas.EXPECT().Enqueue("models", "beta")

	ing := &v1.Ingress{}

	result, err := handler.onRelatedIngress("ignored", ing)
	require.NoError(t, err)
	require.Equal(t, ing, result)
}

func TestDllamaHandlerOnRelatedIngressError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	handler := &dllamaHandler{dllamas: dllamas}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("", gomock.Any()).Return(nil, errors.New("list failure"))

	ing := &v1.Ingress{}

	result, err := handler.onRelatedIngress("ignored", ing)
	require.Error(t, err)
	require.Equal(t, ing, result)
}
