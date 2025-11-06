package controllers

import (
	"errors"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDllamaHandlerOnChangeNilObject(t *testing.T) {
	handler := &dllamaHandler{}
	obj, err := handler.onChange("ns/name", nil)
	require.NoError(t, err)
	require.Nil(t, obj)
}

func TestDllamaHandlerOnChangeDeletionTimestamp(t *testing.T) {
	handler := &dllamaHandler{}
	ts := metav1.NewTime(time.Now())
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sample",
			Namespace:         "default",
			DeletionTimestamp: &ts,
		},
	}

	obj, err := handler.onChange("default/sample", dllama)
	require.NoError(t, err)
	require.Equal(t, dllama, obj)
}

func TestDllamaHandlerOnChangePropagatesTopologyError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &dllamaHandler{
		apply:  &fakeapply.FakeApply{},
		models: modelsController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	expectedErr := errors.New("lookup failed")
	modelsCache.EXPECT().Get("default", "demo-model").Return((*v1.Model)(nil), expectedErr)

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "default",
		},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{
				Kind: "Model",
				Name: "demo-model",
			},
		},
	}

	obj, err := handler.onChange("default/sample", dllama)
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, dllama, obj)
}

func TestDllamaHandlerOnChangeUpdatesStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		apply:   &fakeapply.FakeApply{},
		dllamas: dllamasController,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "default",
		},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{
				Kind: "Service",
			},
		},
	}

	dllamasController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).Return(dllama, nil)

	obj, err := handler.onChange("default/sample", dllama)
	require.NoError(t, err)
	require.Equal(t, dllama, obj)
}

func TestDllamaHandlerOnRemoveReturnsObject(t *testing.T) {
	handler := &dllamaHandler{}
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "default",
		},
	}

	obj, err := handler.onRemove("default/sample", dllama)
	require.NoError(t, err)
	require.Equal(t, dllama, obj)
}
