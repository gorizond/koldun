package controllers

import (
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/stretchr/testify/require"
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
