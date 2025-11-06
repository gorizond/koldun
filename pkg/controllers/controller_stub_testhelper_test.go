package controllers

import (
	"context"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

func TestControllerStubTriggerChange(t *testing.T) {
	t.Parallel()

	stub := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{Group: v1.GroupName, Version: v1.Version, Kind: "Model"})
	model := &v1.Model{}
	var seenKey string

	stub.OnChange(context.Background(), "listener", func(key string, obj *v1.Model) (*v1.Model, error) {
		seenKey = key
		return obj, nil
	})

	result, err := stub.triggerChange("models/mistral", model)
	require.NoError(t, err)
	require.Same(t, model, result)
	require.Equal(t, "models/mistral", seenKey)
}

func TestControllerStubTriggerChangeWithoutHandler(t *testing.T) {
	t.Parallel()

	stub := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	result, err := stub.triggerChange("models/mistral", &v1.Model{})
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestControllerStubTriggerRemove(t *testing.T) {
	t.Parallel()

	stub := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	var removed bool
	stub.OnRemove(context.Background(), "listener", func(key string, obj *v1.Model) (*v1.Model, error) {
		removed = true
		return nil, nil
	})

	result, err := stub.triggerRemove("models/mistral", &v1.Model{})
	require.NoError(t, err)
	require.Nil(t, result)
	require.True(t, removed)

	// Without handler the helper should return zero values
	stub2 := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	result, err = stub2.triggerRemove("models/mistral", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestControllerStubNoopMethods(t *testing.T) {
	t.Parallel()

	gvk := schema.GroupVersionKind{Group: v1.GroupName, Version: v1.Version, Kind: "Model"}
	stub := newControllerStub[*v1.Model, *v1.ModelList](gvk)

	require.Nil(t, stub.Informer())
	require.Equal(t, gvk, stub.GroupVersionKind())
	stub.AddGenericHandler(context.Background(), "handler", nil)
	stub.AddGenericRemoveHandler(context.Background(), "handler", nil)
	require.Nil(t, stub.Updater())
	stub.Enqueue("ns", "name")
	stub.EnqueueAfter("ns", "name", time.Nanosecond)
	require.Nil(t, stub.Cache())

	created, err := stub.Create(&v1.Model{})
	require.NoError(t, err)
	require.Nil(t, created)

	updated, err := stub.Update(&v1.Model{})
	require.NoError(t, err)
	require.Nil(t, updated)

	status, err := stub.UpdateStatus(&v1.Model{})
	require.NoError(t, err)
	require.Nil(t, status)

	require.NoError(t, stub.Delete("ns", "name", nil))

	got, err := stub.Get("ns", "name", metav1.GetOptions{})
	require.NoError(t, err)
	require.Nil(t, got)

	list, err := stub.List("ns", metav1.ListOptions{})
	require.NoError(t, err)
	require.Nil(t, list)

	watcher, err := stub.Watch("ns", metav1.ListOptions{})
	require.NoError(t, err)
	require.Nil(t, watcher)

	patched, err := stub.Patch("ns", "name", types.MergePatchType, nil)
	require.NoError(t, err)
	require.Nil(t, patched)

	client, err := stub.WithImpersonation(rest.ImpersonationConfig{})
	require.NoError(t, err)
	require.Same(t, stub, client)

	require.Nil(t, stub.lastOnChange())
	require.Nil(t, stub.lastOnRemove())
}
