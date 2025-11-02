package controllers

import (
	"context"
	"time"

	"github.com/rancher/wrangler/v3/pkg/generic"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// controllerStub implements generic.ControllerInterface for tests and captures registered handlers.
type controllerStub[T generic.RuntimeMetaObject, TL runtime.Object] struct {
	gvk      schema.GroupVersionKind
	onChange generic.ObjectHandler[T]
	onRemove generic.ObjectHandler[T]
}

func newControllerStub[T generic.RuntimeMetaObject, TL runtime.Object](gvk schema.GroupVersionKind) *controllerStub[T, TL] {
	return &controllerStub[T, TL]{gvk: gvk}
}

func (c *controllerStub[T, TL]) Informer() cache.SharedIndexInformer { return nil }

func (c *controllerStub[T, TL]) GroupVersionKind() schema.GroupVersionKind { return c.gvk }

func (c *controllerStub[T, TL]) AddGenericHandler(context.Context, string, generic.Handler) {}

func (c *controllerStub[T, TL]) AddGenericRemoveHandler(context.Context, string, generic.Handler) {}

func (c *controllerStub[T, TL]) Updater() generic.Updater { return nil }

func (c *controllerStub[T, TL]) OnChange(_ context.Context, _ string, handler generic.ObjectHandler[T]) {
	c.onChange = handler
}

func (c *controllerStub[T, TL]) OnRemove(_ context.Context, _ string, handler generic.ObjectHandler[T]) {
	c.onRemove = handler
}

func (c *controllerStub[T, TL]) Enqueue(string, string) {}

func (c *controllerStub[T, TL]) EnqueueAfter(string, string, time.Duration) {}

func (c *controllerStub[T, TL]) Cache() generic.CacheInterface[T] { return nil }

func (c *controllerStub[T, TL]) Create(T) (T, error) {
	var zero T
	return zero, nil
}

func (c *controllerStub[T, TL]) Update(T) (T, error) {
	var zero T
	return zero, nil
}

func (c *controllerStub[T, TL]) UpdateStatus(T) (T, error) {
	var zero T
	return zero, nil
}

func (c *controllerStub[T, TL]) Delete(string, string, *metav1.DeleteOptions) error { return nil }

func (c *controllerStub[T, TL]) Get(string, string, metav1.GetOptions) (T, error) {
	var zero T
	return zero, nil
}

func (c *controllerStub[T, TL]) List(string, metav1.ListOptions) (TL, error) {
	var zero TL
	return zero, nil
}

func (c *controllerStub[T, TL]) Watch(string, metav1.ListOptions) (watch.Interface, error) { return nil, nil }

func (c *controllerStub[T, TL]) Patch(string, string, types.PatchType, []byte, ...string) (T, error) {
	var zero T
	return zero, nil
}

func (c *controllerStub[T, TL]) WithImpersonation(rest.ImpersonationConfig) (generic.ClientInterface[T, TL], error) {
	return c, nil
}

func (c *controllerStub[T, TL]) lastOnChange() generic.ObjectHandler[T] { return c.onChange }

func (c *controllerStub[T, TL]) lastOnRemove() generic.ObjectHandler[T] { return c.onRemove }

func (c *controllerStub[T, TL]) triggerChange(key string, obj T) (T, error) {
	if c.onChange == nil {
		var zero T
		return zero, nil
	}
	return c.onChange(key, obj)
}

func (c *controllerStub[T, TL]) triggerRemove(key string, obj T) (T, error) {
	if c.onRemove == nil {
		var zero T
		return zero, nil
	}
	return c.onRemove(key, obj)
}
