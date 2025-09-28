package koldun

import (
	"context"
	"fmt"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var modelGVR = schema.GroupVersionResource{
	Group:    v1.GroupName,
	Version:  v1.Version,
	Resource: "models",
}

// ModelClient wraps access to Model custom resources using the dynamic client.
type ModelClient struct {
	resource dynamic.NamespaceableResourceInterface
}

// NewModelClient creates a Model client using the supplied Kubernetes REST config.
func NewModelClient(cfg *rest.Config) (*ModelClient, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	return &ModelClient{resource: dyn.Resource(modelGVR)}, nil
}

// List returns all Model resources within the supplied namespace.
// When namespace is empty, resources from all namespaces are returned.
func (c *ModelClient) List(ctx context.Context, namespace string) ([]v1.Model, error) {
	var (
		list *unstructured.UnstructuredList
		err  error
	)
	if namespace == "" {
		list, err = c.resource.List(ctx, metav1.ListOptions{})
	} else {
		list, err = c.resource.Namespace(namespace).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("list models: %w", err)
	}

	items := make([]v1.Model, 0, len(list.Items))
	for i := range list.Items {
		obj := list.Items[i]
		model, err := toModel(&obj)
		if err != nil {
			return nil, fmt.Errorf("convert model %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		}
		items = append(items, *model)
	}
	return items, nil
}

// Get returns a single Model resource by namespace and name.
func (c *ModelClient) Get(ctx context.Context, namespace, name string) (*v1.Model, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required for Model lookup")
	}
	u, err := c.resource.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get model %s/%s: %w", namespace, name, err)
	}
	return toModel(u)
}

func toModel(u *unstructured.Unstructured) (*v1.Model, error) {
	model := new(v1.Model)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, model); err != nil {
		return nil, err
	}
	return model, nil
}
