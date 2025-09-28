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

var tokenGVR = schema.GroupVersionResource{
	Group:    v1.GroupName,
	Version:  v1.Version,
	Resource: "tokens",
}

// TokenClient wraps access to Token custom resources using the dynamic client.
type TokenClient struct {
	resource dynamic.NamespaceableResourceInterface
}

// NewTokenClient creates a Token client using the supplied Kubernetes REST config.
func NewTokenClient(cfg *rest.Config) (*TokenClient, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	return &TokenClient{resource: dyn.Resource(tokenGVR)}, nil
}

// List returns all Token resources within the supplied namespace.
// When namespace is empty, resources from all namespaces are returned.
func (c *TokenClient) List(ctx context.Context, namespace string) ([]v1.Token, error) {
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
		return nil, fmt.Errorf("list tokens: %w", err)
	}

	items := make([]v1.Token, 0, len(list.Items))
	for i := range list.Items {
		obj := list.Items[i]
		token, err := toToken(&obj)
		if err != nil {
			return nil, fmt.Errorf("convert token %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		}
		items = append(items, *token)
	}
	return items, nil
}

// Get returns a single Token resource by namespace and name.
func (c *TokenClient) Get(ctx context.Context, namespace, name string) (*v1.Token, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required for Token lookup")
	}
	u, err := c.resource.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get token %s/%s: %w", namespace, name, err)
	}
	return toToken(u)
}

func toToken(u *unstructured.Unstructured) (*v1.Token, error) {
	token := new(v1.Token)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, token); err != nil {
		return nil, err
	}
	return token, nil
}
