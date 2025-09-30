package koldun

import (
	"context"
	"fmt"
	"strings"

	"github.com/gorizond/koldun/pkg/registry"
	"github.com/gorizond/koldun/pkg/tokens"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// TokenClient wraps access to Secrets that model Koldun API tokens.
type TokenClient struct {
	client kubernetes.Interface
}

// NewTokenClient creates a token Secret client using the supplied Kubernetes REST config.
func NewTokenClient(cfg *rest.Config) (*TokenClient, error) {
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	return &TokenClient{client: client}, nil
}

// List returns all token Secrets within the supplied namespace.
// When namespace is empty, Secrets from all namespaces are returned.
func (c *TokenClient) List(ctx context.Context, namespace string) ([]registry.Token, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("token client not initialised")
	}

	ns := namespace
	if strings.TrimSpace(ns) == "" {
		ns = metav1.NamespaceAll
	}

	secrets, err := c.client.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{LabelSelector: tokens.LabelToken})
	if err != nil {
		return nil, fmt.Errorf("list token secrets: %w", err)
	}

	items := make([]registry.Token, 0, len(secrets.Items))
	for i := range secrets.Items {
		secret := secrets.Items[i]
		if !tokens.IsTokenSecret(&secret) {
			continue
		}
		entry, err := tokens.ExtractRegistryToken(&secret)
		if err != nil {
			return nil, fmt.Errorf("convert token secret %s/%s: %w", secret.Namespace, secret.Name, err)
		}
		items = append(items, *entry)
	}
	return items, nil
}

// Get returns a single token Secret by namespace and name.
func (c *TokenClient) Get(ctx context.Context, namespace, name string) (*registry.Token, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("token client not initialised")
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, fmt.Errorf("namespace is required for token Secret lookup")
	}

	secret, err := c.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get token secret %s/%s: %w", namespace, name, err)
	}
	if !tokens.IsTokenSecret(secret) {
		return nil, fmt.Errorf("secret %s/%s is not labelled as a koldun token", namespace, name)
	}
	entry, err := tokens.ExtractRegistryToken(secret)
	if err != nil {
		return nil, fmt.Errorf("convert token secret %s/%s: %w", namespace, name, err)
	}
	return entry, nil
}
