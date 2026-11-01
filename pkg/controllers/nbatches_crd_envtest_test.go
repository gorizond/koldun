package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	kubernetes "k8s.io/client-go/kubernetes"
)

// TestCRDPersistsNBatches guards the schema fields that carry the dllama
// prefill batch size from the Ingress (backend.nBatches) to the Root
// (spec.nBatches). A missing CRD property is silently pruned on write and the
// A/B knob would have no effect.
func TestCRDPersistsNBatches(t *testing.T) {
	if testEnvConfig == nil {
		t.Skip("envtest unavailable")
	}
	ctx := context.Background()
	kube, err := kubernetes.NewForConfig(testEnvConfig)
	require.NoError(t, err)
	dyn, err := dynamic.NewForConfig(testEnvConfig)
	require.NoError(t, err)

	namespace := "nbatches-crd-envtest"
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	})

	ingresses := dyn.Resource(schema.GroupVersionResource{Group: "koldun.gorizond.io", Version: "v1", Resource: "ingresses"}).Namespace(namespace)
	ingress := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "koldun.gorizond.io/v1",
		"kind":       "Ingress",
		"metadata":   map[string]interface{}{"name": "demo"},
		"spec": map[string]interface{}{
			"backend": map[string]interface{}{
				"image":       "root:test",
				"rootImage":   "root:test",
				"workerImage": "worker:test",
				"nBatches":    int64(128),
				"nats":        map[string]interface{}{"url": "nats://nats:4222"},
			},
			"route": map[string]interface{}{"host": "demo.example.com"},
		},
	}}
	created, err := ingresses.Create(ctx, ingress, metav1.CreateOptions{})
	require.NoError(t, err)
	got, _, err := unstructured.NestedInt64(created.Object, "spec", "backend", "nBatches")
	require.NoError(t, err)
	require.Equal(t, int64(128), got, "Ingress CRD must persist backend.nBatches")

	roots := dyn.Resource(schema.GroupVersionResource{Group: "koldun.gorizond.io", Version: "v1", Resource: "roots"}).Namespace(namespace)
	root := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "koldun.gorizond.io/v1",
		"kind":       "Root",
		"metadata":   map[string]interface{}{"name": "demo-root"},
		"spec": map[string]interface{}{
			"modelRef": "model-pvc",
			"image":    "root:test",
			"nBatches": int64(128),
		},
	}}
	createdRoot, err := roots.Create(ctx, root, metav1.CreateOptions{})
	require.NoError(t, err)
	gotRoot, _, err := unstructured.NestedInt64(createdRoot.Object, "spec", "nBatches")
	require.NoError(t, err)
	require.Equal(t, int64(128), gotRoot, "Root CRD must persist spec.nBatches")
}
