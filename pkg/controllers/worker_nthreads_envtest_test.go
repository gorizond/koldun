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

// TestWorkerCRDPersistsNThreads guards the schema field that carries
// backend.nThreads into worker pods; a missing CRD property is pruned on
// write and the worker silently falls back to --nthreads 1.
func TestWorkerCRDPersistsNThreads(t *testing.T) {
	if testEnvConfig == nil {
		t.Skip("envtest unavailable")
	}
	ctx := context.Background()
	kube, err := kubernetes.NewForConfig(testEnvConfig)
	require.NoError(t, err)
	dyn, err := dynamic.NewForConfig(testEnvConfig)
	require.NoError(t, err)

	namespace := "worker-nthreads-envtest"
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	})

	workers := dyn.Resource(schema.GroupVersionResource{Group: "koldun.gorizond.io", Version: "v1", Resource: "workers"}).Namespace(namespace)
	worker := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "koldun.gorizond.io/v1",
		"kind":       "Worker",
		"metadata":   map[string]interface{}{"name": "demo-workers"},
		"spec": map[string]interface{}{
			"modelRef": "model-pvc",
			"image":    "worker:test",
			"rootRef":  "demo-root",
			"slot":     int64(0),
			"nThreads": int64(4),
		},
	}}
	created, err := workers.Create(ctx, worker, metav1.CreateOptions{})
	require.NoError(t, err)
	got, _, err := unstructured.NestedInt64(created.Object, "spec", "nThreads")
	require.NoError(t, err)
	require.Equal(t, int64(4), got, "worker CRD must persist spec.nThreads")
}
