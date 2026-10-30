package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes"
)

// TestStatefulSetPodManagementPolicyImmutable documents the Kubernetes rule
// that forces the controller to roll the group instead of applying an updated
// podManagementPolicy over an existing StatefulSet.
func TestStatefulSetPodManagementPolicyImmutable(t *testing.T) {
	if testEnvConfig == nil {
		t.Skip("envtest unavailable")
	}
	ctx := context.Background()
	kube, err := kubernetes.NewForConfig(testEnvConfig)
	require.NoError(t, err)

	namespace := "worker-policy-envtest"
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	})

	labels := map[string]string{"app": "dllama"}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-workers", Namespace: namespace},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "demo-workers",
			Replicas:    int32Ptr(1),
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "worker",
					Image: "example.invalid/worker:test",
				}}},
			},
		},
	}
	created, err := kube.AppsV1().StatefulSets(namespace).Create(ctx, sts, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, appsv1.OrderedReadyPodManagement, created.Spec.PodManagementPolicy)

	created.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
	_, err = kube.AppsV1().StatefulSets(namespace).Update(ctx, created, metav1.UpdateOptions{})
	require.Error(t, err)
	require.True(t, apierrors.IsInvalid(err), "expected immutable-field validation, got %v", err)
}
