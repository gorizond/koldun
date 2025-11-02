package controllers

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func TestDllamaReconciliationCreatesRootAndWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest integration in short mode")
	}
	if reason := envtestSkipReason; reason != "" {
		t.Skip(reason)
	}
	if testEnvConfig == nil {
		t.Skip("envtest assets unavailable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager, err := NewManager(testEnvConfig)
	require.NoError(t, err)
	// Disable bucket provisioning to keep the test cluster lean.
	manager.SetEnsureObjectStorageBuckets(false)

	require.NoError(t, registerDllamaController(ctx, manager))

	errChan := make(chan error, 1)
	go func() {
		errChan <- manager.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errChan:
			if err != nil && ctx.Err() == nil {
				t.Fatalf("manager.Start failed: %v", err)
			}
		case <-time.After(2 * time.Second):
			// Manager shutdown delays should not fail the test but are worth logging.
			t.Log("manager shutdown exceeded 2s grace period")
		}
	})

	require.Eventually(t, func() bool {
		return manager.Health().Ready()
	}, 15*time.Second, 100*time.Millisecond, "manager never reported ready")

	kube, err := kubernetes.NewForConfig(testEnvConfig)
	require.NoError(t, err)

	const (
		namespace   = "dllama-test"
		modelName   = "mistral"
		dllamaName  = "demo-dllama"
		outputPVC   = "mistral-output"
		rootImage   = "root:latest"
		workerImage = "worker:latest"
	)

	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	model := &v1.Model{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Model",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      modelName,
			Namespace: namespace,
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/mistral",
			LocalPath: "/models/mistral",
		},
		Status: v1.ModelStatus{
			OutputPVCName: outputPVC,
		},
	}

	createdModel, err := manager.Kold.Model().Create(model)
	require.NoError(t, err)

	createdModel.Status.OutputPVCName = outputPVC
	_, err = manager.Kold.Model().UpdateStatus(createdModel)
	require.NoError(t, err)

	dllama := &v1.Dllama{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Dllama",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dllamaName,
			Namespace: namespace,
		},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{
				Kind: "Model",
				Name: modelName,
			},
			ReplicaPower: 1,
			RootImage:    rootImage,
			WorkerImage:  workerImage,
			NATS: &v1.DllamaNATSConfig{
				URL: "nats://demo:4222",
			},
		},
	}

	_, err = manager.Kold.Dllama().Create(dllama)
	require.NoError(t, err)

	expectedRootName := fmt.Sprintf("%s-root", dllamaName)
	require.Eventually(t, func() bool {
		root, getErr := manager.Kold.Root().Get(namespace, expectedRootName, metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		return root.Spec.ModelRef == outputPVC &&
			root.Spec.Image == rootImage &&
			root.Spec.WorkerSelector[labelDllamaName] == dllamaName
	}, 20*time.Second, 200*time.Millisecond, "root resource was not reconciled as expected")

	expectedWorkerName := workerResourceName(dllamaName)
	require.Eventually(t, func() bool {
		worker, getErr := manager.Kold.Worker().Get(namespace, expectedWorkerName, metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		return worker.Spec.ModelRef == outputPVC &&
			worker.Spec.Image == workerImage &&
			worker.Spec.RootRef == expectedRootName &&
			worker.Spec.NATS != nil &&
			worker.Spec.NATS.URL == "nats://demo:4222"
	}, 20*time.Second, 200*time.Millisecond, "worker resource was not reconciled as expected")
}
