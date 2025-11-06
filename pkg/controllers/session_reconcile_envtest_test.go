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

func TestSessionReconciliationCreatesDllamaAndDispatcher(t *testing.T) {
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
	manager.SetEnsureObjectStorageBuckets(false)

	require.NoError(t, registerSessionController(ctx, manager))
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
			t.Log("manager shutdown exceeded 2s grace period")
		}
	})

	require.Eventually(t, func() bool {
		return manager.Health().Ready()
	}, 15*time.Second, 100*time.Millisecond, "manager never reported ready")

	kube, err := kubernetes.NewForConfig(testEnvConfig)
	require.NoError(t, err)

	const (
		namespace   = "session-envtest"
		modelName   = "mistral"
		sessionName = "chat-demo"
		outputPVC   = "mistral-output"
	)

	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	require.NoError(t, err)

	model := &v1.Model{
		TypeMeta: metav1.TypeMeta{APIVersion: v1.SchemeGroupVersion.String(), Kind: "Model"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      modelName,
			Namespace: namespace,
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model",
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

	session := &v1.Session{
		TypeMeta: metav1.TypeMeta{APIVersion: v1.SchemeGroupVersion.String(), Kind: "Session"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sessionName,
			Namespace: namespace,
		},
		Spec: v1.SessionSpec{
			Hash:            "hash-abc",
			ModelRef:        v1.ModelReference{Kind: "Model", Name: modelName},
			RootImage:       "root:latest",
			WorkerImage:     "worker:latest",
			DispatcherImage: "dispatcher:latest",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dllama",
				StateStream:         "sessions.state",
			},
			NATS: &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	_, err = manager.Kold.Session().Create(session)
	require.NoError(t, err)

	var dllamaName string
	require.Eventually(t, func() bool {
		list, err := manager.Kold.Dllama().List(namespace, metav1.ListOptions{})
		if err != nil || len(list.Items) == 0 {
			return false
		}
		dllamaName = list.Items[0].Name
		return list.Items[0].Spec.ModelRef.Name == modelName
	}, 20*time.Second, 200*time.Millisecond, "session controller did not create dllama")

	expectedRootName := fmt.Sprintf("%s-root", dllamaName)
	require.Eventually(t, func() bool {
		root, err := manager.Kold.Root().Get(namespace, expectedRootName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return root.Spec.ModelRef == outputPVC
	}, 20*time.Second, 200*time.Millisecond, "dllama controller did not create root")

	expectedWorkerName := workerResourceName(dllamaName)
	require.Eventually(t, func() bool {
		worker, err := manager.Kold.Worker().Get(namespace, expectedWorkerName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return worker.Spec.RootRef == expectedRootName
	}, 20*time.Second, 200*time.Millisecond, "dllama controller did not create worker")

	require.Eventually(t, func() bool {
		deployment, err := kube.AppsV1().Deployments(namespace).Get(ctx, fmt.Sprintf("%s-dispatcher", sessionName), metav1.GetOptions{})
		if err != nil {
			return false
		}
		if len(deployment.Spec.Template.Spec.Containers) == 0 {
			return false
		}
		return deployment.Spec.Template.Spec.Containers[0].Image == "dispatcher:latest"
	}, 20*time.Second, 200*time.Millisecond, "dispatcher deployment was not created")
}
