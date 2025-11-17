package controllers

import (
	"fmt"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDesiredWorkersFallsBackToIngressNATS(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{}, nil)
	ingressCache.EXPECT().List("", gomock.Any()).Return([]*v1.Ingress{{
		Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{NATS: v1.IngressNATSConfig{URL: "nats://shared:4222"}}},
	}}, nil)

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Labels: map[string]string{
				labelConversationHash: "hash-from-label",
				labelSessionName:      "session-name",
			},
		},
		Spec: v1.DllamaSpec{
			WorkerImage:  "ghcr.io/example/worker:latest",
			ReplicaPower: 1, // Enable workers (non-zero)
		},
	}

	model := &v1.Model{Status: v1.ModelStatus{OutputPVCName: "model-pvc"}}

	workers := handler.desiredWorkers(dllama, model)
	require.Len(t, workers, 1)
	worker := workers[0]

	require.NotNil(t, worker.Spec.NATS, "worker should inherit NATS config from ingress fallback")
	require.Equal(t, "nats://shared:4222", worker.Spec.NATS.URL)
	require.Equal(t, fmt.Sprintf("%s-root", dllama.Name), worker.Spec.RootRef)
	require.Equal(t, sanitizeLabelValue(dllama.Labels[labelSessionName]), worker.Labels[labelSessionName])
	require.Equal(t, "hash-from-label", worker.Annotations[labelConversationHash])
}

func TestDesiredWorkersUsesExistingNATSConfigWhenURLPresent(t *testing.T) {

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: v1.DllamaSpec{
			WorkerImage:  "ghcr.io/example/worker:latest",
			ReplicaPower: 1, // Enable workers (non-zero)
			NATS: &v1.DllamaNATSConfig{
				URL: "nats://explicit:4222",
			},
		},
	}

	model := &v1.Model{Status: v1.ModelStatus{OutputPVCName: "model-pvc"}}

	handler := &dllamaHandler{}
	workers := handler.desiredWorkers(dllama, model)
	require.Len(t, workers, 1)
	worker := workers[0]

	require.NotNil(t, worker.Spec.NATS)
	require.Equal(t, "nats://explicit:4222", worker.Spec.NATS.URL)
}

func TestDesiredWorkersPopulatesAnnotationsFromIngressWhenAnnotationsEmpty(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{{
		Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{NATS: v1.IngressNATSConfig{URL: "nats://ns-only:4222"}}},
	}}, nil)

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Labels: map[string]string{
				labelConversationHash: "label-hash",
			},
		},
		Spec: v1.DllamaSpec{
			WorkerImage:  "ghcr.io/example/worker:latest",
			ReplicaPower: 1, // Enable workers (non-zero)
			NATS:         &v1.DllamaNATSConfig{},
		},
	}

	model := &v1.Model{Status: v1.ModelStatus{OutputPVCName: "model-pvc"}}

	workers := handler.desiredWorkers(dllama, model)
	require.Len(t, workers, 1)
	worker := workers[0]

	require.Equal(t, "label-hash", worker.Annotations[labelConversationHash])
	require.Equal(t, "nats://ns-only:4222", worker.Spec.NATS.URL)
}
