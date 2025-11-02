package controllers

import (
	"context"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	appscb "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestRegisterDllamaControllerRegistersHandlers(t *testing.T) {
	dllamaCtrl := newControllerStub[*v1.Dllama, *v1.DllamaList](schema.GroupVersionKind{})
	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	rootCtrl := newControllerStub[*v1.Root, *v1.RootList](schema.GroupVersionKind{})
	workerCtrl := newControllerStub[*v1.Worker, *v1.WorkerList](schema.GroupVersionKind{})
	ingressCtrl := newControllerStub[*v1.Ingress, *v1.IngressList](schema.GroupVersionKind{})
	statefulsetCtrl := newControllerStub[*appsv1.StatefulSet, *appsv1.StatefulSetList](schema.GroupVersionKind{})

	mgr := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			dllama:  dllamaCtrl,
			model:   modelCtrl,
			root:    rootCtrl,
			worker:  workerCtrl,
			ingress: ingressCtrl,
		},
		Apps: &dllamaAppsStub{statefulset: statefulsetCtrl},
	}

	require.NoError(t, registerDllamaController(context.Background(), mgr))

	require.NotNil(t, dllamaCtrl.lastOnChange())
	require.NotNil(t, dllamaCtrl.lastOnRemove())
	require.NotNil(t, modelCtrl.lastOnChange())
	require.NotNil(t, rootCtrl.lastOnChange())
	require.NotNil(t, workerCtrl.lastOnChange())
	require.NotNil(t, ingressCtrl.lastOnChange())
	require.NotNil(t, statefulsetCtrl.lastOnChange())
}

type dllamaAppsStub struct {
	statefulset appscb.StatefulSetController
}

func (d *dllamaAppsStub) DaemonSet() appscb.DaemonSetController { return nil }

func (d *dllamaAppsStub) Deployment() appscb.DeploymentController { return nil }

func (d *dllamaAppsStub) StatefulSet() appscb.StatefulSetController { return d.statefulset }
