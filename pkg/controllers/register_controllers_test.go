package controllers

import (
	"context"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	appscb "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	batchcb "github.com/rancher/wrangler/v3/pkg/generated/controllers/batch/v1"
	corecb "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestRegisterWorkerController(t *testing.T) {
	workerCtrl := newControllerStub[*v1.Worker, *v1.WorkerList](schema.GroupVersionKind{})
	dllamaCtrl := newControllerStub[*v1.Dllama, *v1.DllamaList](schema.GroupVersionKind{})
	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	statefulsetCtrl := newControllerStub[*appsv1.StatefulSet, *appsv1.StatefulSetList](schema.GroupVersionKind{})
	serviceCtrl := newControllerStub[*corev1.Service, *corev1.ServiceList](schema.GroupVersionKind{})

	mgr := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			worker: workerCtrl,
			dllama: dllamaCtrl,
			model:  modelCtrl,
		},
		Apps: &workerAppsStub{statefulset: statefulsetCtrl},
		Core: &workerCoreStub{service: serviceCtrl},
	}

	require.NoError(t, registerWorkerController(context.Background(), mgr))

	require.NotNil(t, workerCtrl.lastOnChange())
	require.NotNil(t, workerCtrl.lastOnRemove())
	require.NotNil(t, statefulsetCtrl.lastOnChange())
	require.NotNil(t, serviceCtrl.lastOnChange())
	require.NotNil(t, dllamaCtrl.lastOnChange())
	require.NotNil(t, modelCtrl.lastOnChange())
}

func TestRegisterRootController(t *testing.T) {
	rootCtrl := newControllerStub[*v1.Root, *v1.RootList](schema.GroupVersionKind{})
	dllamaCtrl := newControllerStub[*v1.Dllama, *v1.DllamaList](schema.GroupVersionKind{})
	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	statefulsetCtrl := newControllerStub[*appsv1.StatefulSet, *appsv1.StatefulSetList](schema.GroupVersionKind{})
	serviceCtrl := newControllerStub[*corev1.Service, *corev1.ServiceList](schema.GroupVersionKind{})

	mgr := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			root:   rootCtrl,
			dllama: dllamaCtrl,
			model:  modelCtrl,
		},
		Apps: &rootAppsStub{statefulset: statefulsetCtrl},
		Core: &rootCoreStub{service: serviceCtrl},
	}

	require.NoError(t, registerRootController(context.Background(), mgr))

	require.NotNil(t, rootCtrl.lastOnChange())
	require.NotNil(t, rootCtrl.lastOnRemove())
	require.NotNil(t, statefulsetCtrl.lastOnChange())
	require.NotNil(t, serviceCtrl.lastOnChange())
	require.NotNil(t, dllamaCtrl.lastOnChange())
	require.NotNil(t, modelCtrl.lastOnChange())
}

func TestRegisterModelController(t *testing.T) {
	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	jobCtrl := newControllerStub[*batchv1.Job, *batchv1.JobList](schema.GroupVersionKind{})
	pvcCtrl := newControllerStub[*corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaimList](schema.GroupVersionKind{})

	mgr := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			model: modelCtrl,
		},
		Batch: &modelBatchStub{
			job: jobCtrl,
		},
		Core: &modelCoreStub{
			pvc: pvcCtrl,
		},
		ensureObjectStorageBuckets: false,
	}

	require.NoError(t, registerModelController(context.Background(), mgr))

	require.NotNil(t, modelCtrl.lastOnChange())
	require.NotNil(t, modelCtrl.lastOnRemove())
	require.NotNil(t, jobCtrl.lastOnChange())
}

func TestRegisterSessionController(t *testing.T) {
	sessionCtrl := newControllerStub[*v1.Session, *v1.SessionList](schema.GroupVersionKind{})

	mgr := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			session: sessionCtrl,
		},
	}

	require.NoError(t, registerSessionController(context.Background(), mgr))

	require.NotNil(t, sessionCtrl.lastOnChange())
	require.NotNil(t, sessionCtrl.lastOnRemove())
}

// Test stubs for worker controller
type workerAppsStub struct {
	statefulset appscb.StatefulSetController
}

func (w *workerAppsStub) DaemonSet() appscb.DaemonSetController     { return nil }
func (w *workerAppsStub) Deployment() appscb.DeploymentController   { return nil }
func (w *workerAppsStub) StatefulSet() appscb.StatefulSetController { return w.statefulset }

type workerCoreStub struct {
	service corecb.ServiceController
}

func (w *workerCoreStub) ConfigMap() corecb.ConfigMapController                         { return nil }
func (w *workerCoreStub) Endpoints() corecb.EndpointsController                         { return nil }
func (w *workerCoreStub) Event() corecb.EventController                                 { return nil }
func (w *workerCoreStub) Namespace() corecb.NamespaceController                         { return nil }
func (w *workerCoreStub) Node() corecb.NodeController                                   { return nil }
func (w *workerCoreStub) PersistentVolume() corecb.PersistentVolumeController           { return nil }
func (w *workerCoreStub) PersistentVolumeClaim() corecb.PersistentVolumeClaimController { return nil }
func (w *workerCoreStub) Pod() corecb.PodController                                     { return nil }
func (w *workerCoreStub) Secret() corecb.SecretController                               { return nil }
func (w *workerCoreStub) Service() corecb.ServiceController                             { return w.service }
func (w *workerCoreStub) ServiceAccount() corecb.ServiceAccountController               { return nil }

// Test stubs for root controller
type rootAppsStub struct {
	statefulset appscb.StatefulSetController
}

func (r *rootAppsStub) DaemonSet() appscb.DaemonSetController     { return nil }
func (r *rootAppsStub) Deployment() appscb.DeploymentController   { return nil }
func (r *rootAppsStub) StatefulSet() appscb.StatefulSetController { return r.statefulset }

type rootCoreStub struct {
	service corecb.ServiceController
}

func (r *rootCoreStub) ConfigMap() corecb.ConfigMapController                         { return nil }
func (r *rootCoreStub) Endpoints() corecb.EndpointsController                         { return nil }
func (r *rootCoreStub) Event() corecb.EventController                                 { return nil }
func (r *rootCoreStub) Namespace() corecb.NamespaceController                         { return nil }
func (r *rootCoreStub) Node() corecb.NodeController                                   { return nil }
func (r *rootCoreStub) PersistentVolume() corecb.PersistentVolumeController           { return nil }
func (r *rootCoreStub) PersistentVolumeClaim() corecb.PersistentVolumeClaimController { return nil }
func (r *rootCoreStub) Pod() corecb.PodController                                     { return nil }
func (r *rootCoreStub) Secret() corecb.SecretController                               { return nil }
func (r *rootCoreStub) Service() corecb.ServiceController                             { return r.service }
func (r *rootCoreStub) ServiceAccount() corecb.ServiceAccountController               { return nil }

// Test stubs for model controller
type modelCoreStub struct {
	configMap corecb.ConfigMapController
	pvc       corecb.PersistentVolumeClaimController
}

func (m *modelCoreStub) ConfigMap() corecb.ConfigMapController                         { return m.configMap }
func (m *modelCoreStub) Endpoints() corecb.EndpointsController                         { return nil }
func (m *modelCoreStub) Event() corecb.EventController                                 { return nil }
func (m *modelCoreStub) Namespace() corecb.NamespaceController                         { return nil }
func (m *modelCoreStub) Node() corecb.NodeController                                   { return nil }
func (m *modelCoreStub) PersistentVolume() corecb.PersistentVolumeController           { return nil }
func (m *modelCoreStub) PersistentVolumeClaim() corecb.PersistentVolumeClaimController { return m.pvc }
func (m *modelCoreStub) Pod() corecb.PodController                                     { return nil }
func (m *modelCoreStub) Secret() corecb.SecretController                               { return nil }
func (m *modelCoreStub) Service() corecb.ServiceController                             { return nil }
func (m *modelCoreStub) ServiceAccount() corecb.ServiceAccountController               { return nil }

type modelBatchStub struct {
	job batchcb.JobController
}

func (m *modelBatchStub) Job() batchcb.JobController { return m.job }
