package controllers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	lassocache "github.com/rancher/lasso/pkg/cache"
	"github.com/rancher/lasso/pkg/client"
	lassocontroller "github.com/rancher/lasso/pkg/controller"
	appscb "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	batchcb "github.com/rancher/wrangler/v3/pkg/generated/controllers/batch/v1"
	corecb "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

type managerAppsStub struct {
	deployment  appscb.DeploymentController
	statefulset appscb.StatefulSetController
}

func (m *managerAppsStub) DaemonSet() appscb.DaemonSetController     { return nil }
func (m *managerAppsStub) Deployment() appscb.DeploymentController   { return m.deployment }
func (m *managerAppsStub) StatefulSet() appscb.StatefulSetController { return m.statefulset }

type managerCoreStub struct {
	service corecb.ServiceController
	secret  corecb.SecretController
	pvc     corecb.PersistentVolumeClaimController
}

func (c *managerCoreStub) ConfigMap() corecb.ConfigMapController               { return nil }
func (c *managerCoreStub) Endpoints() corecb.EndpointsController               { return nil }
func (c *managerCoreStub) Event() corecb.EventController                       { return nil }
func (c *managerCoreStub) Namespace() corecb.NamespaceController               { return nil }
func (c *managerCoreStub) Node() corecb.NodeController                         { return nil }
func (c *managerCoreStub) PersistentVolume() corecb.PersistentVolumeController { return nil }
func (c *managerCoreStub) PersistentVolumeClaim() corecb.PersistentVolumeClaimController {
	return c.pvc
}
func (c *managerCoreStub) Pod() corecb.PodController                       { return nil }
func (c *managerCoreStub) Secret() corecb.SecretController                 { return c.secret }
func (c *managerCoreStub) Service() corecb.ServiceController               { return c.service }
func (c *managerCoreStub) ServiceAccount() corecb.ServiceAccountController { return nil }

type managerBatchStub struct {
	job batchcb.JobController
}

func (b *managerBatchStub) Job() batchcb.JobController { return b.job }

type sharedCacheFactoryStub struct {
	startCalls int
	waitCalls  int
}

func (s *sharedCacheFactoryStub) Start(ctx context.Context) error { s.startCalls++; return nil }
func (s *sharedCacheFactoryStub) StartGVK(context.Context, schema.GroupVersionKind) error {
	return nil
}
func (s *sharedCacheFactoryStub) ForObject(runtime.Object) (cache.SharedIndexInformer, error) {
	return nil, nil
}
func (s *sharedCacheFactoryStub) ForKind(schema.GroupVersionKind) (cache.SharedIndexInformer, error) {
	return nil, nil
}
func (s *sharedCacheFactoryStub) ForResource(schema.GroupVersionResource, bool) (cache.SharedIndexInformer, error) {
	return nil, nil
}
func (s *sharedCacheFactoryStub) ForResourceKind(schema.GroupVersionResource, string, bool) (cache.SharedIndexInformer, error) {
	return nil, nil
}
func (s *sharedCacheFactoryStub) WaitForCacheSync(context.Context) map[schema.GroupVersionKind]bool {
	s.waitCalls++
	return map[schema.GroupVersionKind]bool{}
}
func (s *sharedCacheFactoryStub) SharedClientFactory() client.SharedClientFactory { return nil }

type controllerFactoryStub struct {
	startCalls  int
	lastWorkers int
	startErr    error
}

func (c *controllerFactoryStub) ForObject(runtime.Object) (lassocontroller.SharedController, error) {
	return nil, nil
}
func (c *controllerFactoryStub) ForKind(schema.GroupVersionKind) (lassocontroller.SharedController, error) {
	return nil, nil
}
func (c *controllerFactoryStub) ForResource(schema.GroupVersionResource, bool) lassocontroller.SharedController {
	return nil
}
func (c *controllerFactoryStub) ForResourceKind(schema.GroupVersionResource, string, bool) lassocontroller.SharedController {
	return nil
}
func (c *controllerFactoryStub) SharedCacheFactory() lassocache.SharedCacheFactory { return nil }
func (c *controllerFactoryStub) Start(ctx context.Context, workers int) error {
	c.startCalls++
	c.lastWorkers = workers
	return c.startErr
}

type managerFactoryStub struct {
	syncCalls int
	syncErr   error
	ctrl      lassocontroller.SharedControllerFactory
}

func (m *managerFactoryStub) Sync(ctx context.Context) error {
	m.syncCalls++
	return m.syncErr
}

func (m *managerFactoryStub) ControllerFactory() lassocontroller.SharedControllerFactory {
	return m.ctrl
}

type noopControllerStarter struct {
	startCalls int
}

func (n *noopControllerStarter) Start(ctx context.Context, workers int) error {
	n.startCalls++
	return nil
}

func TestManagerRegisterWiresAllControllers(t *testing.T) {
	sessionCtrl := newControllerStub[*v1.Session, *v1.SessionList](schema.GroupVersionKind{})
	dllamaCtrl := newControllerStub[*v1.Dllama, *v1.DllamaList](schema.GroupVersionKind{})
	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	rootCtrl := newControllerStub[*v1.Root, *v1.RootList](schema.GroupVersionKind{})
	workerCtrl := newControllerStub[*v1.Worker, *v1.WorkerList](schema.GroupVersionKind{})
	ingressCtrl := newControllerStub[*v1.Ingress, *v1.IngressList](schema.GroupVersionKind{})

	deploymentCtrl := newControllerStub[*appsv1.Deployment, *appsv1.DeploymentList](schema.GroupVersionKind{})
	statefulsetCtrl := newControllerStub[*appsv1.StatefulSet, *appsv1.StatefulSetList](schema.GroupVersionKind{})
	serviceCtrl := newControllerStub[*corev1.Service, *corev1.ServiceList](schema.GroupVersionKind{})
	jobCtrl := newControllerStub[*batchv1.Job, *batchv1.JobList](schema.GroupVersionKind{})

	manager := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			session: sessionCtrl,
			dllama:  dllamaCtrl,
			model:   modelCtrl,
			root:    rootCtrl,
			worker:  workerCtrl,
			ingress: ingressCtrl,
		},
		Apps: &managerAppsStub{
			deployment:  deploymentCtrl,
			statefulset: statefulsetCtrl,
		},
		Core: &managerCoreStub{
			service: serviceCtrl,
		},
		Batch: &managerBatchStub{
			job: jobCtrl,
		},
	}

	require.NoError(t, manager.Register(context.Background()))

	require.NotNil(t, ingressCtrl.lastOnChange())
	require.NotNil(t, workerCtrl.lastOnChange())
	require.NotNil(t, workerCtrl.lastOnRemove())
	require.NotNil(t, rootCtrl.lastOnChange())
	require.NotNil(t, rootCtrl.lastOnRemove())
	require.NotNil(t, modelCtrl.lastOnChange())
	require.NotNil(t, modelCtrl.lastOnRemove())
	require.NotNil(t, sessionCtrl.lastOnChange())
	require.NotNil(t, sessionCtrl.lastOnRemove())
	require.NotNil(t, dllamaCtrl.lastOnChange())
	require.NotNil(t, dllamaCtrl.lastOnRemove())
	require.NotNil(t, deploymentCtrl.lastOnChange())
	require.NotNil(t, statefulsetCtrl.lastOnChange())
	require.NotNil(t, serviceCtrl.lastOnChange())
	require.NotNil(t, jobCtrl.lastOnChange())
}

func TestManagerRegisterPropagatesIngressError(t *testing.T) {
	manager := &Manager{}
	original := registerIngressControllerFn
	t.Cleanup(func() { registerIngressControllerFn = original })

	registerIngressControllerFn = func(context.Context, *Manager) error {
		return errors.New("boom")
	}

	err := manager.Register(context.Background())
	require.EqualError(t, err, "register ingress controller: boom")
}

func TestManagerRegisterStopsAfterFailure(t *testing.T) {
	manager := &Manager{}
	var calls []string

	origIngress := registerIngressControllerFn
	origModel := registerModelControllerFn
	origRoot := registerRootControllerFn
	origWorker := registerWorkerControllerFn
	origSession := registerSessionControllerFn
	origDllama := registerDllamaControllerFn
	t.Cleanup(func() {
		registerIngressControllerFn = origIngress
		registerModelControllerFn = origModel
		registerRootControllerFn = origRoot
		registerWorkerControllerFn = origWorker
		registerSessionControllerFn = origSession
		registerDllamaControllerFn = origDllama
	})

	registerIngressControllerFn = func(context.Context, *Manager) error {
		calls = append(calls, "ingress")
		return nil
	}
	registerModelControllerFn = func(context.Context, *Manager) error {
		calls = append(calls, "model")
		return nil
	}
	registerRootControllerFn = func(context.Context, *Manager) error {
		calls = append(calls, "root")
		return nil
	}
	registerWorkerControllerFn = func(context.Context, *Manager) error {
		calls = append(calls, "worker")
		return errors.New("boom")
	}
	registerSessionControllerFn = func(context.Context, *Manager) error {
		t.Fatalf("session controller should not be registered after failure")
		return nil
	}
	registerDllamaControllerFn = func(context.Context, *Manager) error {
		t.Fatalf("dllama controller should not be registered after failure")
		return nil
	}

	err := manager.Register(context.Background())
	require.EqualError(t, err, "register worker controller: boom")
	require.Equal(t, []string{"ingress", "model", "root", "worker"}, calls)
}

func TestManagerRegisterErrorPaths(t *testing.T) {
	origIngress := registerIngressControllerFn
	origModel := registerModelControllerFn
	origRoot := registerRootControllerFn
	origWorker := registerWorkerControllerFn
	origSession := registerSessionControllerFn
	origDllama := registerDllamaControllerFn
	defer func() {
		registerIngressControllerFn = origIngress
		registerModelControllerFn = origModel
		registerRootControllerFn = origRoot
		registerWorkerControllerFn = origWorker
		registerSessionControllerFn = origSession
		registerDllamaControllerFn = origDllama
	}()

	noOp := func(context.Context, *Manager) error { return nil }
	setAll := func(fn func(context.Context, *Manager) error) {
		registerIngressControllerFn = fn
		registerModelControllerFn = fn
		registerRootControllerFn = fn
		registerWorkerControllerFn = fn
		registerSessionControllerFn = fn
		registerDllamaControllerFn = fn
	}

	cases := []struct {
		name string
		set  func(func(context.Context, *Manager) error)
		msg  string
	}{
		{"ingress", func(fn func(context.Context, *Manager) error) { registerIngressControllerFn = fn }, "register ingress controller"},
		{"model", func(fn func(context.Context, *Manager) error) { registerModelControllerFn = fn }, "register model controller"},
		{"root", func(fn func(context.Context, *Manager) error) { registerRootControllerFn = fn }, "register root controller"},
		{"worker", func(fn func(context.Context, *Manager) error) { registerWorkerControllerFn = fn }, "register worker controller"},
		{"session", func(fn func(context.Context, *Manager) error) { registerSessionControllerFn = fn }, "register session controller"},
		{"dllama", func(fn func(context.Context, *Manager) error) { registerDllamaControllerFn = fn }, "register dllama controller"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setAll(noOp)
			boom := errors.New("boom")
			tc := tc
			tc.set(func(context.Context, *Manager) error { return boom })

			manager := &Manager{}
			err := manager.Register(context.Background())
			require.EqualError(t, err, fmt.Sprintf("%s: boom", tc.msg))
		})
	}
}

func TestManagerStartRunsFactories(t *testing.T) {
	cacheStub := &sharedCacheFactoryStub{}
	factory, err := generic.NewFactoryFromConfigWithOptions(nil, &generic.FactoryOptions{
		SharedCacheFactory: cacheStub,
	})
	require.NoError(t, err)

	controllerStub := &controllerFactoryStub{}

	manager := &Manager{
		factory:           factory,
		controllerFactory: controllerStub,
		health:            NewHealth(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, manager.Start(ctx))

	require.Equal(t, 1, cacheStub.startCalls)
	require.Equal(t, 1, cacheStub.waitCalls)
	require.Equal(t, 1, controllerStub.startCalls)
	require.Equal(t, 2, controllerStub.lastWorkers)
	require.True(t, manager.health.CachesSynced())
}

func TestManagerStartPropagatesControllerErrors(t *testing.T) {
	cacheStub := &sharedCacheFactoryStub{}
	factory, err := generic.NewFactoryFromConfigWithOptions(nil, &generic.FactoryOptions{
		SharedCacheFactory: cacheStub,
	})
	require.NoError(t, err)

	controllerStub := &controllerFactoryStub{
		startErr: errors.New("start failed"),
	}

	manager := &Manager{
		factory:           factory,
		controllerFactory: controllerStub,
		health:            NewHealth(),
	}

	err = manager.Start(context.Background())
	require.EqualError(t, err, "start controller factory: start failed")
	require.Equal(t, 1, controllerStub.startCalls)
}

func TestManagerStartPropagatesSyncErrors(t *testing.T) {
	factory := &managerFactoryStub{syncErr: errors.New("sync failed")}
	controller := &noopControllerStarter{}
	manager := &Manager{factory: factory, controllerFactory: controller, health: NewHealth()}

	err := manager.Start(context.Background())
	require.EqualError(t, err, "start controller factory: sync failed")
	require.Equal(t, 1, factory.syncCalls)
	require.Equal(t, 0, controller.startCalls)
	require.False(t, manager.Health().CachesSynced())
}
