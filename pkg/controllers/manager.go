package controllers

import (
	"context"
	"fmt"

	koldv1 "github.com/gorizond/koldun/pkg/controllers/koldunv1"
	lassocontroller "github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/wrangler/v3/pkg/apply"
	appsv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	batchv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/batch/v1"
	corev1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/schemes"
	"k8s.io/client-go/rest"
)

type sharedFactory interface {
	Sync(ctx context.Context) error
	ControllerFactory() lassocontroller.SharedControllerFactory
}

type controllerStarter interface {
	Start(ctx context.Context, workers int) error
}

var (
	registerIngressControllerFn = registerIngressController
	registerModelControllerFn   = registerModelController
	registerRootControllerFn    = registerRootController
	registerWorkerControllerFn  = registerWorkerController
	registerSessionControllerFn = registerSessionController
	registerDllamaControllerFn  = registerDllamaController
	addSchemesFn                = schemes.AddToScheme
	newFactoryFromConfigFn      = func(cfg *rest.Config, opts *generic.FactoryOptions) (sharedFactory, error) {
		return generic.NewFactoryFromConfigWithOptions(cfg, opts)
	}
	newApplyForConfigFn = apply.NewForConfig
	factoryOptionsFn    = func(health *Health) *generic.FactoryOptions {
		return &generic.FactoryOptions{
			HealthCallback: health.SetAPIHealthy,
		}
	}
	newCoreControllersFn  = corev1.New
	newAppsControllersFn  = appsv1.New
	newBatchControllersFn = batchv1.New
	newKoldControllersFn  = koldv1.New
)

// Manager wires together Wrangler factories, controllers, and reconcilers for the operator.
type Manager struct {
	factory           sharedFactory
	controllerFactory controllerStarter
	apply             apply.Apply

	health *Health

	ensureObjectStorageBuckets bool

	Core  corev1.Interface
	Apps  appsv1.Interface
	Batch batchv1.Interface
	Kold  koldv1.Interface
}

// NewManager creates all controller factories required by the operator and prepares the reconciliation pipeline.
func NewManager(cfg *rest.Config) (*Manager, error) {
	if err := addSchemesFn(schemes.All); err != nil {
		return nil, fmt.Errorf("register base schemes: %w", err)
	}

	health := NewHealth()

	factoryOptions := factoryOptionsFn(health)
	factory, err := newFactoryFromConfigFn(cfg, factoryOptions)
	if err != nil {
		return nil, fmt.Errorf("build controller factory: %w", err)
	}

	ctrlFactory := factory.ControllerFactory()
	core := newCoreControllersFn(ctrlFactory)
	apps := newAppsControllersFn(ctrlFactory)
	batch := newBatchControllersFn(ctrlFactory)
	kold := newKoldControllersFn(ctrlFactory)

	applier, err := newApplyForConfigFn(cfg)
	if err != nil {
		return nil, fmt.Errorf("build apply client: %w", err)
	}

	applier = applier.
		WithDynamicLookup().
		WithCacheTypes(
			kold.Dllama(),
			kold.Model(),
			kold.Root(),
			kold.Worker(),
			kold.Ingress(),
			kold.Session(),
			apps.Deployment(),
			apps.StatefulSet(),
			batch.Job(),
			core.ConfigMap(),
			core.Secret(),
			core.Service(),
			core.PersistentVolume(),
			core.PersistentVolumeClaim(),
			core.Pod(),
		)

	return &Manager{
		factory:                    factory,
		controllerFactory:          ctrlFactory,
		apply:                      applier,
		health:                     health,
		Core:                       core,
		Apps:                       apps,
		Batch:                      batch,
		Kold:                       kold,
		ensureObjectStorageBuckets: true,
	}, nil
}

// Register initialises all controller handlers.
func (m *Manager) Register(ctx context.Context) error {
	if err := registerIngressControllerFn(ctx, m); err != nil {
		return fmt.Errorf("register ingress controller: %w", err)
	}
	if err := registerModelControllerFn(ctx, m); err != nil {
		return fmt.Errorf("register model controller: %w", err)
	}
	if err := registerRootControllerFn(ctx, m); err != nil {
		return fmt.Errorf("register root controller: %w", err)
	}
	if err := registerWorkerControllerFn(ctx, m); err != nil {
		return fmt.Errorf("register worker controller: %w", err)
	}
	if err := registerSessionControllerFn(ctx, m); err != nil {
		return fmt.Errorf("register session controller: %w", err)
	}
	if err := registerDllamaControllerFn(ctx, m); err != nil {
		return fmt.Errorf("register dllama controller: %w", err)
	}
	return nil
}

// Start runs the shared informers and controllers.
func (m *Manager) Start(ctx context.Context) error {
	m.health.SetCachesSynced(false)

	if err := m.factory.Sync(ctx); err != nil {
		return fmt.Errorf("start controller factory: %w", err)
	}

	m.health.SetCachesSynced(true)

	if err := m.controllerFactory.Start(ctx, 2); err != nil {
		return fmt.Errorf("start controller factory: %w", err)
	}

	return nil
}

// Apply returns a copy of the apply helper bound to the given context.
func (m *Manager) Apply(ctx context.Context) apply.Apply {
	return m.apply.WithContext(ctx)
}

// Health exposes the manager health tracker for readiness endpoints.
func (m *Manager) Health() *Health {
	return m.health
}

// SetEnsureObjectStorageBuckets toggles automatic S3 bucket provisioning for model reconcilers.
func (m *Manager) SetEnsureObjectStorageBuckets(enabled bool) {
	m.ensureObjectStorageBuckets = enabled
}

// EnsureObjectStorageBuckets reports whether model reconciliation should attempt to provision object storage buckets.
func (m *Manager) EnsureObjectStorageBuckets() bool {
	return m.ensureObjectStorageBuckets
}
