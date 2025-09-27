package controllers

import (
	"context"
	"fmt"

	koldv1 "github.com/gorizond/koldun/pkg/controllers/koldunv1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	appsv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	batchv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/batch/v1"
	corev1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/schemes"
	"k8s.io/client-go/rest"
)

// Manager wires together Wrangler factories, controllers, and reconcilers for the operator.
type Manager struct {
	factory *generic.Factory
	apply   apply.Apply

	Core  corev1.Interface
	Apps  appsv1.Interface
	Batch batchv1.Interface
	Kold  koldv1.Interface
}

// NewManager creates all controller factories required by the operator and prepares the reconciliation pipeline.
func NewManager(cfg *rest.Config) (*Manager, error) {
	if err := schemes.AddToScheme(schemes.All); err != nil {
		return nil, fmt.Errorf("register base schemes: %w", err)
	}

	factory, err := generic.NewFactoryFromConfigWithOptions(cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("build controller factory: %w", err)
	}

	ctrlFactory := factory.ControllerFactory()
	core := corev1.New(ctrlFactory)
	apps := appsv1.New(ctrlFactory)
	batch := batchv1.New(ctrlFactory)
	kold := koldv1.New(ctrlFactory)

	applier, err := apply.NewForConfig(cfg)
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
			apps.Deployment(),
			batch.Job(),
			core.ConfigMap(),
			core.Secret(),
			core.Service(),
			core.PersistentVolume(),
			core.PersistentVolumeClaim(),
			core.Pod(),
		)

	return &Manager{
		factory: factory,
		apply:   applier,
		Core:    core,
		Apps:    apps,
		Batch:   batch,
		Kold:    kold,
	}, nil
}

// Register initialises all controller handlers.
func (m *Manager) Register(ctx context.Context) error {
	if err := registerModelController(ctx, m); err != nil {
		return fmt.Errorf("register model controller: %w", err)
	}
	if err := registerRootController(ctx, m); err != nil {
		return fmt.Errorf("register root controller: %w", err)
	}
	if err := registerWorkerController(ctx, m); err != nil {
		return fmt.Errorf("register worker controller: %w", err)
	}
	if err := registerDllamaController(ctx, m); err != nil {
		return fmt.Errorf("register dllama controller: %w", err)
	}
	return nil
}

// Start runs the shared informers and controllers.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.factory.Start(ctx, 2); err != nil {
		return fmt.Errorf("start controller factory: %w", err)
	}
	<-ctx.Done()
	return nil
}

// Apply returns a copy of the apply helper bound to the given context.
func (m *Manager) Apply(ctx context.Context) apply.Apply {
	return m.apply.WithContext(ctx)
}
