package controllers

import (
	"context"
	"fmt"
	"testing"

	koldv1 "github.com/gorizond/koldun/pkg/controllers/koldunv1"
	lassocontroller "github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/wrangler/v3/pkg/apply"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	appsv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	batchv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/batch/v1"
	corev1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func TestManagerHelpers(t *testing.T) {

	fake := newFakeApply()
	manager := &Manager{
		apply:                      fake,
		health:                     NewHealth(),
		ensureObjectStorageBuckets: true,
	}

	ctx := context.Background()

	require.Same(t, fake, manager.Apply(ctx))
	require.Same(t, manager.health, manager.Health())

	require.True(t, manager.EnsureObjectStorageBuckets())

	manager.SetEnsureObjectStorageBuckets(false)
	require.False(t, manager.EnsureObjectStorageBuckets())

	manager.SetEnsureObjectStorageBuckets(true)
	require.True(t, manager.EnsureObjectStorageBuckets())
}

func TestNewManagerRequiresConfig(t *testing.T) {
	origFactoryFn := newFactoryFromConfigFn
	t.Cleanup(func() { newFactoryFromConfigFn = origFactoryFn })

	newFactoryFromConfigFn = func(*rest.Config, *generic.FactoryOptions) (sharedFactory, error) {
		return nil, fmt.Errorf("boom")
	}

	_, err := NewManager(nil)
	require.EqualError(t, err, "build controller factory: boom")
}

func TestNewManagerHandlesSchemeError(t *testing.T) {
	origAdd := addSchemesFn
	t.Cleanup(func() { addSchemesFn = origAdd })

	addSchemesFn = func(*runtime.Scheme) error { return fmt.Errorf("scheme boom") }

	_, err := NewManager(nil)
	require.EqualError(t, err, "register base schemes: scheme boom")
}

func TestNewManagerHandlesApplyError(t *testing.T) {
	factoryStub := &managerFactoryStub{ctrl: &controllerFactoryStub{}}
	origFactory := newFactoryFromConfigFn
	origApply := newApplyForConfigFn
	t.Cleanup(func() {
		newFactoryFromConfigFn = origFactory
		newApplyForConfigFn = origApply
	})

	newFactoryFromConfigFn = func(*rest.Config, *generic.FactoryOptions) (sharedFactory, error) {
		return factoryStub, nil
	}
	newApplyForConfigFn = func(*rest.Config) (apply.Apply, error) {
		return nil, fmt.Errorf("apply boom")
	}

	_, err := NewManager(nil)
	require.EqualError(t, err, "build apply client: apply boom")
}

func TestNewManagerInitializesClients(t *testing.T) {
	fakeApply := &fakeapply.FakeApply{}
	factoryStub := &managerFactoryStub{ctrl: &controllerFactoryStub{}}
	origFactoryFn := newFactoryFromConfigFn
	origApplyFn := newApplyForConfigFn
	origCoreFn := newCoreControllersFn
	origAppsFn := newAppsControllersFn
	origBatchFn := newBatchControllersFn
	origKoldFn := newKoldControllersFn
	t.Cleanup(func() {
		newFactoryFromConfigFn = origFactoryFn
		newApplyForConfigFn = origApplyFn
		newCoreControllersFn = origCoreFn
		newAppsControllersFn = origAppsFn
		newBatchControllersFn = origBatchFn
		newKoldControllersFn = origKoldFn
	})

	newFactoryFromConfigFn = func(*rest.Config, *generic.FactoryOptions) (sharedFactory, error) {
		return factoryStub, nil
	}

	newApplyForConfigFn = func(*rest.Config) (apply.Apply, error) {
		return fakeApply, nil
	}

	coreStub := &managerCoreStub{}
	appsStub := &managerAppsStub{}
	batchStub := &managerBatchStub{}
	koldStub := &fakeKoldInterface{}

	newCoreControllersFn = func(lassocontroller.SharedControllerFactory) corev1.Interface { return coreStub }
	newAppsControllersFn = func(lassocontroller.SharedControllerFactory) appsv1.Interface { return appsStub }
	newBatchControllersFn = func(lassocontroller.SharedControllerFactory) batchv1.Interface { return batchStub }
	newKoldControllersFn = func(lassocontroller.SharedControllerFactory) koldv1.Interface { return koldStub }

	manager, err := NewManager(nil)
	require.NoError(t, err)
	require.NotNil(t, manager.Core)
	require.NotNil(t, manager.Apps)
	require.NotNil(t, manager.Batch)
	require.NotNil(t, manager.Kold)
	require.Same(t, fakeApply, manager.apply)
	require.NotNil(t, manager.Apply(context.Background()))
}

func TestDefaultFactoryFromConfigUsesCreator(t *testing.T) {
	orig := factoryCreatorFn
	t.Cleanup(func() { factoryCreatorFn = orig })

	called := false
	stub := &managerFactoryStub{ctrl: &controllerFactoryStub{}}
	factoryCreatorFn = func(cfg *rest.Config, opts *generic.FactoryOptions) (sharedFactory, error) {
		called = true
		return stub, nil
	}

	result, err := defaultFactoryFromConfig(nil, nil)
	require.NoError(t, err)
	require.True(t, called)
	require.Same(t, stub, result)
}

func TestDefaultFactoryCreatorDelegates(t *testing.T) {
	orig := newGenericFactoryWithOptions
	t.Cleanup(func() { newGenericFactoryWithOptions = orig })

	called := false
	stub := &managerFactoryStub{ctrl: &controllerFactoryStub{}}
	newGenericFactoryWithOptions = func(cfg *rest.Config, opts *generic.FactoryOptions) (sharedFactory, error) {
		called = true
		return stub, nil
	}

	result, err := defaultFactoryCreator(nil, nil)
	require.NoError(t, err)
	require.True(t, called)
	require.Same(t, stub, result)
}

func TestDefaultFactoryCreatorDefaultPath(t *testing.T) {
	factory, err := defaultFactoryCreator(&rest.Config{}, &generic.FactoryOptions{})
	require.NoError(t, err)
	require.NotNil(t, factory)
}
