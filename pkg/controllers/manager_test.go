package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManagerHelpers(t *testing.T) {
	t.Parallel()

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
