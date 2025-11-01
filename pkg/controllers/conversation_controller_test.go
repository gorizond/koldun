package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/apply/injectors"
	"github.com/rancher/wrangler/v3/pkg/objectset"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestEnsureSessionCreatesExpectedSession(t *testing.T) {
	record := &conversation.Record{
		Hash:            "test-hash-should-be-truncated-because-it-is-way-too-long-to-fit",
		Session:         "custom-session-name",
		Namespace:       "test-namespace",
		Model:           "models/custom-model",
		ReplicaPower:    4,
		RootImage:       "ghcr.io/example/root:latest",
		WorkerImage:     "ghcr.io/example/worker:latest",
		DispatcherImage: "ghcr.io/example/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL:               "nats://nats.example.svc:4222",
			CredentialsSecret: "nats-secret",
		},
		Queue: &conversation.QueueConfig{
			BacklogSubject:        "custom.backlog",
			ResponseSubjectPrefix: "responses.",
			AssignmentsBucket:     "custom_assignments",
			DllamaSubjectPrefix:   "dllama.custom.",
			StateStream:           "CUSTOM_STREAM",
		},
		Scaling: &conversation.SessionScalingConfig{
			MinDllamas:           2,
			MaxDllamas:           6,
			ScaleUpBacklog:       3,
			ScaleDownIdleSeconds: 120,
			DesiredDllamas:       5,
		},
	}
	require.NoError(t, record.Validate())

	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg:   ConversationConfig{TTLPrefix: "nats_ttl_"},
		apply: fakeApply,
	}

	err := reconciler.ensureSession(record)
	require.NoError(t, err)

	require.Equal(t, record.Namespace, fakeApply.defaultNamespace)
	require.Equal(t, fmt.Sprintf("conversation-session-%s", record.Hash), fakeApply.setID)
	require.Len(t, fakeApply.appliedObjects, 1)

	session, ok := fakeApply.appliedObjects[0].(*v1.Session)
	require.True(t, ok, "expected Session object to be applied")

	require.Equal(t, record.SessionName(), session.Name)
	require.Equal(t, record.Namespace, session.Namespace)
	require.Contains(t, session.Labels, labelConversationHash)
	require.LessOrEqual(t, len(session.Labels[labelConversationHash]), 63)

	require.Equal(t, record.Hash, session.Spec.Hash)
	require.Equal(t, "Model", session.Spec.ModelRef.Kind)
	require.Equal(t, "models", session.Spec.ModelRef.Namespace)
	require.Equal(t, "custom-model", session.Spec.ModelRef.Name)
	require.Equal(t, int32(4), session.Spec.ReplicaPower)
	require.Equal(t, record.DispatcherImage, session.Spec.DispatcherImage)
	require.Equal(t, record.RootImage, session.Spec.RootImage)
	require.Equal(t, record.WorkerImage, session.Spec.WorkerImage)

	require.NotNil(t, session.Spec.NATS)
	require.Equal(t, record.NATS.URL, session.Spec.NATS.URL)
	require.NotNil(t, session.Spec.NATS.CredentialsSecret)
	require.Equal(t, "nats-secret", session.Spec.NATS.CredentialsSecret.Name)

	require.NotNil(t, session.Spec.Queue)
	require.Equal(t, record.Queue.BacklogSubject, session.Spec.Queue.BacklogSubject)
	require.Equal(t, record.Queue.ResponseSubjectPrefix, session.Spec.Queue.ResponseSubjectPrefix)
	require.Equal(t, record.Queue.AssignmentsBucket, session.Spec.Queue.AssignmentsBucket)
	require.Equal(t, record.Queue.DllamaSubjectPrefix, session.Spec.Queue.DllamaSubjectPrefix)
	require.Equal(t, record.Queue.StateStream, session.Spec.Queue.StateStream)

	require.NotNil(t, session.Spec.Scaling)
	require.Equal(t, record.Scaling.MinDllamas, session.Spec.Scaling.MinDllamas)
	require.Equal(t, record.Scaling.MaxDllamas, session.Spec.Scaling.MaxDllamas)
	require.Equal(t, record.Scaling.ScaleUpBacklog, session.Spec.Scaling.ScaleUpBacklog)
	require.Equal(t, record.Scaling.ScaleDownIdleSeconds, session.Spec.Scaling.ScaleDownIdleSeconds)
	require.Equal(t, record.Scaling.DesiredDllamas, session.Spec.Scaling.DesiredDllamas)
}

func TestEnsureSessionValidationErrors(t *testing.T) {
	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg:   ConversationConfig{TTLPrefix: "nats_ttl_"},
		apply: fakeApply,
	}

	t.Run("missing session name", func(t *testing.T) {
		record := &conversation.Record{
			Hash:        "",
			Session:     "",
			Namespace:   "ns",
			Model:       "model",
			RootImage:   "root",
			WorkerImage: "worker",
		}
		err := reconciler.ensureSession(record)
		require.Error(t, err)
		require.Contains(t, err.Error(), "session name missing")
	})

	t.Run("missing model name", func(t *testing.T) {
		record := &conversation.Record{
			Hash:        "hash",
			Session:     "session",
			Namespace:   "ns",
			Model:       "",
			RootImage:   "root",
			WorkerImage: "worker",
		}
		err := reconciler.ensureSession(record)
		require.Error(t, err)
		require.Contains(t, err.Error(), "model name missing")
	})

	t.Run("missing images", func(t *testing.T) {
		record := &conversation.Record{
			Hash:      "hash",
			Session:   "session",
			Namespace: "ns",
			Model:     "model",
		}
		err := reconciler.ensureSession(record)
		require.Error(t, err)
		require.Contains(t, err.Error(), "images missing")
	})
}

func TestEnsureSessionDefaults(t *testing.T) {
	longHash := "hash-with-extra-characters-that-should-be-clipped-because-label-limits-are-strict"
	record := &conversation.Record{
		Hash:        longHash,
		Session:     "session-defaults",
		Namespace:   "default",
		Model:       "models::namespace/default-model",
		RootImage:   "ghcr.io/example/root:1.0.0",
		WorkerImage: "ghcr.io/example/worker:1.0.0",
		// ReplicaPower unset (0), Scaling and Queue nil to exercise default branches
	}
	require.NoError(t, record.Validate())

	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg:   ConversationConfig{TTLPrefix: "nats_ttl_"},
		apply: fakeApply,
	}

	require.NoError(t, reconciler.ensureSession(record))
	require.Len(t, fakeApply.appliedObjects, 1)

	session, ok := fakeApply.appliedObjects[0].(*v1.Session)
	require.True(t, ok)

	require.Equal(t, "default", session.Namespace)
	require.Equal(t, "session-defaults", session.Name)
	require.Equal(t, int32(1), session.Spec.ReplicaPower, "replica power should default to 1 when not provided")

	require.Equal(t, int32(1), session.Spec.MinIdle, "min idle should default to 1")
	require.Equal(t, int32(0), session.Spec.MaxWorkers, "max workers defaults to zero when scaling not provided")
	require.NotNil(t, session.Spec.Scaling, "scaling spec should be allocated when record scaling is defaulted")
	require.Equal(t, int32(1), session.Spec.Scaling.MinDllamas)
	require.Equal(t, int32(0), session.Spec.Scaling.MaxDllamas)
	require.Equal(t, int32(1), session.Spec.Scaling.DesiredDllamas)
	require.NotNil(t, session.Spec.Queue, "queue spec should be populated with defaults")
	require.True(t, strings.HasPrefix(session.Spec.Queue.BacklogSubject, "sessions."), "backlog subject should adopt default prefix")
	require.True(t, strings.HasPrefix(session.Spec.Queue.DllamaSubjectPrefix, "sessions."), "dllama subject should adopt default prefix")
	require.NotEmpty(t, session.Spec.Queue.AssignmentsBucket)
	require.NotEmpty(t, session.Spec.Queue.StateStream)
	require.Nil(t, session.Spec.NATS, "nats spec should be nil when URL is empty")

	label := session.Labels[labelConversationHash]
	require.NotEmpty(t, label, "conversation hash label should be populated")
	require.LessOrEqual(t, len(label), 63, "label value must satisfy DNS1123 label length")
	require.True(t, strings.HasPrefix(label, "hash-with-extra-characters"), "label should be derived from record hash")

	require.Equal(t, "default", fakeApply.defaultNamespace)
	require.Equal(t, fmt.Sprintf("conversation-session-%s", record.Hash), fakeApply.setID)
}

type fakeApply struct {
	defaultNamespace string
	setID            string
	appliedObjects   []runtime.Object
}

func newFakeApply() *fakeApply {
	return &fakeApply{appliedObjects: []runtime.Object{}}
}

func (f *fakeApply) Apply(*objectset.ObjectSet) error {
	return nil
}

func (f *fakeApply) ApplyObjects(objs ...runtime.Object) error {
	f.appliedObjects = append(f.appliedObjects, objs...)
	return nil
}

func (f *fakeApply) WithContext(context.Context) apply.Apply { return f }
func (f *fakeApply) WithCacheTypes(...apply.InformerGetter) apply.Apply {
	return f
}
func (f *fakeApply) WithCacheTypeFactory(apply.InformerFactory) apply.Apply {
	return f
}
func (f *fakeApply) WithSetID(id string) apply.Apply {
	f.setID = id
	return f
}
func (f *fakeApply) WithOwner(runtime.Object) apply.Apply { return f }
func (f *fakeApply) WithOwnerKey(string, schema.GroupVersionKind) apply.Apply {
	return f
}
func (f *fakeApply) WithInjector(...injectors.ConfigInjector) apply.Apply { return f }
func (f *fakeApply) WithInjectorName(...string) apply.Apply               { return f }
func (f *fakeApply) WithPatcher(schema.GroupVersionKind, apply.Patcher) apply.Apply {
	return f
}
func (f *fakeApply) WithReconciler(schema.GroupVersionKind, apply.Reconciler) apply.Apply {
	return f
}
func (f *fakeApply) WithStrictCaching() apply.Apply { return f }
func (f *fakeApply) WithDynamicLookup() apply.Apply { return f }
func (f *fakeApply) WithRestrictClusterScoped() apply.Apply {
	return f
}
func (f *fakeApply) WithDefaultNamespace(ns string) apply.Apply {
	f.defaultNamespace = ns
	return f
}
func (f *fakeApply) WithListerNamespace(string) apply.Apply { return f }
func (f *fakeApply) WithRateLimiting(float32) apply.Apply   { return f }
func (f *fakeApply) WithNoDelete() apply.Apply              { return f }
func (f *fakeApply) WithNoDeleteGVK(...schema.GroupVersionKind) apply.Apply {
	return f
}
func (f *fakeApply) WithGVK(...schema.GroupVersionKind) apply.Apply { return f }
func (f *fakeApply) WithSetOwnerReference(bool, bool) apply.Apply   { return f }
func (f *fakeApply) WithIgnorePreviousApplied() apply.Apply         { return f }
func (f *fakeApply) WithDiffPatch(schema.GroupVersionKind, string, string, []byte) apply.Apply {
	return f
}

func (f *fakeApply) FindOwner(runtime.Object) (runtime.Object, error) { return nil, nil }
func (f *fakeApply) PurgeOrphan(runtime.Object) error                 { return nil }
func (f *fakeApply) DryRun(...runtime.Object) (apply.Plan, error) {
	return apply.Plan{}, nil
}

var _ apply.Apply = (*fakeApply)(nil)
