package controllers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func TestEnsureLabels(t *testing.T) {
	meta := &metav1.ObjectMeta{}

	require.False(t, ensureLabels(meta, nil))
	require.False(t, ensureLabels(meta, map[string]string{}))

	changed := ensureLabels(meta, map[string]string{"a": "b"})
	require.True(t, changed)
	require.Equal(t, "b", meta.Labels["a"])

	require.False(t, ensureLabels(meta, map[string]string{"a": "b"}))

	changed = ensureLabels(meta, map[string]string{"a": "c", "b": "d"})
	require.True(t, changed)
	require.Equal(t, map[string]string{"a": "c", "b": "d"}, meta.Labels)
}

func TestEnsureAnnotations(t *testing.T) {
	meta := &metav1.ObjectMeta{}

	require.False(t, ensureAnnotations(meta, nil))
	require.False(t, ensureAnnotations(meta, map[string]string{}))

	require.True(t, ensureAnnotations(meta, map[string]string{"key": "value"}))
	require.Equal(t, "value", meta.Annotations["key"])

	require.False(t, ensureAnnotations(meta, map[string]string{"key": "value"}))

	require.True(t, ensureAnnotations(meta, map[string]string{"another": "entry"}))
	require.Equal(t, map[string]string{"key": "value", "another": "entry"}, meta.Annotations)
}

func TestEnsureOwnerReferenceHandlesNilControllerRef(t *testing.T) {
	orig := newControllerRef
	t.Cleanup(func() { newControllerRef = orig })
	newControllerRef = func(owner metav1.Object, gvk schema.GroupVersionKind) *metav1.OwnerReference {
		return nil
	}
	meta := &metav1.ObjectMeta{}
	require.False(t, ensureOwnerReference(meta, &v1.Session{}))
}

func TestEnsureTrailingDotHelper(t *testing.T) {
	require.Equal(t, "", ensureTrailingDot(""))
	require.Equal(t, "prefix.", ensureTrailingDot("prefix"))
	require.Equal(t, "subject.", ensureTrailingDot("subject."))
}

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "   ", want: ""},
		{name: "alphanumeric", in: "Model-123", want: "Model-123"},
		{name: "spaces and dots", in: "model name.v1", want: "model-name-v1"},
		{name: "unicode fallback", in: "модель", want: strings.Repeat("-", len([]rune("модель")))},
		{name: "mixed punctuation", in: "a/b?c*d", want: "a-b-c-d"},
		{name: "underscores preserved", in: "demo_value", want: "demo_value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sanitizeIdentifier(tt.in))
		})
	}
}

func TestResourceSessionKey(t *testing.T) {
	require.Equal(t, "", resourceSessionKey("", "ns", "name"))
	require.Equal(t, "", resourceSessionKey(resourceDllama, "ns", ""))
	require.Equal(t, "dllama/ns/name", resourceSessionKey(resourceDllama, "ns", "name"))
}

func TestTrackAndPopResourceSession(t *testing.T) {
	handler := &sessionHandler{resourceSessions: map[string]string{}}

	handler.trackResourceSession(resourceDllama, "ns", "name", "session")
	require.Equal(t, "session", handler.popResourceSession(resourceDllama, "ns", "name"))

	require.Equal(t, "", handler.popResourceSession(resourceDllama, "ns", "name"), "pop should delete entry")

	handler.trackResourceSession(resourceDllama, "ns", "", "session")
	require.Empty(t, handler.resourceSessions)
}

func TestSplitNamespaceName(t *testing.T) {

	ns, name := splitNamespaceName("sessions/demo")
	require.Equal(t, "sessions", ns)
	require.Equal(t, "demo", name)

	ns, name = splitNamespaceName("dllama-only")
	require.Equal(t, "", ns)
	require.Equal(t, "dllama-only", name)

	ns, name = splitNamespaceName("")
	require.Equal(t, "", ns)
	require.Equal(t, "", name)
}

func TestGuessSessionNameHelpers(t *testing.T) {

	require.Equal(t, "demo", guessSessionFromDllamaName("demo-dllama-0"))
	require.Equal(t, "", guessSessionFromDllamaName("dllama-only"))
	require.Equal(t, "demo", guessSessionFromRootName("demo-dllama-root"))
	require.Equal(t, "", guessSessionFromRootName("root"))
	require.Equal(t, "demo", guessSessionFromWorkerName("demo-dllama-workers"))
	require.Equal(t, "", guessSessionFromWorkerName(""))
}

func TestEnsureOwnerReference(t *testing.T) {

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo",
			UID:  types.UID("demo-uid"),
		},
	}

	t.Run("adds owner when missing", func(t *testing.T) {
		meta := &metav1.ObjectMeta{}
		require.True(t, ensureOwnerReference(meta, sess))
		require.Len(t, meta.OwnerReferences, 1)
		require.Equal(t, sess.Name, meta.OwnerReferences[0].Name)
	})

	t.Run("updates mismatched owner", func(t *testing.T) {
		meta := &metav1.ObjectMeta{
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:        types.UID("demo-uid"),
					APIVersion: "different/v1",
					Kind:       "Other",
					Name:       "wrong",
				},
			},
		}
		require.True(t, ensureOwnerReference(meta, sess))
		require.Equal(t, v1.SchemeGroupVersion.String(), meta.OwnerReferences[0].APIVersion)
		require.Equal(t, "Session", meta.OwnerReferences[0].Kind)
		require.Equal(t, sess.Name, meta.OwnerReferences[0].Name)
	})

	t.Run("no-op when already up to date", func(t *testing.T) {
		meta := &metav1.ObjectMeta{}
		require.True(t, ensureOwnerReference(meta, sess))
		require.False(t, ensureOwnerReference(meta, sess))
	})

	t.Run("nil session returns false", func(t *testing.T) {
		meta := &metav1.ObjectMeta{}
		require.False(t, ensureOwnerReference(meta, nil))
		require.Empty(t, meta.OwnerReferences)
	})
}

func TestSessionHandlerEnqueueSession(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	handler := &sessionHandler{sessions: sessions}

	session := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sessions",
		},
	}

	sessions.EXPECT().Enqueue("sessions", "demo")
	handler.enqueueSession(session)

	handler.sessions = nil
	require.NotPanics(t, func() { handler.enqueueSession(session) })

	handler.sessions = sessions
	require.NotPanics(t, func() { handler.enqueueSession(nil) })
}

func TestDeleteDllama(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	handler := &sessionHandler{
		dllamas: mockDllamas,
	}

	sess := &v1.Session{}
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sessions",
		},
	}

	mockDllamas.EXPECT().
		Delete("sessions", "demo", gomock.Any()).
		Return(nil)
	require.NoError(t, handler.deleteDllama(sess, dllama))

	mockDllamas.EXPECT().
		Delete("sessions", "demo", gomock.Any()).
		Return(apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "dllamas"}, "demo"))
	require.NoError(t, handler.deleteDllama(sess, dllama))

	mockDllamas.EXPECT().
		Delete("sessions", "demo", gomock.Any()).
		Return(errors.New("delete failure"))
	require.EqualError(t, handler.deleteDllama(sess, dllama), "delete failure")

	require.NoError(t, handler.deleteDllama(sess, nil))
}

func TestSessionHandlerOnRelatedDllama(t *testing.T) {

	t.Run("delete uses tracked session", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{resourceSessionKey(resourceDllama, "ns", "demo-dllama"): "demo"},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedDllama("ns/demo-dllama", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("delete falls back to name guess", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedDllama("ns/demo-dllama", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("active dllama tracks session", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{},
		}

		dllama := &v1.Dllama{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "demo-dllama",
				Labels: map[string]string{
					labelSessionName: "demo",
				},
			},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedDllama("ns/demo-dllama", dllama)
		require.NoError(t, err)
		require.Equal(t, dllama, result)

		key := resourceSessionKey(resourceDllama, "ns", "demo-dllama")
		require.Equal(t, "demo", handler.resourceSessions[key])
	})
}

func TestSessionHandlerOnRelatedRoot(t *testing.T) {

	t.Run("delete guesses session from name", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedRoot("ns/demo-dllama-1-root", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("delete uses tracked session", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{resourceSessionKey(resourceRoot, "ns", "demo-root"): "demo"},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedRoot("ns/demo-root", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("active root tracks session", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{},
		}

		root := &v1.Root{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "demo-root",
				Labels: map[string]string{
					labelSessionName: "demo",
				},
			},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedRoot("ns/demo-root", root)
		require.NoError(t, err)
		require.Equal(t, root, result)

		key := resourceSessionKey(resourceRoot, "ns", "demo-root")
		require.Equal(t, "demo", handler.resourceSessions[key])
	})
}

func TestSessionHandlerOnRelatedWorker(t *testing.T) {

	t.Run("delete guesses session from name", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedWorker("ns/demo-dllama-1-workers", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("delete uses tracked session", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{resourceSessionKey(resourceWorker, "ns", "demo-workers"): "demo"},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedWorker("ns/demo-workers", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("active worker tracks session", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
		handler := &sessionHandler{
			sessions:         sessions,
			resourceSessions: map[string]string{},
		}

		worker := &v1.Worker{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "demo-workers",
				Labels: map[string]string{
					labelSessionName: "demo",
				},
			},
		}

		sessions.EXPECT().Enqueue("ns", "demo")
		result, err := handler.onRelatedWorker("ns/demo-workers", worker)
		require.NoError(t, err)
		require.Equal(t, worker, result)

		key := resourceSessionKey(resourceWorker, "ns", "demo-workers")
		require.Equal(t, "demo", handler.resourceSessions[key])
	})
}

func TestSessionHandlerLookupRootEndpoint(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	roots := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Root](ctrl)
	roots.EXPECT().Cache().Return(cache).AnyTimes()

	handler := &sessionHandler{roots: roots}

	cache.EXPECT().Get("ns", "demo-root").Return(&v1.Root{
		Status: v1.RootStatus{Endpoint: " root.endpoint.svc "},
	}, nil)
	require.Equal(t, "root.endpoint.svc", handler.lookupRootEndpoint("ns", "demo"))

	cache.EXPECT().Get("ns", "missing-root").Return(nil, errors.New("not found"))
	require.Equal(t, "", handler.lookupRootEndpoint("ns", "missing"))
}

func TestSessionHandlerCheckHealth(t *testing.T) {

	handler := &sessionHandler{
		ctx: context.Background(),
	}

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()

	handler.httpClient = healthy.Client()
	require.True(t, handler.checkHealth(strings.TrimPrefix(healthy.URL, "http://")))

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer unhealthy.Close()

	handler.httpClient = unhealthy.Client()
	require.False(t, handler.checkHealth(strings.TrimPrefix(unhealthy.URL, "http://")))

	// missing endpoint should fail fast
	require.False(t, handler.checkHealth(""))
}
