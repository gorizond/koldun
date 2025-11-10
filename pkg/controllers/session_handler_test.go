package controllers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestSessionHandlerOnChangeScenarios(t *testing.T) {
	sess := &v1.Session{}

	t.Run("nil session returns nil", func(t *testing.T) {
		var (
			topologyCalled bool
			statusCalled   bool
		)
		handler := &sessionHandler{
			ensureTopologyFn: func(*v1.Session) error {
				topologyCalled = true
				return nil
			},
			ensureStatusFn: func(sess *v1.Session) (*v1.Session, error) {
				statusCalled = true
				return sess, nil
			},
		}

		result, err := handler.onChange("ns/session", nil)
		require.NoError(t, err)
		require.Nil(t, result)
		require.False(t, topologyCalled)
		require.False(t, statusCalled)
	})

	t.Run("deletion timestamp short-circuits reconciliation", func(t *testing.T) {
		var (
			topologyCalled bool
			statusCalled   bool
		)
		handler := &sessionHandler{
			ensureTopologyFn: func(*v1.Session) error {
				topologyCalled = true
				return nil
			},
			ensureStatusFn: func(sess *v1.Session) (*v1.Session, error) {
				statusCalled = true
				return sess, nil
			},
		}
		deleting := sess.DeepCopy()
		now := metav1.Now()
		deleting.DeletionTimestamp = &now

		result, err := handler.onChange("ns/session", deleting)
		require.NoError(t, err)
		require.Same(t, deleting, result)
		require.False(t, topologyCalled)
		require.False(t, statusCalled)
	})

	t.Run("topology error surfaces", func(t *testing.T) {
		var topologyCalls int
		handler := &sessionHandler{
			ensureTopologyFn: func(*v1.Session) error {
				topologyCalls++
				return errors.New("boom")
			},
		}

		result, err := handler.onChange("ns/session", sess)
		require.EqualError(t, err, "boom")
		require.Same(t, sess, result)
		require.Equal(t, 1, topologyCalls)
	})

	t.Run("status result returned", func(t *testing.T) {
		updated := &v1.Session{ObjectMeta: metav1.ObjectMeta{Name: "updated"}}
		var (
			topologyCalls int
			statusCalls   int
		)
		handler := &sessionHandler{
			ensureTopologyFn: func(*v1.Session) error {
				topologyCalls++
				return nil
			},
			ensureStatusFn: func(*v1.Session) (*v1.Session, error) {
				statusCalls++
				return updated, nil
			},
		}

		result, err := handler.onChange("ns/session", sess)
		require.NoError(t, err)
		require.Same(t, updated, result)
		require.Equal(t, 1, topologyCalls)
		require.Equal(t, 1, statusCalls)
	})
}

func TestSessionHandlerOnRemove(t *testing.T) {
	t.Run("nil session", func(t *testing.T) {
		handler := &sessionHandler{}

		result, err := handler.onRemove("ns/session", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("returns original session", func(t *testing.T) {
		handler := &sessionHandler{}
		sess := &v1.Session{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "models"}}

		result, err := handler.onRemove("models/chat", sess)
		require.NoError(t, err)
		require.Same(t, sess, result)
	})
}

func TestSessionCheckHealthScenarios(t *testing.T) {
	t.Run("empty endpoint returns false", func(t *testing.T) {
		handler := &sessionHandler{ctx: context.Background(), httpClient: &http.Client{Timeout: time.Second}}
		require.False(t, handler.checkHealth(""))
	})

	t.Run("successful endpoint returns true", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/models", r.URL.Path)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)

		handler := &sessionHandler{
			ctx:        context.Background(),
			httpClient: server.Client(),
		}
		handler.httpClient.Timeout = time.Second

		require.True(t, handler.checkHealth(server.Listener.Addr().String()))
	})

	t.Run("non-successful status returns false", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/models", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(server.Close)

		handler := &sessionHandler{
			ctx:        context.Background(),
			httpClient: server.Client(),
		}
		handler.httpClient.Timeout = time.Second

		require.False(t, handler.checkHealth(server.Listener.Addr().String()))
	})

	t.Run("http client errors return false", func(t *testing.T) {
		handler := &sessionHandler{
			ctx: context.Background(),
			httpClient: &http.Client{
				Timeout: time.Second,
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("dial failed")
				}),
			},
		}

		require.False(t, handler.checkHealth("unreachable-host"))
	})

	t.Run("invalid endpoint returns false", func(t *testing.T) {
		handler := &sessionHandler{
			ctx:        context.Background(),
			httpClient: &http.Client{Timeout: time.Second},
		}

		require.False(t, handler.checkHealth("://bad-endpoint"))
	})
}

func TestSessionEnsureTopologyScalesUp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	createCalls := 0
	dispatchCalls := 0

	h := &sessionHandler{
		dllamas: dllamas,
		createDllamaFn: func(*v1.Session) error {
			createCalls++
			return nil
		},
		ensureDispatcherFn: func(*v1.Session) error {
			dispatchCalls++
			return nil
		},
		log: logrus.NewEntry(logrus.New()),
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return(nil, nil)

	sess := &v1.Session{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "ns"}, Spec: v1.SessionSpec{MinIdle: 2}}
	require.NoError(t, h.ensureTopology(sess))
	require.Equal(t, 1, createCalls)
	require.Equal(t, 1, dispatchCalls)
}

func TestSessionCheckHealthInvalidURL(t *testing.T) {
	h := &sessionHandler{ctx: context.Background(), httpClient: &http.Client{Timeout: time.Second}}
	require.False(t, h.checkHealth("bad\x00host"))
}

func TestScalingParamsClampMax(t *testing.T) {
	sess := &v1.Session{Spec: v1.SessionSpec{MinIdle: 5, MaxWorkers: 2}}
	params := scalingParamsFromSession(sess)
	require.Equal(t, int32(5), params.max)
}

func TestScalingParamsDefaultMin(t *testing.T) {
	sess := &v1.Session{Spec: v1.SessionSpec{MinIdle: 0}}
	params := scalingParamsFromSession(sess)
	require.Equal(t, int32(1), params.min)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSessionEnsureTopologyCreatesResources(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		ctx:     context.Background(),
		apply:   fakeApply,
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{}, nil)
	dllamas.EXPECT().Create(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(d *v1.Dllama) (*v1.Dllama, error) {
		copy := d.DeepCopy()
		copy.Name = "sess-dllama-001"
		if copy.Labels == nil {
			copy.Labels = map[string]string{}
		}
		copy.Labels[labelDllamaName] = sanitizeLabelValue(copy.Name)
		return copy, nil
	})

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess",
			Namespace: "ns",
			UID:       "uid-1",
		},
		Spec: v1.SessionSpec{
			Hash:            "hash",
			ModelRef:        v1.ModelReference{Kind: "Model", Name: "model"},
			RootImage:       "root:latest",
			WorkerImage:     "worker:latest",
			DispatcherImage: "dispatcher:latest",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
				StateStream:         "sessions.state",
			},
			NATS: &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	require.NoError(t, handler.ensureTopology(sess))
	require.Len(t, fakeApply.appliedObjects, 1, "dispatcher deployment should be applied")
}

func TestSessionEnsureTopologyScalesUpWhenBelowDesired(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	createCalls := 0
	dispatchCalls := 0
	handler.createDllamaFn = func(*v1.Session) error {
		createCalls++
		return nil
	}
	handler.ensureDispatcherFn = func(*v1.Session) error {
		dispatchCalls++
		return nil
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "models"},
		Spec: v1.SessionSpec{
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			Scaling: &v1.SessionScalingSpec{
				MinDllamas:     1,
				DesiredDllamas: 2,
			},
		},
		Status: v1.SessionStatus{
			Workers: []v1.SessionWorker{{Name: "chat-dllama-0"}},
		},
	}
	sess.UID = "sess-uid"

	desired := desiredDllamaSpecForSession(sess)
	readyDllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-dllama-0",
			Namespace: "models",
			Labels: map[string]string{
				labelSessionName:      sanitizeLabelValue(sess.Name),
				labelConversationHash: sanitizeLabelValue(sess.Spec.Hash),
				labelModelName:        sanitizeLabelValue(sess.Spec.ModelRef.Name),
				labelDllamaName:       sanitizeLabelValue("chat-dllama-0"),
			},
			Annotations: map[string]string{
				labelConversationHash: strings.TrimSpace(sess.Spec.Hash),
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sess, v1.SchemeGroupVersion.WithKind("Session"))},
		},
		Spec: desired,
		Status: v1.DllamaStatus{
			ReadyRoot: true,
			Conditions: []metav1.Condition{{
				Type:   conditionReady,
				Status: metav1.ConditionTrue,
			}},
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("models", gomock.Any()).Return([]*v1.Dllama{readyDllama}, nil)

	require.NoError(t, handler.ensureTopology(sess))
	require.Equal(t, 1, createCalls, "create hook should run when below desired replicas")
	require.Equal(t, 1, dispatchCalls, "dispatcher should run after scale up")
}

func TestSessionEnsureTopologyListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return(nil, errors.New("list failure"))

	sess := &v1.Session{ObjectMeta: metav1.ObjectMeta{Name: "sess", Namespace: "ns"}}

	err := handler.ensureTopology(sess)
	require.EqualError(t, err, "list failure")
}

func TestSessionEnsureTopologyPropagatesScaleUpError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	createErr := errors.New("create failure")
	handler.createDllamaFn = func(*v1.Session) error {
		return createErr
	}
	handler.ensureDispatcherFn = func(*v1.Session) error {
		t.Fatal("dispatcher should not run when create fails")
		return nil
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "models"},
		Spec: v1.SessionSpec{
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			Scaling: &v1.SessionScalingSpec{
				MinDllamas:     1,
				DesiredDllamas: 2,
			},
		},
		Status: v1.SessionStatus{
			Workers: []v1.SessionWorker{{Name: "chat-dllama-0"}},
		},
	}
	sess.UID = "sess-uid"

	desired := desiredDllamaSpecForSession(sess)
	readyDllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-dllama-0",
			Namespace: "models",
			Labels: map[string]string{
				labelSessionName:      sanitizeLabelValue(sess.Name),
				labelConversationHash: sanitizeLabelValue(sess.Spec.Hash),
				labelModelName:        sanitizeLabelValue(sess.Spec.ModelRef.Name),
				labelDllamaName:       sanitizeLabelValue("chat-dllama-0"),
			},
			Annotations: map[string]string{
				labelConversationHash: strings.TrimSpace(sess.Spec.Hash),
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sess, v1.SchemeGroupVersion.WithKind("Session"))},
		},
		Spec: desired,
		Status: v1.DllamaStatus{
			ReadyRoot: true,
			Conditions: []metav1.Condition{{
				Type:   conditionReady,
				Status: metav1.ConditionTrue,
			}},
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("models", gomock.Any()).Return([]*v1.Dllama{readyDllama}, nil)

	err := handler.ensureTopology(sess)
	require.ErrorIs(t, err, createErr)
}

func TestSessionEnsureTopologyReconcileError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	existing := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess-dllama-0",
			Namespace: "ns",
			Labels: map[string]string{
				labelSessionName: "sess",
			},
		},
		Spec: v1.DllamaSpec{
			ModelRef:    v1.ModelReference{Name: "model"},
			WorkerImage: "worker:v1",
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{existing}, nil)
	dllamas.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Dllama{})).Return(nil, errors.New("update failure"))

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess",
			Namespace: "ns",
		},
		Spec: v1.SessionSpec{
			Hash:            "hash",
			ModelRef:        v1.ModelReference{Name: "model"},
			RootImage:       "root:v2",
			WorkerImage:     "worker:v2",
			DispatcherImage: "dispatcher:v1",
		},
	}

	err := handler.ensureTopology(sess)
	require.EqualError(t, err, "update failure")
}

func TestSessionEnsureTopologyScalesUpWhenBacklogHasNoIdleSets(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		ctx:     context.Background(),
		apply:   fakeApply,
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
			UID:       "chat-uid",
		},
		Spec: v1.SessionSpec{
			Hash:            "hash",
			ModelRef:        v1.ModelReference{Name: "model"},
			RootImage:       "root:v2",
			WorkerImage:     "worker:v2",
			DispatcherImage: "dispatcher:v1",
			ReplicaPower:    1,
			NATS:            &v1.SessionNATSConfig{URL: "nats://demo:4222"},
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
				StateStream:         "sessions.state",
			},
			Scaling: &v1.SessionScalingSpec{
				MinDllamas:     1,
				MaxDllamas:     5,
				ScaleUpBacklog: 5,
			},
		},
		Status: v1.SessionStatus{
			Backlog: 20,
			Workers: []v1.SessionWorker{
				{
					Name:           "chat-dllama-0",
					ActiveMessages: 3,
				},
			},
		},
	}

	ownerRef := metav1.NewControllerRef(sess, v1.SchemeGroupVersion.WithKind("Session"))
	existing := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-dllama-0",
			Namespace: "models",
			Labels: map[string]string{
				labelSessionName:      "chat",
				labelConversationHash: "hash",
				labelModelName:        "model",
				labelDllamaName:       sanitizeLabelValue("chat-dllama-0"),
			},
			Annotations: map[string]string{
				labelConversationHash:              "hash",
				annotationSessionQueuePrefix:       "sessions.dl.",
				annotationSessionAssignmentsBucket: "sessions.assign",
				annotationSessionBacklogSubject:    "sessions.backlog",
				annotationSessionStateStream:       "sessions.state",
			},
			OwnerReferences: []metav1.OwnerReference{*ownerRef},
		},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Name: "model"},
			ReplicaPower: 1,
			RootImage:    "root:v2",
			WorkerImage:  "worker:v2",
			NATS:         &v1.DllamaNATSConfig{URL: "nats://demo:4222"},
		},
		Status: v1.DllamaStatus{
			ReadyRoot:  true,
			Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("models", gomock.Any()).Return([]*v1.Dllama{existing}, nil)
	dllamas.EXPECT().Create(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(d *v1.Dllama) (*v1.Dllama, error) {
		copy := d.DeepCopy()
		copy.Name = "chat-dllama-1"
		if copy.Labels == nil {
			copy.Labels = map[string]string{}
		}
		copy.Labels[labelDllamaName] = sanitizeLabelValue(copy.Name)
		return copy, nil
	})

	require.NoError(t, handler.ensureTopology(sess))
	require.Len(t, fakeApply.appliedObjects, 1, "dispatcher deployment should be reapplied after scale up")
}

func TestSessionEnsureTopologyScalesDownAndDeletesIdleDllama(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		apply:   newFakeApply(),
		log:     logrus.NewEntry(logrus.New()),
	}

	dllamas.EXPECT().Cache().Return(cache)
	session, readyList := sessionScaleDownFixtures()
	cache.EXPECT().List("models", gomock.Any()).Return(readyList, nil)
	dllamas.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Dllama{})).Return(nil, nil).AnyTimes()
	dllamas.EXPECT().Delete("models", "chat-dllama-0", gomock.Any()).Return(nil)

	err := handler.ensureTopology(session)
	require.NoError(t, err)
	require.Len(t, handler.apply.(*fakeApply).appliedObjects, 1, "dispatcher deployment should be applied after scaling down")
}

func TestSessionEnsureTopologyScaleDownDeleteError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		apply:   newFakeApply(),
		log:     logrus.NewEntry(logrus.New()),
	}

	dllamas.EXPECT().Cache().Return(cache)
	session, readyList := sessionScaleDownFixtures()
	cache.EXPECT().List("models", gomock.Any()).Return(readyList, nil)
	dllamas.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Dllama{})).Return(nil, nil).AnyTimes()
	dllamas.EXPECT().Delete("models", "chat-dllama-0", gomock.Any()).Return(errors.New("delete failure"))

	err := handler.ensureTopology(session)
	require.EqualError(t, err, "delete failure")
	require.Len(t, handler.apply.(*fakeApply).appliedObjects, 0, "dispatcher should not apply when delete fails")
}

func TestSessionEnsureTopologyNoScaleRunsDispatcher(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:     context.Background(),
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	handler.createDllamaFn = func(*v1.Session) error {
		t.Fatal("create hook should not run when pool size is steady")
		return nil
	}
	handler.deleteDllamaFn = func(*v1.Session, *v1.Dllama) error {
		t.Fatal("delete hook should not run when pool size is steady")
		return nil
	}

	dispatchCalls := 0
	handler.ensureDispatcherFn = func(*v1.Session) error {
		dispatchCalls++
		return nil
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "models"},
		Spec: v1.SessionSpec{
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			Scaling: &v1.SessionScalingSpec{
				MinDllamas:     1,
				MaxDllamas:     3,
				DesiredDllamas: 1,
			},
		},
		Status: v1.SessionStatus{
			Workers: []v1.SessionWorker{{Name: "chat-dllama-0"}},
		},
	}
	sess.UID = "sess-uid"

	desired := desiredDllamaSpecForSession(sess)
	readyDllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-dllama-0",
			Namespace: "models",
			Labels: map[string]string{
				labelSessionName:      sanitizeLabelValue(sess.Name),
				labelConversationHash: sanitizeLabelValue(sess.Spec.Hash),
				labelModelName:        sanitizeLabelValue(sess.Spec.ModelRef.Name),
				labelDllamaName:       sanitizeLabelValue("chat-dllama-0"),
			},
			Annotations: map[string]string{
				labelConversationHash: strings.TrimSpace(sess.Spec.Hash),
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sess, v1.SchemeGroupVersion.WithKind("Session"))},
		},
		Spec: desired,
		Status: v1.DllamaStatus{
			ReadyRoot: true,
			Conditions: []metav1.Condition{{
				Type:   conditionReady,
				Status: metav1.ConditionTrue,
			}},
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("models", gomock.Any()).Return([]*v1.Dllama{readyDllama}, nil)

	require.NoError(t, handler.ensureTopology(sess))
	require.Equal(t, 1, dispatchCalls, "dispatcher should still run when no scaling occurs")
}

func TestSessionEnsureTopologyCreateDllamaError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		ctx:      context.Background(),
		sessions: sessions,
		dllamas:  dllamas,
		log:      logrus.NewEntry(logrus.New()),
		apply:    newFakeApply(),
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{}, nil)
	dllamas.EXPECT().
		Create(gomock.AssignableToTypeOf(&v1.Dllama{})).
		Return(nil, errors.New("create failure"))
	sessions.EXPECT().Enqueue("ns", "sess")

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess",
			Namespace: "ns",
		},
		Spec: v1.SessionSpec{
			Hash:            "hash",
			ModelRef:        v1.ModelReference{Name: "model"},
			RootImage:       "root:v2",
			WorkerImage:     "worker:v2",
			DispatcherImage: "dispatcher:v1",
			NATS:            &v1.SessionNATSConfig{URL: "nats://demo:4222"},
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
			},
		},
	}

	err := handler.ensureTopology(sess)
	require.EqualError(t, err, "create failure")
}

func TestSessionEnsureDispatcherSkipsWithoutQueue(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		apply: fakeApply,
		log:   logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.NoError(t, err)
	require.Empty(t, fakeApply.appliedObjects)
}

func TestSessionEnsureStatusListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)

	handler := &sessionHandler{
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return(nil, errors.New("list failure"))

	sess := &v1.Session{ObjectMeta: metav1.ObjectMeta{Name: "sess", Namespace: "ns"}}

	result, err := handler.ensureStatus(sess)
	require.EqualError(t, err, "list failure")
	require.Same(t, sess, result)
}

func TestSessionEnsureStatusUpdatesAggregates(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)

	endpoints := map[string]string{
		"sess-dllama-0": "root.endpoint.svc",
	}
	healthChecks := map[string]bool{"root.endpoint.svc": true}

	handler := &sessionHandler{
		dllamas:  dllamas,
		sessions: sessions,
		log:      logrus.NewEntry(logrus.New()),
		lookupRootEndpointFn: func(_ string, dllamaName string) string {
			return endpoints[dllamaName]
		},
		checkHealthFn: func(endpoint string) bool {
			return healthChecks[endpoint]
		},
	}

	dllamaReady := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess-dllama-0",
			Namespace: "ns",
		},
		Status: v1.DllamaStatus{
			Conditions:   []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
			ReadyRoot:    true,
			ReadyWorkers: 4,
		},
	}
	dllamaPending := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess-dllama-1",
			Namespace: "ns",
		},
		Status: v1.DllamaStatus{
			Conditions:   []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionFalse}},
			ReadyRoot:    false,
			ReadyWorkers: 1,
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{dllamaReady, dllamaPending}, nil)

	var captured *v1.Session
	sessions.EXPECT().UpdateStatus(gomock.Any()).DoAndReturn(func(updated *v1.Session) (*v1.Session, error) {
		captured = updated
		return updated, nil
	})

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sess",
			Namespace:  "ns",
			Generation: 3,
		},
		Status: v1.SessionStatus{
			Workers: []v1.SessionWorker{
				{Name: "sess-dllama-0", ActiveMessages: 2},
			},
			ActiveRequests:     5,
			InFlight:           3,
			Backlog:            7,
			LastActivity:       &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
			ObservedGeneration: 1,
		},
	}

	result, err := handler.ensureStatus(sess)
	require.NoError(t, err)
	require.Same(t, captured, result)

	require.NotNil(t, captured)
	require.Equal(t, sess.Generation, captured.Status.ObservedGeneration)
	require.Equal(t, int32(5), captured.Status.ActiveRequests)
	require.Equal(t, int64(3), captured.Status.InFlight)
	require.Equal(t, int64(7), captured.Status.Backlog)
	require.Equal(t, int32(5), captured.Status.ReadyWorkers)
	require.Equal(t, int32(1), captured.Status.BusyWorkers)
	require.Equal(t, int32(0), captured.Status.AvailableWorkers)
	require.Len(t, captured.Status.Workers, 2)
	require.Len(t, captured.Status.Conditions, 1)
	require.Equal(t, metav1.ConditionTrue, captured.Status.Conditions[0].Status)
	require.Equal(t, "WorkersBusy", captured.Status.Conditions[0].Reason)
}

func TestSessionEnsureStatusNoChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)

	endpoints := map[string]string{
		"sess-dllama-0": "root.endpoint.svc",
	}
	healthChecks := map[string]bool{"root.endpoint.svc": true}

	handler := &sessionHandler{
		dllamas:  dllamas,
		sessions: sessions,
		log:      logrus.NewEntry(logrus.New()),
		lookupRootEndpointFn: func(_ string, dllamaName string) string {
			return endpoints[dllamaName]
		},
		checkHealthFn: func(endpoint string) bool {
			return healthChecks[endpoint]
		},
	}

	dllamaReady := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess-dllama-0",
			Namespace: "ns",
		},
		Status: v1.DllamaStatus{
			Conditions:   []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
			ReadyRoot:    true,
			ReadyWorkers: 1,
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{dllamaReady}, nil)
	sessions.EXPECT().UpdateStatus(gomock.Any()).Times(0)

	readyCondition := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "WorkersReady",
		Message: "Ready worker sets: 1",
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sess",
			Namespace:  "ns",
			Generation: 2,
		},
		Status: v1.SessionStatus{
			ObservedGeneration: 2,
			ReadyWorkers:       1,
			BusyWorkers:        0,
			AvailableWorkers:   1,
			Workers: []v1.SessionWorker{
				{
					Name:     "sess-dllama-0",
					Ready:    true,
					Phase:    "Ready",
					Healthy:  true,
					Endpoint: "root.endpoint.svc",
				},
			},
			Conditions: []metav1.Condition{readyCondition},
		},
	}

	result, err := handler.ensureStatus(sess)
	require.NoError(t, err)
	require.Same(t, sess, result)
}

func TestSessionEnsureStatusHandlesDeletingDllama(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)

	handler := &sessionHandler{
		dllamas:  dllamas,
		sessions: sessions,
		log:      logrus.NewEntry(logrus.New()),
		lookupRootEndpointFn: func(_, _ string) string {
			return ""
		},
		checkHealthFn: func(string) bool {
			return false
		},
	}

	deleting := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sess-dllama-0",
			Namespace:         "ns",
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Status: v1.DllamaStatus{
			Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionFalse}},
			ReadyRoot:  false,
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{deleting}, nil)

	var captured *v1.Session
	sessions.EXPECT().UpdateStatus(gomock.Any()).DoAndReturn(func(updated *v1.Session) (*v1.Session, error) {
		captured = updated
		return updated, nil
	})

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sess",
			Namespace:  "ns",
			Generation: 2,
		},
		Status: v1.SessionStatus{
			Workers: []v1.SessionWorker{
				{Name: "sess-dllama-0"},
			},
		},
	}

	result, err := handler.ensureStatus(sess)
	require.NoError(t, err)
	require.Same(t, captured, result)
	require.Len(t, captured.Status.Workers, 1)
	require.Equal(t, "Terminating", captured.Status.Workers[0].Phase)
	require.False(t, captured.Status.Workers[0].Healthy)
	require.Equal(t, "", captured.Status.Workers[0].Endpoint)
}

func TestSessionEnsureStatusMarksWorkerUnhealthyWhenHealthChecksFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)

	handler := &sessionHandler{
		dllamas:  dllamas,
		sessions: sessions,
		log:      logrus.NewEntry(logrus.New()),
		lookupRootEndpointFn: func(_, _ string) string {
			return "root.endpoint.svc"
		},
		checkHealthFn: func(string) bool {
			return false
		},
	}

	ready := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sess-dllama-0",
			Namespace: "ns",
		},
		Status: v1.DllamaStatus{
			Conditions:   []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
			ReadyRoot:    true,
			ReadyWorkers: 2,
		},
	}

	dllamas.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("ns", gomock.Any()).Return([]*v1.Dllama{ready}, nil)

	var captured *v1.Session
	sessions.EXPECT().UpdateStatus(gomock.Any()).DoAndReturn(func(updated *v1.Session) (*v1.Session, error) {
		captured = updated
		return updated, nil
	})

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sess",
			Namespace:  "ns",
			Generation: 4,
		},
		Status: v1.SessionStatus{},
	}

	result, err := handler.ensureStatus(sess)
	require.NoError(t, err)
	require.Same(t, captured, result)

	require.Len(t, captured.Status.Workers, 1)
	worker := captured.Status.Workers[0]
	require.Equal(t, "sess-dllama-0", worker.Name)
	require.Equal(t, "root.endpoint.svc", worker.Endpoint)
	require.False(t, worker.Healthy, "health check failure should mark worker unhealthy")
	require.Equal(t, "Ready", worker.Phase)
	require.True(t, worker.Ready)
	require.Len(t, captured.Status.Conditions, 1)
	require.Equal(t, metav1.ConditionTrue, captured.Status.Conditions[0].Status)
	require.Equal(t, "WorkersReady", captured.Status.Conditions[0].Reason)
}

func TestSessionCreateDllamaForSessionCreateError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	handler := &sessionHandler{dllamas: dllamas, sessions: sessions}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
			UID:       "uid-1",
		},
		Spec: v1.SessionSpec{
			Hash:        "hash",
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS:        &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	sentinel := errors.New("create failed")
	dllamas.EXPECT().Create(gomock.AssignableToTypeOf(&v1.Dllama{})).Return(nil, sentinel)
	sessions.EXPECT().Enqueue("models", "chat")

	err := handler.createDllamaForSession(sess)
	require.ErrorIs(t, err, sentinel)
}

func TestSessionCreateDllamaForSessionCreateErrorRequeuesEachAttempt(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	handler := &sessionHandler{dllamas: dllamas, sessions: sessions}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
			UID:       "uid-1",
		},
		Spec: v1.SessionSpec{
			Hash:        "hash",
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS:        &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	sentinel := errors.New("create failed")
	dllamas.EXPECT().Create(gomock.AssignableToTypeOf(&v1.Dllama{})).Return(nil, sentinel).Times(2)
	sessions.EXPECT().Enqueue("models", "chat").Times(2)

	err := handler.createDllamaForSession(sess)
	require.ErrorIs(t, err, sentinel)

	err = handler.createDllamaForSession(sess)
	require.ErrorIs(t, err, sentinel)
}

func TestSessionCreateDllamaForSessionCreateAlreadyExists(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	handler := &sessionHandler{dllamas: dllamas, sessions: sessions}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
			UID:       "uid-1",
		},
		Spec: v1.SessionSpec{
			Hash:        "hash",
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS:        &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	dllamas.EXPECT().
		Create(gomock.AssignableToTypeOf(&v1.Dllama{})).
		Return(nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: "koldun.gorizond.io", Resource: "dllamas"}, "chat"))
	sessions.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

	err := handler.createDllamaForSession(sess)
	require.NoError(t, err)
}

func TestSessionCreateDllamaForSessionUpdateError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	handler := &sessionHandler{dllamas: dllamas, sessions: sessions}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
			UID:       "uid-1",
		},
		Spec: v1.SessionSpec{
			Hash:        "hash",
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS:        &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	createReturn := func(d *v1.Dllama) (*v1.Dllama, error) {
		copy := d.DeepCopy()
		copy.Name = "chat-dllama-q1"
		copy.Labels = nil
		return copy, nil
	}

	sentinel := errors.New("update failed")

	dllamas.EXPECT().
		Create(gomock.AssignableToTypeOf(&v1.Dllama{})).
		DoAndReturn(createReturn)
	dllamas.EXPECT().
		Update(gomock.AssignableToTypeOf(&v1.Dllama{})).
		Return(nil, sentinel)
	sessions.EXPECT().Enqueue("models", "chat")

	err := handler.createDllamaForSession(sess)
	require.ErrorIs(t, err, sentinel)
}

func TestSessionCreateDllamaForSessionUpdateNotFoundIgnored(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	handler := &sessionHandler{dllamas: dllamas, sessions: sessions}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
			UID:       "uid-1",
		},
		Spec: v1.SessionSpec{
			Hash:        "hash",
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS:        &v1.SessionNATSConfig{URL: "nats://demo:4222"},
		},
	}

	createReturn := func(d *v1.Dllama) (*v1.Dllama, error) {
		copy := d.DeepCopy()
		copy.Name = "chat-dllama-q1"
		copy.Labels = map[string]string{}
		return copy, nil
	}

	dllamas.EXPECT().
		Create(gomock.AssignableToTypeOf(&v1.Dllama{})).
		DoAndReturn(createReturn)
	dllamas.EXPECT().
		Update(gomock.AssignableToTypeOf(&v1.Dllama{})).
		Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "koldun.gorizond.io", Resource: "dllamas"}, "chat-dllama-q1"))
	sessions.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

	err := handler.createDllamaForSession(sess)
	require.NoError(t, err)
}

func TestSessionReconcileDllamaUpdateNotFoundIgnored(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	handler := &sessionHandler{
		dllamas: dllamas,
		log:     logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash:        "hash",
			ModelRef:    v1.ModelReference{Name: "model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS:        &v1.SessionNATSConfig{URL: "nats://demo:4222"},
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
			},
		},
	}

	existing := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-dllama-0",
			Namespace: "models",
		},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Name: "stale"},
			ReplicaPower: 1,
			RootImage:    "root:old",
			WorkerImage:  "worker:old",
			NATS:         &v1.DllamaNATSConfig{URL: "nats://demo:4222"},
		},
	}

	dllamas.EXPECT().
		Update(gomock.AssignableToTypeOf(&v1.Dllama{})).
		Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "dllamas"}, "chat-dllama-0"))

	require.NoError(t, handler.reconcileDllama(sess, existing))
}

func TestSessionEnsureDispatcherMissingImage(t *testing.T) {

	handler := &sessionHandler{
		apply: newFakeApply(),
		log:   logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
			},
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			DispatcherImage: "",
			RootImage:       "",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dispatcher image missing")
}

func TestSessionEnsureDispatcherSkipsWithoutNATS(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		apply: fakeApply,
		log:   logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
			},
			DispatcherImage: "dispatcher:latest",
			RootImage:       "root:latest",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.NoError(t, err)
	require.Empty(t, fakeApply.appliedObjects, "dispatcher should not be applied without NATS")
}

func TestSessionEnsureDispatcherSkipsWithBlankNATSURL(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		apply: fakeApply,
		log:   logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
			},
			NATS: &v1.SessionNATSConfig{
				URL: "   ",
			},
			DispatcherImage: "dispatcher:latest",
			RootImage:       "root:latest",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.NoError(t, err)
	require.Empty(t, fakeApply.appliedObjects, "dispatcher should not be applied when NATS URL is blank")
}

func TestSessionEnsureDispatcherSkipsWithIncompleteQueue(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		apply: fakeApply,
		log:   logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "   ",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
			},
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			DispatcherImage: "dispatcher:latest",
			RootImage:       "root:latest",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.NoError(t, err)
	require.Empty(t, fakeApply.appliedObjects, "dispatcher should not be applied with incomplete queue config")
}

func TestSessionEnsureDispatcherApplyError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)

	applyErr := errors.New("apply failed")
	handler := &sessionHandler{
		apply:    &failingApply{fakeApply: newFakeApply(), err: applyErr},
		log:      logrus.NewEntry(logrus.New()),
		sessions: sessions,
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
				StateStream:         "sessions.state",
			},
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			DispatcherImage: "dispatcher:latest",
			RootImage:       "root:latest",
		},
	}

	sessions.EXPECT().Enqueue("models", "chat")

	err := handler.ensureDispatcher(sess)
	require.ErrorIs(t, err, applyErr)
}

func TestSessionEnsureDispatcherApplyErrorRequeuesEachAttempt(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)

	applyErr := errors.New("apply failed")
	handler := &sessionHandler{
		apply:    &failingApply{fakeApply: newFakeApply(), err: applyErr},
		log:      logrus.NewEntry(logrus.New()),
		sessions: sessions,
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
				StateStream:         "sessions.state",
			},
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			DispatcherImage: "dispatcher:latest",
			RootImage:       "root:latest",
		},
	}

	sessions.EXPECT().Enqueue("models", "chat").Times(2)

	err := handler.ensureDispatcher(sess)
	require.ErrorIs(t, err, applyErr)

	err = handler.ensureDispatcher(sess)
	require.ErrorIs(t, err, applyErr)
}

func TestSessionEnsureDispatcherHonorsAckTimeout(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		apply: fakeApply,
		log:   logrus.NewEntry(logrus.New()),
	}

	timeout := metav1.Duration{Duration: 45 * time.Second}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
				StateStream:         "sessions.state",
				AckTimeout:          &timeout,
			},
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			DispatcherImage: "dispatcher:latest",
			RootImage:       "root:latest",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.NoError(t, err)
	require.Len(t, fakeApply.appliedObjects, 1)

	deployment, ok := fakeApply.appliedObjects[0].(*appsv1.Deployment)
	require.True(t, ok, "expected dispatcher deployment to be applied")
	require.NotEmpty(t, deployment.Spec.Template.Spec.Containers)
	args := deployment.Spec.Template.Spec.Containers[0].Args
	require.Contains(t, args, "--dispatcher-ack-wait=45s")
}

func TestSessionEnsureDispatcherUsesStateStreamAndRootImageFallback(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &sessionHandler{
		apply: fakeApply,
		log:   logrus.NewEntry(logrus.New()),
	}

	sess := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash: "hash",
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions",
				StateStream:         "streams.state",
			},
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			DispatcherImage: "",
			RootImage:       "root:latest",
		},
	}

	err := handler.ensureDispatcher(sess)
	require.NoError(t, err)
	require.Len(t, fakeApply.appliedObjects, 1)

	deployment, ok := fakeApply.appliedObjects[0].(*appsv1.Deployment)
	require.True(t, ok, "dispatcher deployment should be applied")
	require.NotEmpty(t, deployment.Spec.Template.Spec.Containers)
	container := deployment.Spec.Template.Spec.Containers[0]
	require.Equal(t, "root:latest", container.Image, "root image should be used as fallback")
	require.Contains(t, container.Args, "--dispatcher-dllama-prefix=sessions.", "dllama prefix should gain trailing dot")
	require.Contains(t, container.Args, "--dispatcher-state-prefix=streams.state.", "state stream should override state prefix when it contains dots")
}

func sessionScaleDownFixtures() (*v1.Session, []*v1.Dllama) {
	now := time.Now()

	session := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat",
			Namespace: "models",
		},
		Spec: v1.SessionSpec{
			Hash:            "hash",
			ModelRef:        v1.ModelReference{Name: "model"},
			RootImage:       "root:latest",
			WorkerImage:     "worker:latest",
			DispatcherImage: "dispatcher:latest",
			NATS: &v1.SessionNATSConfig{
				URL: "nats://demo:4222",
			},
			Queue: &v1.SessionQueueSpec{
				BacklogSubject:      "sessions.backlog",
				AssignmentsBucket:   "sessions.assign",
				DllamaSubjectPrefix: "sessions.dl",
				StateStream:         "sessions.state",
			},
			Scaling: &v1.SessionScalingSpec{
				MinDllamas:           1,
				MaxDllamas:           5,
				ScaleDownIdleSeconds: 30,
			},
		},
		Status: v1.SessionStatus{
			Workers: []v1.SessionWorker{
				{
					Name:           "chat-dllama-0",
					ActiveMessages: 0,
					LastHeartbeat:  &metav1.Time{Time: now.Add(-10 * time.Minute)},
				},
				{
					Name:           "chat-dllama-1",
					ActiveMessages: 2,
					LastHeartbeat:  &metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			LastActivity: &metav1.Time{Time: now.Add(-1 * time.Hour)},
		},
	}

	spec := desiredDllamaSpecForSession(session)

	idleDllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "chat-dllama-0",
			Namespace:         "models",
			CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			Labels: map[string]string{
				labelSessionName: "chat",
			},
		},
		Spec: spec,
		Status: v1.DllamaStatus{
			Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
			ReadyRoot:  true,
		},
	}

	busyDllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "chat-dllama-1",
			Namespace:         "models",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
			Labels: map[string]string{
				labelSessionName: "chat",
			},
		},
		Spec: spec,
		Status: v1.DllamaStatus{
			Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
			ReadyRoot:  true,
		},
	}

	return session, []*v1.Dllama{idleDllama, busyDllama}
}

func TestSessionDeleteDllamaPropagatesError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dllamas := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)
	handler := &sessionHandler{dllamas: dllamas}

	target := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-dllama-1",
			Namespace: "models",
		},
	}

	dllamas.EXPECT().Delete("models", "chat-dllama-1", gomock.Any()).Return(errors.New("delete failure"))

	err := handler.deleteDllama(&v1.Session{}, target)
	require.EqualError(t, err, "delete failure")
}

func TestSessionDeleteDllamaUsesHook(t *testing.T) {
	called := 0
	handler := &sessionHandler{
		deleteDllamaFn: func(sess *v1.Session, dllama *v1.Dllama) error {
			require.Equal(t, "chat", sess.Name)
			require.Equal(t, "chat-dllama-0", dllama.Name)
			called++
			return nil
		},
	}

	sess := &v1.Session{ObjectMeta: metav1.ObjectMeta{Name: "chat"}}
	target := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "chat-dllama-0"}}

	require.NoError(t, handler.deleteDllama(sess, target))
	require.Equal(t, 1, called, "hook should be invoked exactly once")
}
