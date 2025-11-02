package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/apply/injectors"
	"github.com/rancher/wrangler/v3/pkg/generic"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/rancher/wrangler/v3/pkg/objectset"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestConversationSyncAppliesSessionsAndDeletesStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessionsController := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	sessionCache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessionsController.EXPECT().Cache().Return(sessionCache).AnyTimes()

	recordA := &conversation.Record{
		Hash:        "hash-a",
		Session:     "active-session",
		Namespace:   "default",
		Model:       "models/demo",
		RootImage:   "ghcr.io/demo/root:latest",
		WorkerImage: "ghcr.io/demo/worker:latest",
	}
	recordB := &conversation.Record{
		Hash:        "hash-b",
		Session:     "second-session",
		Namespace:   "default",
		Model:       "models/other",
		RootImage:   "ghcr.io/demo/root:latest",
		WorkerImage: "ghcr.io/demo/worker:latest",
	}

	payloads := make(map[string][]byte)
	for _, record := range []*conversation.Record{recordA, recordB} {
		data, err := record.Marshal()
		require.NoError(t, err)
		payloads[fmt.Sprintf("nats_ttl_%s", record.Hash)] = data
	}

	kv := &fakeMemoryKV{
		bucket:  "sessions",
		keys:    []string{"nats_ttl_hash-a", "nats_ttl_hash-b"},
		payload: payloads,
	}

	activeSession := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      recordA.SessionName(),
			Namespace: recordA.Namespace,
			Labels: map[string]string{
				labelConversationHash: recordA.Hash,
			},
		},
		Spec: v1.SessionSpec{
			Hash: recordA.Hash,
		},
	}
	staleSession := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale-session",
			Namespace: "default",
		},
		Spec: v1.SessionSpec{
			Hash: "stale-hash",
		},
	}

	sessionCache.EXPECT().
		List("", gomock.Any()).
		Return([]*v1.Session{activeSession, staleSession}, nil)

	sessionsController.EXPECT().
		Delete("default", "stale-session", gomock.Any()).
		Return(nil)

	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			TTLPrefix: "nats_ttl_",
		},
		log:      logrus.New().WithField("component", "conversation-sync-test"),
		sessions: sessionsController,
		apply:    fakeApply,
		kv:       kv,
	}

	reconciler.sync(context.Background())

	require.Len(t, fakeApply.appliedObjects, 2, "expected two sessions to be applied")

	applied := map[string]struct{}{}
	for _, obj := range fakeApply.appliedObjects {
		session, ok := obj.(*v1.Session)
		require.True(t, ok, "expected Session object")
		applied[fmt.Sprintf("%s/%s", session.Namespace, session.Name)] = struct{}{}
		require.NotEmpty(t, session.Spec.Hash, "session hash should be populated")
		require.NotEmpty(t, session.Spec.ModelRef.Name, "model reference should be populated")
	}

	require.Contains(t, applied, fmt.Sprintf("%s/%s", recordA.Namespace, recordA.SessionName()))
	require.Contains(t, applied, fmt.Sprintf("%s/%s", recordB.Namespace, recordB.SessionName()))
}

func TestConversationSyncDeletesWhenNoKeysFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessionsController := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	sessionCache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessionsController.EXPECT().Cache().Return(sessionCache).AnyTimes()

	stale := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-session",
			Namespace: "conversations",
		},
		Spec: v1.SessionSpec{
			Hash: "legacy-hash",
		},
	}

	sessionCache.EXPECT().
		List("", gomock.Any()).
		Return([]*v1.Session{stale}, nil)

	sessionsController.EXPECT().
		Delete("conversations", "legacy-session", gomock.Any()).
		Return(nil)

	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			TTLPrefix: "nats_ttl_",
		},
		log:      logrus.New().WithField("component", "conversation-sync-test"),
		sessions: sessionsController,
		apply:    fakeApply,
		kv: &fakeMemoryKV{
			keysErr: nats.ErrNoKeysFound,
		},
	}

	reconciler.sync(context.Background())

	require.Empty(t, fakeApply.appliedObjects, "no sessions should be applied when KV empty")
}

func TestConversationSyncSkipsOnKeyError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessionsController := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	sessionCache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)

	sessionsController.EXPECT().Cache().Return(sessionCache).AnyTimes()

	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			TTLPrefix: "nats_ttl_",
		},
		log:      logrus.New().WithField("component", "conversation-sync-test"),
		sessions: sessionsController,
		apply:    fakeApply,
		kv: &fakeMemoryKV{
			keysErr: errors.New("kv lookup failed"),
		},
	}

	reconciler.sync(context.Background())

	require.Empty(t, fakeApply.appliedObjects, "no apply operations should occur on KV error")
}

func TestEnsureConversationBucketCreatesAndReuses(t *testing.T) {
	srv := runJetStreamServer(t)
	nc, err := nats.Connect(srv.ClientURL(), nats.Name("conversation-bucket-test"))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	require.NoError(t, err)

	bucket := fmt.Sprintf("test-bucket-%d", time.Now().UnixNano())

	kv, err := ensureConversationBucket(js, bucket)
	require.NoError(t, err)
	require.NotNil(t, kv)

	rev, err := kv.PutString("greeting", "world")
	require.NoError(t, err)
	require.Greater(t, rev, uint64(0))

	kvExisting, err := ensureConversationBucket(js, bucket)
	require.NoError(t, err)
	require.NotNil(t, kvExisting)

	entry, err := kvExisting.Get("greeting")
	require.NoError(t, err)
	require.Equal(t, "world", string(entry.Value()))
}

func TestEnsureConversationBucketRejectsEmptyName(t *testing.T) {
	_, err := ensureConversationBucket(nil, "   ")
	require.Error(t, err)
	require.Contains(t, err.Error(), "bucket name cannot be empty")
}

func TestEnsureConversationBucketPropagatesLookupError(t *testing.T) {
	srv := runJetStreamServer(t)
	nc, err := nats.Connect(srv.ClientURL(), nats.Name("conversation-bucket-error"))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	require.NoError(t, err)

	_, err = ensureConversationBucket(js, "invalid bucket name")
	require.Error(t, err)
	require.ErrorIs(t, err, nats.ErrInvalidBucketName)
}

func TestStartConversationReconcilerReturnsDialError(t *testing.T) {
	cfg := ConversationConfig{
		NATSURL:  "nats://should-not-connect:4222",
		KVBucket: "test-bucket",
	}
	cfg.dialer = func(string, ...nats.Option) (natsConnection, error) {
		return nil, errors.New("dial failure")
	}

	err := StartConversationReconciler(context.Background(), &Manager{}, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connect NATS")
	require.Contains(t, err.Error(), "dial failure")
}

func TestStartConversationReconcilerClosesOnJetStreamError(t *testing.T) {
	cfg := ConversationConfig{
		NATSURL:  "nats://example:4222",
		KVBucket: "test-bucket",
	}
	fakeConn := &fakeNATSConn{
		jsErr: errors.New("jetstream unavailable"),
	}
	cfg.dialer = func(string, ...nats.Option) (natsConnection, error) {
		return fakeConn, nil
	}

	err := StartConversationReconciler(context.Background(), &Manager{}, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "jetstream context")
	require.True(t, fakeConn.closed, "connection should be closed on jetstream error")
	require.False(t, fakeConn.drained, "drain should not run on jetstream error")
}

func TestStartConversationReconcilerClosesOnBucketError(t *testing.T) {
	srv := runJetStreamServer(t)

	cfg := ConversationConfig{
		NATSURL:  srv.ClientURL(),
		KVBucket: "invalid bucket name",
	}

	var tracked *trackingConn
	cfg.dialer = func(url string, opts ...nats.Option) (natsConnection, error) {
		conn, err := nats.Connect(url, opts...)
		if err != nil {
			return nil, err
		}
		tracked = newTrackingConn(conn)
		return tracked, nil
	}

	err := StartConversationReconciler(context.Background(), &Manager{}, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "kv bucket")
	require.NotNil(t, tracked, "dialer should have captured the connection")
	require.True(t, tracked.Closed(), "connection should be closed on bucket failure")
	require.False(t, tracked.Drained(), "drain should not run on setup failure")
}

func TestStartConversationReconcilerDrainsOnContextCancel(t *testing.T) {
	srv := runJetStreamServer(t)

	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)

	sessionController := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](mockCtrl)
	sessionCache := genericfake.NewMockCacheInterface[*v1.Session](mockCtrl)

	sessionController.EXPECT().Cache().Return(sessionCache).AnyTimes()
	sessionCache.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	sessionController.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	manager := &Manager{
		Kold: &fakeKoldInterface{
			session: sessionController,
		},
		apply: newFakeApply(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     fmt.Sprintf("session-bucket-%d", time.Now().UnixNano()),
		PollInterval: 5 * time.Millisecond,
	}

	var tracked *trackingConn
	cfg.dialer = func(url string, opts ...nats.Option) (natsConnection, error) {
		conn, err := nats.Connect(url, opts...)
		if err != nil {
			return nil, err
		}
		tracked = newTrackingConn(conn)
		return tracked, nil
	}

	err := StartConversationReconciler(ctx, manager, cfg)
	require.NoError(t, err)
	require.NotNil(t, tracked, "tracking connection should be initialised")

	cancel()

	require.Eventually(t, func() bool {
		return tracked.Drained()
	}, 2*time.Second, 10*time.Millisecond, "connection was not drained after cancellation")

	require.True(t, tracked.Closed(), "connection must be closed after drain")
}

func runJetStreamServer(t *testing.T) *server.Server {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		Host:      "127.0.0.1",
		Port:      -1,
	}

	srv, err := server.NewServer(opts)
	require.NoError(t, err)

	go srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		require.FailNow(t, "nats server not ready for connections")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return srv
}

type trackingConn struct {
	*nats.Conn

	mu      sync.Mutex
	drained bool
	closed  bool
}

func newTrackingConn(conn *nats.Conn) *trackingConn {
	return &trackingConn{Conn: conn}
}

func (t *trackingConn) JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	return t.Conn.JetStream(opts...)
}

func (t *trackingConn) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.Conn.Close()
}

func (t *trackingConn) Drain() error {
	t.mu.Lock()
	t.drained = true
	t.mu.Unlock()
	err := t.Conn.Drain()
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return err
}

func (t *trackingConn) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *trackingConn) Drained() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.drained
}

type fakeNATSConn struct {
	jsErr   error
	closed  bool
	drained bool
}

func (f *fakeNATSConn) JetStream(...nats.JSOpt) (nats.JetStreamContext, error) {
	return nil, f.jsErr
}

func (f *fakeNATSConn) Close() {
	f.closed = true
}

func (f *fakeNATSConn) Drain() error {
	f.drained = true
	return nil
}

type fakeKoldInterface struct {
    dllama  generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
    model   generic.ControllerInterface[*v1.Model, *v1.ModelList]
    root    generic.ControllerInterface[*v1.Root, *v1.RootList]
    worker  generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
    ingress generic.ControllerInterface[*v1.Ingress, *v1.IngressList]
    session generic.ControllerInterface[*v1.Session, *v1.SessionList]
}

func (f *fakeKoldInterface) Dllama() generic.ControllerInterface[*v1.Dllama, *v1.DllamaList] {
    return f.dllama
}

func (f *fakeKoldInterface) Model() generic.ControllerInterface[*v1.Model, *v1.ModelList] {
    return f.model
}

func (f *fakeKoldInterface) Root() generic.ControllerInterface[*v1.Root, *v1.RootList] {
    return f.root
}

func (f *fakeKoldInterface) Worker() generic.ControllerInterface[*v1.Worker, *v1.WorkerList] {
    return f.worker
}

func (f *fakeKoldInterface) Ingress() generic.ControllerInterface[*v1.Ingress, *v1.IngressList] {
    return f.ingress
}

func (f *fakeKoldInterface) Session() generic.ControllerInterface[*v1.Session, *v1.SessionList] {
    return f.session
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

type fakeMemoryKV struct {
	bucket  string
	keys    []string
	payload map[string][]byte
	keysErr error

	putCalls    []string
	deleteCalls []string
	putErr      error
	deleteErr   error
	revision    uint64
}

func (f *fakeMemoryKV) Get(key string) (nats.KeyValueEntry, error) {
	if f == nil {
		return nil, nats.ErrBucketNotFound
	}
	value, ok := f.payload[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return &fakeKVEntry{
		bucket:   f.bucket,
		key:      key,
		value:    append([]byte(nil), value...),
		revision: 1,
	}, nil
}

func (f *fakeMemoryKV) GetRevision(string, uint64) (nats.KeyValueEntry, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMemoryKV) Put(key string, value []byte) (uint64, error) {
	if f == nil {
		return 0, errors.New("kv not initialised")
	}
	if f.putErr != nil {
		return 0, f.putErr
	}
	if f.payload == nil {
		f.payload = make(map[string][]byte)
	}
	if f.keys == nil {
		f.keys = []string{}
	}
	if _, ok := f.payload[key]; !ok {
		found := false
		for _, existing := range f.keys {
			if existing == key {
				found = true
				break
			}
		}
		if !found {
			f.keys = append(f.keys, key)
		}
	}
	f.payload[key] = append([]byte(nil), value...)
	f.putCalls = append(f.putCalls, key)
	f.revision++
	return f.revision, nil
}

func (f *fakeMemoryKV) PutString(string, string) (uint64, error) {
	return 0, errors.New("not implemented")
}

func (f *fakeMemoryKV) Create(string, []byte) (uint64, error) {
	return 0, errors.New("not implemented")
}

func (f *fakeMemoryKV) Update(string, []byte, uint64) (uint64, error) {
	return 0, errors.New("not implemented")
}

func (f *fakeMemoryKV) Delete(key string, _ ...nats.DeleteOpt) error {
	if f == nil {
		return errors.New("kv not initialised")
	}
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if f.payload == nil {
		return nats.ErrKeyNotFound
	}
	if _, ok := f.payload[key]; !ok {
		return nats.ErrKeyNotFound
	}
	delete(f.payload, key)
	f.deleteCalls = append(f.deleteCalls, key)
	for i, existing := range f.keys {
		if existing == key {
			f.keys = append(f.keys[:i], f.keys[i+1:]...)
			break
		}
	}
	return nil
}

func (f *fakeMemoryKV) Purge(string, ...nats.DeleteOpt) error {
	return errors.New("not implemented")
}

func (f *fakeMemoryKV) Watch(string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMemoryKV) WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMemoryKV) WatchFiltered([]string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMemoryKV) Keys(...nats.WatchOpt) ([]string, error) {
	if f == nil {
		return nil, nats.ErrBucketNotFound
	}
	if f.keysErr != nil {
		return nil, f.keysErr
	}
	if len(f.keys) == 0 && len(f.payload) > 0 {
		keys := make([]string, 0, len(f.payload))
		for key := range f.payload {
			keys = append(keys, key)
		}
		return keys, nil
	}
	return append([]string(nil), f.keys...), nil
}

func (f *fakeMemoryKV) ListKeys(...nats.WatchOpt) (nats.KeyLister, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMemoryKV) History(string, ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMemoryKV) Bucket() string {
	if f == nil || f.bucket == "" {
		return "test"
	}
	return f.bucket
}

func (f *fakeMemoryKV) PurgeDeletes(...nats.PurgeOpt) error {
	return errors.New("not implemented")
}

func (f *fakeMemoryKV) Status() (nats.KeyValueStatus, error) {
	return nil, errors.New("not implemented")
}

type fakeKVEntry struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
}

func (e *fakeKVEntry) Bucket() string { return e.bucket }
func (e *fakeKVEntry) Key() string    { return e.key }
func (e *fakeKVEntry) Value() []byte  { return e.value }
func (e *fakeKVEntry) Revision() uint64 {
	if e.revision == 0 {
		return 1
	}
	return e.revision
}
func (e *fakeKVEntry) Created() time.Time { return time.Time{} }
func (e *fakeKVEntry) Delta() uint64      { return 0 }
func (e *fakeKVEntry) Operation() nats.KeyValueOp {
	return nats.KeyValuePut
}

var _ nats.KeyValue = (*fakeMemoryKV)(nil)
var _ nats.KeyValueEntry = (*fakeKVEntry)(nil)
