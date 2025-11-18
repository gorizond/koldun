package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/gorizond/koldun/pkg/testutil"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestEnsureSessionValidatesRecord(t *testing.T) {

	base := conversation.Record{
		Hash:        "hash",
		Session:     "session",
		Namespace:   "default",
		Model:       "model",
		RootImage:   "ghcr.io/root:latest",
		WorkerImage: "ghcr.io/worker:latest",
	}
	clone := func() *conversation.Record {
		copy := base
		return &copy
	}

	tests := []struct {
		name    string
		mutate  func(*conversation.Record)
		wantErr string
	}{
		{
			name: "missing session name",
			mutate: func(r *conversation.Record) {
				r.Hash = ""
				r.Session = ""
			},
			wantErr: "session name missing",
		},
		{
			name: "missing model name",
			mutate: func(r *conversation.Record) {
				r.Model = " "
			},
			wantErr: "model name missing",
		},
		{
			name: "missing images",
			mutate: func(r *conversation.Record) {
				r.RootImage = " "
			},
			wantErr: "images missing",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)
			reconciler := &conversationReconciler{apply: newGomockApply(ctrl)}

			record := clone()
			tt.mutate(record)
			err := reconciler.ensureSession(record)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestEnsureSessionAppliesSessionFromRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApply := newGomockApply(ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, apierrors.NewNotFound(schema.GroupResource{}, "")).AnyTimes()
	reconciler := &conversationReconciler{apply: mockApply, sessions: sessions}

	const secretName = "nats-credentials"
	hash := strings.Repeat("abc", 30)
	record := &conversation.Record{
		Hash:                    hash,
		Session:                 "conversation-session",
		Namespace:               "tenant-a",
		Model:                   "models-ns/instruct",
		ReplicaPower:            0,
		RootImage:               "ghcr.io/root:stable",
		WorkerImage:             "ghcr.io/worker:stable",
		DispatcherImage:         "ghcr.io/dispatcher:stable",
		DispatcherMetricsListen: ":9090",
		Scaling: &conversation.SessionScalingConfig{
			MinDllamas:           2,
			MaxDllamas:           5,
			DesiredDllamas:       4,
			ScaleUpBacklog:       3,
			ScaleDownIdleSeconds: 15,
		},
		Queue: &conversation.QueueConfig{
			BacklogSubject:        "sessions.hash.requests",
			ResponseSubjectPrefix: "responses.",
			AssignmentsBucket:     "assignments",
			DllamaSubjectPrefix:   "sessions.hash.dllama.",
			StateStream:           "STATE",
		},
		NATS: conversation.NATSConfig{
			URL:               "nats://demo:4222",
			CredentialsSecret: secretName,
		},
	}

	var applied *v1.Session
	mockApply.EXPECT().
		ApplyObjects(gomock.AssignableToTypeOf(&v1.Session{})).
		DoAndReturn(func(objs ...runtime.Object) error {
			require.Len(t, objs, 1)
			var ok bool
			applied, ok = objs[0].(*v1.Session)
			require.True(t, ok)
			return nil
		})

	require.NoError(t, reconciler.ensureSession(record))
	require.NotNil(t, applied)
	require.Equal(t, record.Namespace, applied.Namespace)
	require.Equal(t, record.SessionName(), applied.Name)
	require.Equal(t, record.Hash, applied.Spec.Hash)
	require.Equal(t, "Model", applied.Spec.ModelRef.Kind)
	require.Equal(t, v1.GroupName, applied.Spec.ModelRef.APIGroup)
	require.Equal(t, "instruct", applied.Spec.ModelRef.Name)
	require.Equal(t, "models-ns", applied.Spec.ModelRef.Namespace)
	require.Equal(t, record.RootImage, applied.Spec.RootImage)
	require.Equal(t, record.WorkerImage, applied.Spec.WorkerImage)
	require.Equal(t, record.DispatcherImage, applied.Spec.DispatcherImage)
	require.Equal(t, record.DispatcherMetricsListen, applied.Spec.DispatcherMetricsListen)
	require.Equal(t, int32(1), applied.Spec.ReplicaPower, "replica power defaults to 1")
	require.Equal(t, int32(2), applied.Spec.MinIdle)
	require.Equal(t, int32(5), applied.Spec.MaxWorkers)
	require.NotNil(t, applied.Spec.Scaling)
	require.Equal(t, record.Scaling.DesiredDllamas, applied.Spec.Scaling.DesiredDllamas)
	require.Equal(t, record.Scaling.ScaleUpBacklog, applied.Spec.Scaling.ScaleUpBacklog)
	require.Equal(t, record.Scaling.ScaleDownIdleSeconds, applied.Spec.Scaling.ScaleDownIdleSeconds)
	require.NotNil(t, applied.Spec.Queue)
	require.Equal(t, record.Queue.BacklogSubject, applied.Spec.Queue.BacklogSubject)
	require.Equal(t, record.Queue.AssignmentsBucket, applied.Spec.Queue.AssignmentsBucket)
	require.Equal(t, record.Queue.StateStream, applied.Spec.Queue.StateStream)
	require.Equal(t, record.Queue.ResponseSubjectPrefix, applied.Spec.Queue.ResponseSubjectPrefix)
	require.Equal(t, record.Queue.DllamaSubjectPrefix, applied.Spec.Queue.DllamaSubjectPrefix)
	require.NotNil(t, applied.Spec.NATS)
	require.Equal(t, record.NATS.URL, applied.Spec.NATS.URL)
	require.NotNil(t, applied.Spec.NATS.CredentialsSecret)
	require.Equal(t, secretName, applied.Spec.NATS.CredentialsSecret.Name)
	value := applied.Labels[labelConversationHash]
	require.Equal(t, truncateName(record.Hash, validation.LabelValueMaxLength), value)
}

func TestEnsureSessionDefaultsOptionalFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	applyMock := newGomockApply(ctrl)
	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, apierrors.NewNotFound(schema.GroupResource{}, "")).AnyTimes()
	reconciler := &conversationReconciler{apply: applyMock, sessions: sessions}

	record := &conversation.Record{
		Hash:        "hash",
		Session:     "sess",
		Namespace:   "tenant",
		Model:       "tenant/model",
		RootImage:   "root:latest",
		WorkerImage: "worker:latest",
	}

	var applied *v1.Session
	applyMock.EXPECT().ApplyObjects(gomock.AssignableToTypeOf(&v1.Session{})).DoAndReturn(func(objs ...runtime.Object) error {
		require.Len(t, objs, 1)
		var ok bool
		applied, ok = objs[0].(*v1.Session)
		require.True(t, ok)
		return nil
	})

	require.NoError(t, reconciler.ensureSession(record))
	require.NotNil(t, applied)
	require.Equal(t, int32(1), applied.Spec.MinIdle, "MinIdle should default to 1")
	require.Equal(t, int32(0), applied.Spec.MaxWorkers, "MaxWorkers should default to 0")
	require.Nil(t, applied.Spec.Scaling, "Scaling should be nil when record lacks scaling")
	require.Nil(t, applied.Spec.Queue, "Queue should be nil when record lacks queue")
	require.Nil(t, applied.Spec.NATS, "NATS should be nil when URL empty")
}

func TestStartConversationReconcilerSkipsWithoutNATSURL(t *testing.T) {

	require.NoError(t, StartConversationReconciler(context.Background(), nil, ConversationConfig{}))
}

func TestStartConversationReconcilerRequiresBucket(t *testing.T) {

	err := StartConversationReconciler(context.Background(), nil, ConversationConfig{
		NATSURL: "nats://example:4222",
	})
	require.EqualError(t, err, "conversation reconciler requires operator-kv-bucket")
}

func TestStartConversationReconcilerDialError(t *testing.T) {

	manager := newConversationManagerStub()

	err := StartConversationReconciler(context.Background(), manager, ConversationConfig{
		NATSURL:  "nats://example:4222",
		KVBucket: "conversations",
		dialer: func(string, ...nats.Option) (natsConnection, error) {
			return nil, fmt.Errorf("dial boom")
		},
	})
	require.EqualError(t, err, "connect NATS: dial boom")
}

func TestStartConversationReconcilerJetStreamErrorClosesConnection(t *testing.T) {

	conn := &failingConn{jetStreamErr: fmt.Errorf("js boom")}
	cfg := ConversationConfig{
		NATSURL:  "nats://example:4222",
		KVBucket: "bucket",
		dialer: func(string, ...nats.Option) (natsConnection, error) {
			return conn, nil
		},
	}

	manager := newConversationManagerStub()

	err := StartConversationReconciler(context.Background(), manager, cfg)
	require.EqualError(t, err, "jetstream context: js boom")
	require.True(t, conn.closed.Load(), "connection should be closed on JetStream error")
}

func TestStartConversationReconcilerFailsWhenJetStreamDisabled(t *testing.T) {
	manager := newConversationManagerStub()
	conn := &failingConn{jetStreamErr: nats.ErrJetStreamNotEnabled}

	cfg := ConversationConfig{
		NATSURL:  "nats://example:4222",
		KVBucket: "conversations",
		dialer: func(string, ...nats.Option) (natsConnection, error) {
			return conn, nil
		},
	}

	err := StartConversationReconciler(context.Background(), manager, cfg)
	require.ErrorContains(t, err, nats.ErrJetStreamNotEnabled.Error())
	require.True(t, conn.closed.Load(), "connection should be closed when JetStream is unavailable")
}

func TestStartConversationReconcilerStartsLoopAndDrainsConnection(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	fakeApply := newFakeApply()
	manager := &Manager{
		apply: fakeApply,
		Kold: &fakeKoldInterface{
			session: sessions,
		},
	}

	srv := runJetStreamServer(t)
	var tracked atomic.Pointer[trackingConn]

	cfg := ConversationConfig{
		NATSURL:  srv.ClientURL(),
		KVBucket: "conversations",
		dialer: func(url string, opts ...nats.Option) (natsConnection, error) {
			nc, err := nats.Connect(url, opts...)
			if err != nil {
				return nil, err
			}
			conn := newTrackingConn(nc)
			tracked.Store(conn)
			return conn, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))

	require.Eventually(t, func() bool {
		return tracked.Load() != nil
	}, time.Second, 10*time.Millisecond)

	cancel()

	require.Eventually(t, func() bool {
		conn := tracked.Load()
		return conn != nil && conn.Drained()
	}, time.Second, 10*time.Millisecond)
}

func TestStartConversationReconcilerReconnectsAfterConnectionClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	manager := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			session: sessions,
		},
	}

	srv := runJetStreamServer(t)
	var dialCount atomic.Int32
	var tracked atomic.Pointer[trackingConn]

	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     "conversation-reconnect",
		PollInterval: 5 * time.Millisecond,
		dialer: func(url string, opts ...nats.Option) (natsConnection, error) {
			dialCount.Add(1)
			nc, err := nats.Connect(url, opts...)
			if err != nil {
				return nil, err
			}
			conn := newTrackingConn(nc)
			tracked.Store(conn)
			return conn, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))

	require.Eventually(t, func() bool {
		return dialCount.Load() == 1 && tracked.Load() != nil
	}, 5*time.Second, 10*time.Millisecond, "initial connection was not established")

	first := tracked.Load()
	require.NotNil(t, first)

	if inner, ok := first.inner.(*nats.Conn); ok {
		inner.Close()
	} else {
		first.Close()
	}

	require.Eventually(t, func() bool {
		return dialCount.Load() >= 2 && tracked.Load() != nil && tracked.Load() != first
	}, 5*time.Second, 20*time.Millisecond, "conversation reconciler did not reconnect after connection closed")

	cancel()
	require.Eventually(t, func() bool {
		conn := tracked.Load()
		return conn != nil && conn.Drained()
	}, time.Second, 10*time.Millisecond)
}

func TestStartConversationReconcilerUsesDefaultDialer(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	manager := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			session: sessions,
		},
	}

	srv := runJetStreamServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := ConversationConfig{
		NATSURL:  srv.ClientURL(),
		KVBucket: "default-dialer",
	}

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))
	cancel()
}

func TestStartConversationReconcilerClosesConnectionWhenBucketEnsureFails(t *testing.T) {
	srv := runJetStreamServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	tracking := newTrackingConn(nc)
	failingConn := &failingKVConn{
		trackingConn: tracking,
		keyValueErr:  errors.New("kv boom"),
	}

	cfg := ConversationConfig{
		NATSURL:  srv.ClientURL(),
		KVBucket: "conversations",
		dialer: func(string, ...nats.Option) (natsConnection, error) {
			return failingConn, nil
		},
	}

	manager := newConversationManagerStub()

	err = StartConversationReconciler(context.Background(), manager, cfg)
	require.EqualError(t, err, "kv bucket conversations: kv boom")
	require.True(t, failingConn.Drained(), "connection should close when bucket ensure fails")
}

func newConversationManagerStub() *Manager {
	return &Manager{
		apply: newFakeApply(),
		Kold:  &fakeKoldInterface{},
	}
}

func runJetStreamServer(t *testing.T) *server.Server {
	return runJetStreamServerOnPort(t, -1)
}

func runJetStreamServerOnPort(t *testing.T, port int) *server.Server {
	t.Helper()
	testutil.RequireLoopback(t)

	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      port,
	}

	serverInstance, err := server.NewServer(opts)
	require.NoError(t, err)

	go serverInstance.Start()
	if !serverInstance.ReadyForConnections(5 * time.Second) {
		serverInstance.Shutdown()
		t.Fatal("nats server not ready")
	}

	t.Cleanup(func() {
		serverInstance.Shutdown()
	})

	return serverInstance
}

func TestEnsureConversationBucketRequiresName(t *testing.T) {

	kv, err := ensureConversationBucket(nil, " ")
	require.Nil(t, kv)
	require.EqualError(t, err, "bucket name cannot be empty")
}

func TestEnsureConversationBucketReturnsExistingBucket(t *testing.T) {

	srv := runJetStreamServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, nc.Drain())
	})

	js, err := nc.JetStream()
	require.NoError(t, err)

	const bucket = "existing"
	created, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	require.NoError(t, err)
	require.NotNil(t, created)

	found, err := ensureConversationBucket(js, bucket)
	require.NoError(t, err)
	require.Equal(t, created.Bucket(), found.Bucket())
}

func TestEnsureConversationBucketCreatesBucketWhenMissing(t *testing.T) {

	srv := runJetStreamServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, nc.Drain())
	})

	js, err := nc.JetStream()
	require.NoError(t, err)

	const bucket = "new-conversations"
	kv, err := ensureConversationBucket(js, bucket)
	require.NoError(t, err)
	require.Equal(t, bucket, kv.Bucket())

	status, err := kv.Status()
	require.NoError(t, err)
	require.Equal(t, bucket, status.Bucket())
}

func TestEnsureConversationBucketPropagatesUnexpectedErrors(t *testing.T) {
	provider := &stubKeyValueProvider{keyValueErr: errors.New("kv unavailable")}
	kv, err := ensureConversationBucket(provider, "broken")
	require.Nil(t, kv)
	require.EqualError(t, err, "kv unavailable")
	require.False(t, provider.createCalled.Load(), "create should not be invoked on non-notfound errors")
}

type trackingConn struct {
	inner    natsConnection
	drained  atomic.Bool
	closed   atomic.Bool
	drainErr error
}

func newTrackingConn(inner natsConnection) *trackingConn {
	return &trackingConn{inner: inner}
}

func (t *trackingConn) JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	return t.inner.JetStream(opts...)
}

func (t *trackingConn) Close() {
	if t.inner != nil {
		toClose := t.inner
		toClose.Close()
	}
	t.drained.Store(true)
	t.closed.Store(true)
}

func (t *trackingConn) Drain() error {
	var err error
	if t.drainErr != nil {
		err = t.drainErr
	} else if t.inner != nil {
		err = t.inner.Drain()
	}
	if err == nil {
		t.drained.Store(true)
	}
	return err
}

func (t *trackingConn) Drained() bool {
	return t.drained.Load()
}

func TestConversationReconcilerSyncCreatesAndDeletesSessions(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()

	stale := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale",
			Namespace: "tenant-a",
			Labels: map[string]string{
				labelConversationHash: "other-hash",
			},
		},
		Spec: v1.SessionSpec{
			Hash: "other-hash",
		},
	}
	existing := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active",
			Namespace: "tenant-a",
			Labels: map[string]string{
				labelConversationHash: "hash-123",
			},
		},
		Spec: v1.SessionSpec{
			Hash: "hash-123",
		},
	}

	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return([]*v1.Session{stale, existing}, nil)

	// ensureSession will check if Session exists before applying
	cache.EXPECT().
		Get("tenant-a", "active").
		Return(existing, nil)

	sessions.EXPECT().
		Delete("tenant-a", "stale", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).
		Return(nil)

	kv := &fakeMemoryKV{bucket: "conversations"}
	record := conversation.Record{
		Hash:        "hash-123",
		Session:     "active",
		Namespace:   "tenant-a",
		Model:       "model-a",
		RootImage:   "ghcr.io/root:v1",
		WorkerImage: "ghcr.io/worker:v1",
	}
	payload, err := record.Marshal()
	require.NoError(t, err)
	_, err = kv.Put("nats_ttl_hash-123", payload)
	require.NoError(t, err)

	fakeApply := newFakeApply()
	logger := logrus.New().WithField("test", t.Name())

	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			TTLPrefix: "nats_ttl_",
		},
		log:      logger,
		sessions: sessions,
		apply:    fakeApply,
		kv:       kv,
	}

	require.NoError(t, reconciler.sync())

	require.Len(t, fakeApply.appliedObjects, 1)
	session, ok := fakeApply.appliedObjects[0].(*v1.Session)
	require.True(t, ok)
	require.Equal(t, "tenant-a", session.Namespace)
	require.Equal(t, "active", session.Name)
	require.Equal(t, "hash-123", session.Spec.Hash)
}

func TestConversationReconcilerSyncHandlesKeyListingError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	sessions.EXPECT().Cache().Times(0)

	reconciler := &conversationReconciler{
		cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		kv: &fakeMemoryKV{
			bucket:  "conversations",
			keysErr: errors.New("keys failure"),
		},
	}

	require.NoError(t, reconciler.sync())
}

func TestConversationReconcilerSyncHandlesSessionListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache)
	cache.EXPECT().List("", gomock.AssignableToTypeOf(labels.Everything())).Return(nil, errors.New("list failure"))

	reconciler := &conversationReconciler{
		cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		kv:       &fakeMemoryKV{bucket: "conversations"},
	}

	require.NoError(t, reconciler.sync())
}

func TestConversationReconcilerSyncDeletesStaleSessionsWhenNoKVRecords(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache)

	stale := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale",
			Namespace: "tenant-a",
		},
		Spec: v1.SessionSpec{Hash: "hash-stale"},
	}
	noHash := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "noop",
			Namespace: "tenant-a",
		},
	}

	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return([]*v1.Session{stale, noHash}, nil)

	sessions.EXPECT().
		Delete("tenant-a", "stale", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).
		Return(nil)

	fakeApply := newFakeApply()
	reconciler := &conversationReconciler{
		cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    fakeApply,
		kv:       &fakeMemoryKV{bucket: "conversations"},
	}

	require.NoError(t, reconciler.sync())

	require.Empty(t, fakeApply.appliedObjects)
}

func TestConversationReconcilerSyncSkipsInvalidRecordsAndHandlesDeleteErrors(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()

	matching := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "envtest-session",
			Namespace: "tenant-b",
			Labels: map[string]string{
				labelConversationHash: "hash-valid",
			},
		},
	}
	stale := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale-session",
			Namespace: "tenant-b",
		},
		Spec: v1.SessionSpec{Hash: "hash-stale"},
	}
	noHash := &v1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "no-hash",
			Namespace: "tenant-b",
		},
	}

	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return([]*v1.Session{matching, stale, noHash}, nil)

	// ensureSession will check if Session exists before applying
	cache.EXPECT().
		Get("tenant-b", "envtest-session").
		Return(matching, nil)

	sessions.EXPECT().
		Delete("tenant-b", "stale-session", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).
		Return(errors.New("delete failed"))

	kv := &fakeMemoryKV{
		bucket: "conversations",
		getErrors: map[string]error{
			"nats_ttl_error":   errors.New("kv boom"),
			"nats_ttl_missing": nats.ErrKeyNotFound,
		},
	}
	// Non-prefixed key should be ignored.
	_, err := kv.Put("noise", []byte("{}"))
	require.NoError(t, err)

	_, err = kv.Put("nats_ttl_error", []byte("{}"))
	require.NoError(t, err)
	_, err = kv.Put("nats_ttl_parsefail", []byte("not-json"))
	require.NoError(t, err)

	missing := &conversation.Record{
		Hash:        "missing",
		Session:     "missing-session",
		Namespace:   "tenant-b",
		Model:       "tenant-b/missing-model",
		RootImage:   "root:v1",
		WorkerImage: "worker:v1",
	}
	missingPayload, err := missing.Marshal()
	require.NoError(t, err)
	_, err = kv.Put("nats_ttl_missing", missingPayload)
	require.NoError(t, err)

	valid := &conversation.Record{
		Hash:        "hash-valid",
		Session:     "envtest-session",
		Namespace:   "tenant-b",
		Model:       "tenant-b/model-a",
		RootImage:   "root:stable",
		WorkerImage: "worker:stable",
	}
	validPayload, err := valid.Marshal()
	require.NoError(t, err)
	_, err = kv.Put("nats_ttl_hash-valid", validPayload)
	require.NoError(t, err)

	innerApply := newFakeApply()
	applyErr := errors.New("apply failed")
	reconciler := &conversationReconciler{
		cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    &failingApply{fakeApply: innerApply, err: applyErr},
		kv:       kv,
	}

	require.NoError(t, reconciler.sync())

	require.Len(t, innerApply.appliedObjects, 1)
	created, ok := innerApply.appliedObjects[0].(*v1.Session)
	require.True(t, ok)
	require.Equal(t, valid.Namespace, created.Namespace)
	require.Equal(t, valid.SessionName(), created.Name)
	require.Equal(t, valid.Hash, created.Spec.Hash)
}

func TestConversationReconcilerRunDrainsConnectionOnContextCancel(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	fakeApply := newFakeApply()
	logger := logrus.New().WithField("test", t.Name())

	kv := newSpyingKeyValue("conversations")
	conn := newTrackingConn(nil)

	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			PollInterval: 2 * time.Millisecond,
		},
		log:      logger,
		sessions: sessions,
		apply:    fakeApply,
		conn:     conn,
		kv:       kv,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		reconciler.run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return kv.keysCalls.Load() > 0
	}, time.Second, 5*time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not exit after context cancellation")
	}

	require.True(t, conn.Drained())
}

func TestConversationReconcilerReconnectsWhenBucketMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil)

	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			TTLPrefix:    "nats_ttl_",
			PollInterval: 25 * time.Millisecond,
		},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		kv: &fakeMemoryKV{
			bucket:  "conversations",
			keysErr: nats.ErrBucketNotFound,
		},
	}

	var reconnects atomic.Int32
	reconciler.reconnectFn = func(context.Context) error {
		reconnects.Add(1)
		reconciler.kv = &fakeMemoryKV{bucket: "conversations"}
		return nil
	}

	require.True(t, reconciler.syncWithReconnect(context.Background()))
	require.Equal(t, int32(1), reconnects.Load(), "reconciler should reconnect after bucket deletion")
}

func TestConversationReconcilerSyncWithReconnectHandlesRecoverableErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil).AnyTimes()

	tests := []struct {
		name string
		err  error
	}{
		{name: "connection closed", err: nats.ErrConnectionClosed},
		{name: "connection draining", err: nats.ErrConnectionDraining},
		{name: "bucket missing", err: nats.ErrBucketNotFound},
		{name: "stream missing", err: nats.ErrStreamNotFound},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &conversationReconciler{
				cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
				log:      logrus.New().WithField("test", t.Name()),
				sessions: sessions,
				apply:    newFakeApply(),
				kv: &fakeMemoryKV{
					bucket:  "conversations",
					keysErr: tt.err,
				},
			}

			var reconnects atomic.Int32
			reconciler.reconnectFn = func(context.Context) error {
				reconnects.Add(1)
				reconciler.kv = &fakeMemoryKV{bucket: "conversations"}
				return nil
			}

			require.True(t, reconciler.syncWithReconnect(context.Background()))
			require.Equal(t, int32(1), reconnects.Load(), "expected reconnect for %s", tt.name)
		})
	}
}

func TestConversationReconcilerSyncWithReconnectFailsWhenReconnectErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil).AnyTimes()

	reconnectErr := errors.New("reconnect failed")
	reconciler := &conversationReconciler{
		cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		kv: &fakeMemoryKV{
			bucket:  "conversations",
			keysErr: nats.ErrConnectionClosed,
		},
	}

	var reconnects atomic.Int32
	reconciler.reconnectFn = func(context.Context) error {
		reconnects.Add(1)
		return reconnectErr
	}

	require.False(t, reconciler.syncWithReconnect(context.Background()))
	require.Equal(t, int32(1), reconnects.Load(), "reconnect should be attempted before giving up")
}

func TestConversationReconcilerSyncWithReconnectStopsOnContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil).AnyTimes()

	reconciler := &conversationReconciler{
		cfg:      ConversationConfig{TTLPrefix: "nats_ttl_"},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		kv: &fakeMemoryKV{
			bucket:  "conversations",
			keysErr: nats.ErrBucketNotFound,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	var reconnects atomic.Int32
	reconciler.reconnectFn = func(ctx context.Context) error {
		reconnects.Add(1)
		cancel()
		return ctx.Err()
	}

	require.False(t, reconciler.syncWithReconnect(ctx))
	require.Equal(t, int32(1), reconnects.Load(), "context cancellation should still trigger a single reconnect attempt")
}

func TestConversationReconcilerReconnectPreservesPollInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil)

	cfg := ConversationConfig{
		TTLPrefix:    "nats_ttl_",
		PollInterval: 125 * time.Millisecond,
	}
	reconciler := &conversationReconciler{
		cfg:      cfg,
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		kv: &fakeMemoryKV{
			bucket:  "conversations",
			keysErr: nats.ErrBucketNotFound,
		},
	}

	reconciler.reconnectFn = func(context.Context) error {
		reconciler.kv = &fakeMemoryKV{bucket: "conversations"}
		return nil
	}

	require.True(t, reconciler.syncWithReconnect(context.Background()))
	require.Equal(t, cfg.PollInterval, reconciler.cfg.PollInterval, "poll interval should remain unchanged after reconnect")
}

func TestConversationReconcilerRunDrainsConnectionWhenSyncFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil).AnyTimes()

	conn := newTrackingConn(nil)
	reconciler := &conversationReconciler{
		cfg: ConversationConfig{
			PollInterval: 5 * time.Millisecond,
		},
		log:      logrus.New().WithField("test", t.Name()),
		sessions: sessions,
		apply:    newFakeApply(),
		conn:     conn,
		kv: &fakeMemoryKV{
			bucket:  "conversations",
			keysErr: nats.ErrConnectionClosed,
		},
	}

	reconcileErr := errors.New("persistent reconnect failure")
	reconciler.reconnectFn = func(context.Context) error {
		return reconcileErr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		reconciler.run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconciler.run did not exit after sync failure")
	}

	require.True(t, conn.Drained(), "connection should be drained when run exits due to sync failure")
}

func TestConversationReconcilerRecreatesBucketAfterDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		List("", gomock.AssignableToTypeOf(labels.Everything())).
		Return(nil, nil).AnyTimes()

	manager := &Manager{
		apply: newFakeApply(),
		Kold: &fakeKoldInterface{
			session: sessions,
		},
	}

	srv := runJetStreamServer(t)
	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     "conversations-recreate",
		TTLPrefix:    "nats_ttl_",
		PollInterval: 15 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, nc.Drain())
	})

	js, err := nc.JetStream()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 5*time.Second, 20*time.Millisecond, "conversation reconciler did not create bucket")

	require.NoError(t, js.DeleteKeyValue(cfg.KVBucket))

	require.Eventually(t, func() bool {
		kv, err := js.KeyValue(cfg.KVBucket)
		if err != nil {
			return false
		}
		_, statusErr := kv.Status()
		return statusErr == nil
	}, 5*time.Second, 50*time.Millisecond, "conversation reconciler did not recreate bucket after deletion")
}

func TestConversationReconcilerReconnectReturnsEarlyOnCancelledContext(t *testing.T) {
	reconciler := &conversationReconciler{
		log:    logrus.New().WithField("test", t.Name()),
		dialer: func(string, ...nats.Option) (natsConnection, error) { return nil, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := reconciler.reconnect(ctx)
	require.ErrorIs(t, err, context.Canceled, "reconnect should return context error immediately when cancelled")
}

func TestConversationReconcilerReconnectStopsDuringBackoff(t *testing.T) {
	var attempts atomic.Int32
	reconciler := &conversationReconciler{
		log: logrus.New().WithField("test", t.Name()),
		dialer: func(string, ...nats.Option) (natsConnection, error) {
			attempts.Add(1)
			return nil, errors.New("connection refused")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := reconciler.reconnect(ctx)
	require.ErrorIs(t, err, context.Canceled, "reconnect should stop during backoff when context cancelled")
	require.GreaterOrEqual(t, attempts.Load(), int32(1), "at least one attempt should have been made")
}

func TestConversationReconcilerReconnectCapsBackoffAtMax(t *testing.T) {
	var attempts atomic.Int32
	reconciler := &conversationReconciler{
		log: logrus.New().WithField("test", t.Name()),
		cfg: ConversationConfig{KVBucket: "test-backoff"},
		dialer: func(string, ...nats.Option) (natsConnection, error) {
			count := attempts.Add(1)
			if count >= 6 {
				srv := runJetStreamServer(t)
				conn, err := nats.Connect(srv.ClientURL())
				if err != nil {
					return nil, err
				}
				return newTrackingConn(conn), nil
			}
			return nil, errors.New("connection refused")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err := reconciler.reconnect(ctx)
	require.NoError(t, err, "reconnect should succeed after multiple retries")
	require.GreaterOrEqual(t, attempts.Load(), int32(6), "should have made multiple attempts with exponential backoff")
}

func TestConversationReconcilerDrainConnHandlesNilConnection(t *testing.T) {
	reconciler := &conversationReconciler{
		log:  logrus.New().WithField("test", t.Name()),
		conn: nil,
	}
	require.NotPanics(t, func() {
		reconciler.drainConn()
	}, "drainConn should not panic on nil connection")
}

func TestConversationReconcilerDrainConnClosesOnDrainError(t *testing.T) {
	conn := newTrackingConn(nil)
	conn.drainErr = errors.New("drain failed")
	reconciler := &conversationReconciler{
		log:  logrus.New().WithField("test", t.Name()),
		conn: conn,
	}

	reconciler.drainConn()
	require.True(t, conn.closed.Load(), "drainConn should close connection when drain fails")
}

func TestIsConnectionClosedReturnsTrueForDrainingError(t *testing.T) {
	require.True(t, isConnectionClosed(nats.ErrConnectionDraining), "should recognize draining error")
}

func TestIsConnectionClosedReturnsFalseForNilError(t *testing.T) {
	require.False(t, isConnectionClosed(nil), "should return false for nil error")
}

func TestIsBucketMissingReturnsTrueForStreamNotFoundError(t *testing.T) {
	require.True(t, isBucketMissing(nats.ErrStreamNotFound), "should recognize stream not found error")
}

func TestIsBucketMissingReturnsFalseForNilError(t *testing.T) {
	require.False(t, isBucketMissing(nil), "should return false for nil error")
}

type failingConn struct {
	jetStreamErr error
	closed       atomic.Bool
}

func (f *failingConn) JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	return nil, f.jetStreamErr
}

func (f *failingConn) Close() {
	f.closed.Store(true)
}

func (f *failingConn) Drain() error {
	f.closed.Store(true)
	return nil
}

type spyingKeyValue struct {
	*fakeMemoryKV
	keysCalls atomic.Int32
}

func newSpyingKeyValue(bucket string) *spyingKeyValue {
	return &spyingKeyValue{
		fakeMemoryKV: &fakeMemoryKV{bucket: bucket},
	}
}

func (s *spyingKeyValue) Keys(opts ...nats.WatchOpt) ([]string, error) {
	s.keysCalls.Add(1)
	return s.fakeMemoryKV.Keys(opts...)
}

type failingKVConn struct {
	*trackingConn
	keyValueErr error
}

func (f *failingKVConn) JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	js, err := f.trackingConn.JetStream(opts...)
	if err != nil {
		return nil, err
	}
	return failingKVJetStream{JetStreamContext: js, keyValueErr: f.keyValueErr}, nil
}

type failingKVJetStream struct {
	nats.JetStreamContext
	keyValueErr error
}

func (f failingKVJetStream) KeyValue(string) (nats.KeyValue, error) {
	return nil, f.keyValueErr
}

type stubKeyValueProvider struct {
	keyValueErr  error
	createCalled atomic.Bool
}

func (s *stubKeyValueProvider) KeyValue(string) (nats.KeyValue, error) {
	return nil, s.keyValueErr
}

func (s *stubKeyValueProvider) CreateKeyValue(*nats.KeyValueConfig) (nats.KeyValue, error) {
	s.createCalled.Store(true)
	return nil, errors.New("unexpected create call")
}
