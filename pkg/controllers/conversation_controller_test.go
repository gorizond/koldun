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
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
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
	reconciler := &conversationReconciler{apply: mockApply}

	const secretName = "nats-credentials"
	hash := strings.Repeat("abc", 30)
	record := &conversation.Record{
		Hash:            hash,
		Session:         "conversation-session",
		Namespace:       "tenant-a",
		Model:           "models-ns/instruct",
		ReplicaPower:    0,
		RootImage:       "ghcr.io/root:stable",
		WorkerImage:     "ghcr.io/worker:stable",
		DispatcherImage: "ghcr.io/dispatcher:stable",
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

	err := StartConversationReconciler(context.Background(), &Manager{}, ConversationConfig{
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

	err := StartConversationReconciler(context.Background(), &Manager{}, cfg)
	require.EqualError(t, err, "jetstream context: js boom")
	require.True(t, conn.closed.Load(), "connection should be closed on JetStream error")
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

func runJetStreamServer(t *testing.T) *server.Server {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
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

type trackingConn struct {
	inner   natsConnection
	drained atomic.Bool
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
}

func (t *trackingConn) Drain() error {
	var err error
	if t.inner != nil {
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

	reconciler.sync(context.Background())

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

	reconciler.sync(context.Background())
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

	reconciler.sync(context.Background())

	require.Empty(t, fakeApply.appliedObjects)
}

func TestConversationReconcilerSyncSkipsInvalidRecordsAndHandlesDeleteErrors(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	sessions := genericfake.NewMockControllerInterface[*v1.Session, *v1.SessionList](ctrl)
	cache := genericfake.NewMockCacheInterface[*v1.Session](ctrl)
	sessions.EXPECT().Cache().Return(cache)

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

	reconciler.sync(context.Background())

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
