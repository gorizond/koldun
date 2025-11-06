package controllers

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestEnsureSessionValidatesRecord(t *testing.T) {
	t.Parallel()

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
