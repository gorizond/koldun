package controllers

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func TestConversationReconcilerCreatesSessionFromKV(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest integration in short mode")
	}
	if reason := envtestSkipReason; reason != "" {
		t.Skip(reason)
	}
	if testEnvConfig == nil {
		t.Skip("envtest assets unavailable")
	}

	srv := runJetStreamServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager, err := NewManager(testEnvConfig)
	require.NoError(t, err)

	errChan := make(chan error, 1)
	go func() {
		errChan <- manager.Start(ctx)
	}()
	t.Cleanup(func() {
		select {
		case err := <-errChan:
			if err != nil && ctx.Err() == nil {
				t.Fatalf("manager.Start failed: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Log("manager shutdown exceeded grace period")
		}
	})

	require.Eventually(t, func() bool {
		return manager.Health().Ready()
	}, 15*time.Second, 200*time.Millisecond, "manager never reported ready")

	kube, err := kubernetes.NewForConfig(testEnvConfig)
	require.NoError(t, err)

	namespace := fmt.Sprintf("conversation-envtest-%d", time.Now().UnixNano())
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     fmt.Sprintf("conversation-envtest-%d", time.Now().UnixNano()),
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

	err = StartConversationReconciler(ctx, manager, cfg)
	require.NoError(t, err)
	require.NotNil(t, tracked, "conversation reconciler should establish a NATS connection")

	writer, err := nats.Connect(srv.ClientURL(), nats.Name("conversation-envtest-writer"))
	require.NoError(t, err)
	t.Cleanup(writer.Close)

	js, err := writer.JetStream()
	require.NoError(t, err)

	var kv nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kv, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "kv bucket %s was not created", cfg.KVBucket)

	record := &conversation.Record{
		Hash:            fmt.Sprintf("envtest-%d", time.Now().UnixNano()),
		Session:         "envtest-session",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/envtest/root:latest",
		WorkerImage:     "ghcr.io/envtest/worker:latest",
		DispatcherImage: "ghcr.io/envtest/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	payload, err := record.Marshal()
	require.NoError(t, err)

	key := fmt.Sprintf("nats_ttl_%s", record.Hash)
	_, err = kv.Put(key, payload)
	require.NoError(t, err)

	sessionClient := manager.Kold.Session()
	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == record.Hash &&
			session.Spec.DispatcherImage == record.DispatcherImage &&
			session.Spec.NATS != nil &&
			session.Spec.NATS.URL == record.NATS.URL
	}, 20*time.Second, 200*time.Millisecond, "session was not created from conversation record")

	require.NoError(t, kv.Delete(key))
	require.Eventually(t, func() bool {
		_, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 20*time.Second, 200*time.Millisecond, "stale session was not deleted after KV entry removal")

	writer.Close()

	addr, ok := srv.Addr().(*net.TCPAddr)
	require.True(t, ok, "nats server address must be TCP")
	port := addr.Port

	srv.Shutdown()
	srv = runJetStreamServerOnPort(t, port)

	writerRestart, err := nats.Connect(srv.ClientURL(), nats.Name("conversation-envtest-writer-restart"))
	require.NoError(t, err)
	t.Cleanup(writerRestart.Close)

	jsRestart, err := writerRestart.JetStream()
	require.NoError(t, err)

	var kvRestart nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kvRestart, err = jsRestart.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after restart", cfg.KVBucket)

	recordRestart := &conversation.Record{
		Hash:            fmt.Sprintf("envtest-restart-%d", time.Now().UnixNano()),
		Session:         "envtest-session-restart",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/envtest/root:latest",
		WorkerImage:     "ghcr.io/envtest/worker:latest",
		DispatcherImage: "ghcr.io/envtest/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	payloadRestart, err := recordRestart.Marshal()
	require.NoError(t, err)

	restartKey := fmt.Sprintf("nats_ttl_%s", recordRestart.Hash)
	_, err = kvRestart.Put(restartKey, payloadRestart)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, recordRestart.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == recordRestart.Hash
	}, 20*time.Second, 200*time.Millisecond, "session was not recreated after JetStream restart")

	require.NoError(t, kvRestart.Delete(restartKey))
	require.Eventually(t, func() bool {
		_, err := sessionClient.Get(namespace, recordRestart.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 20*time.Second, 200*time.Millisecond, "stale session was not deleted after restart KV entry removal")

	cancel()
	require.Eventually(t, func() bool {
		return tracked.Drained()
	}, 5*time.Second, 50*time.Millisecond, "nats connection was not drained after cancellation")
}
