package controllers

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/conversation"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
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

	require.NoError(t, js.DeleteKeyValue(cfg.KVBucket), "delete kv bucket to simulate manual removal")

	var kvAfterDeletion nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kvAfterDeletion, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after deletion", cfg.KVBucket)

	recordAfterDeletion := &conversation.Record{
		Hash:            fmt.Sprintf("envtest-deleted-bucket-%d", time.Now().UnixNano()),
		Session:         "envtest-session-deleted-bucket",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/envtest/root:latest",
		WorkerImage:     "ghcr.io/envtest/worker:latest",
		DispatcherImage: "ghcr.io/envtest/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	payloadAfterDeletion, err := recordAfterDeletion.Marshal()
	require.NoError(t, err)

	deleteBucketKey := fmt.Sprintf("nats_ttl_%s", recordAfterDeletion.Hash)
	_, err = kvAfterDeletion.Put(deleteBucketKey, payloadAfterDeletion)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, recordAfterDeletion.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == recordAfterDeletion.Hash
	}, 20*time.Second, 200*time.Millisecond, "session was not recreated after bucket deletion")

	require.NoError(t, kvAfterDeletion.Delete(deleteBucketKey))
	require.Eventually(t, func() bool {
		_, err := sessionClient.Get(namespace, recordAfterDeletion.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 20*time.Second, 200*time.Millisecond, "stale session was not deleted after recreated bucket entry removal")

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

func TestConversationReconcilerCleansOrphanedSessionsAfterBucketLoss(t *testing.T) {
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

	namespace := fmt.Sprintf("conversation-orphan-%d", time.Now().UnixNano())
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     fmt.Sprintf("conversation-orphan-%d", time.Now().UnixNano()),
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

	writer, err := nats.Connect(srv.ClientURL(), nats.Name("conversation-orphan-writer"))
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

	record1 := &conversation.Record{
		Hash:            fmt.Sprintf("orphan-1-%d", time.Now().UnixNano()),
		Session:         "orphan-session-1",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/orphan/root:latest",
		WorkerImage:     "ghcr.io/orphan/worker:latest",
		DispatcherImage: "ghcr.io/orphan/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	payload1, err := record1.Marshal()
	require.NoError(t, err)

	key1 := fmt.Sprintf("nats_ttl_%s", record1.Hash)
	_, err = kv.Put(key1, payload1)
	require.NoError(t, err)

	record2 := &conversation.Record{
		Hash:            fmt.Sprintf("orphan-2-%d", time.Now().UnixNano()),
		Session:         "orphan-session-2",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/orphan/root:latest",
		WorkerImage:     "ghcr.io/orphan/worker:latest",
		DispatcherImage: "ghcr.io/orphan/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	payload2, err := record2.Marshal()
	require.NoError(t, err)

	key2 := fmt.Sprintf("nats_ttl_%s", record2.Hash)
	_, err = kv.Put(key2, payload2)
	require.NoError(t, err)

	sessionClient := manager.Kold.Session()
	require.Eventually(t, func() bool {
		session1, err := sessionClient.Get(namespace, record1.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		session2, err := sessionClient.Get(namespace, record2.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session1.Spec.Hash == record1.Hash && session2.Spec.Hash == record2.Hash
	}, 20*time.Second, 200*time.Millisecond, "sessions were not created from conversation records")

	require.NoError(t, js.DeleteKeyValue(cfg.KVBucket), "delete kv bucket to simulate catastrophic bucket loss")

	var kvAfterLoss nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kvAfterLoss, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after catastrophic loss", cfg.KVBucket)

	require.Eventually(t, func() bool {
		_, err1 := sessionClient.Get(namespace, record1.SessionName(), metav1.GetOptions{})
		_, err2 := sessionClient.Get(namespace, record2.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err1) && apierrors.IsNotFound(err2)
	}, 20*time.Second, 200*time.Millisecond, "orphaned sessions were not cleaned up after bucket recreation")

	recordNew := &conversation.Record{
		Hash:            fmt.Sprintf("orphan-new-%d", time.Now().UnixNano()),
		Session:         "orphan-session-new",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/orphan/root:latest",
		WorkerImage:     "ghcr.io/orphan/worker:latest",
		DispatcherImage: "ghcr.io/orphan/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	payloadNew, err := recordNew.Marshal()
	require.NoError(t, err)

	keyNew := fmt.Sprintf("nats_ttl_%s", recordNew.Hash)
	_, err = kvAfterLoss.Put(keyNew, payloadNew)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, recordNew.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == recordNew.Hash
	}, 20*time.Second, 200*time.Millisecond, "new session was not created after bucket recovery")
}

func TestConversationReconcilerRetriesBucketEnsureWhenJetStreamUnavailable(t *testing.T) {
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

	stdLogger := logrus.StandardLogger()
	origHooks := stdLogger.Hooks
	stdLogger.ReplaceHooks(make(logrus.LevelHooks))
	hook := logrustest.NewLocal(stdLogger)
	t.Cleanup(func() {
		stdLogger.ReplaceHooks(origHooks)
		hook.Reset()
	})

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

	namespace := fmt.Sprintf("conversation-outage-%d", time.Now().UnixNano())
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     fmt.Sprintf("conversation-outage-%d", time.Now().UnixNano()),
		PollInterval: 5 * time.Millisecond,
	}

	var tracked *trackingConn
	var blockDialer atomic.Bool
	cfg.dialer = func(url string, opts ...nats.Option) (natsConnection, error) {
		if blockDialer.Load() {
			return nil, fmt.Errorf("jetstream temporarily unavailable")
		}
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

	writer, err := nats.Connect(srv.ClientURL(), nats.Name("conversation-outage-writer"))
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
		Hash:            fmt.Sprintf("outage-%d", time.Now().UnixNano()),
		Session:         "outage-session",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/outage/root:latest",
		WorkerImage:     "ghcr.io/outage/worker:latest",
		DispatcherImage: "ghcr.io/outage/dispatcher:latest",
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
		return session.Spec.Hash == record.Hash
	}, 20*time.Second, 200*time.Millisecond, "session was not created from conversation record")

	require.NoError(t, kv.Delete(key))
	require.Eventually(t, func() bool {
		_, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 20*time.Second, 200*time.Millisecond, "stale session was not deleted after KV entry removal")

	blockDialer.Store(true)
	// Block new connections before deleting the bucket so reconnect attempts hit the failure path.
	t.Cleanup(func() { blockDialer.Store(false) })
	require.NoError(t, js.DeleteKeyValue(cfg.KVBucket), "delete kv bucket to trigger ensure cycle")

	expectLog := func(substr string) {
		require.Eventually(t, func() bool {
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, substr) {
					return true
				}
			}
			return false
		}, 10*time.Second, 50*time.Millisecond, "expected log containing %q", substr)
	}

	expectLog("conversation bucket missing; reconnecting")
	expectLog("failed to reconnect to NATS, will retry")

	blockDialer.Store(false)

	var kvAfterLoss nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kvAfterLoss, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after outage", cfg.KVBucket)

	recordAfterOutage := &conversation.Record{
		Hash:            fmt.Sprintf("outage-recovery-%d", time.Now().UnixNano()),
		Session:         "outage-session-recovery",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/outage/root:latest",
		WorkerImage:     "ghcr.io/outage/worker:latest",
		DispatcherImage: "ghcr.io/outage/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	recoveryPayload, err := recordAfterOutage.Marshal()
	require.NoError(t, err)

	recoveryKey := fmt.Sprintf("nats_ttl_%s", recordAfterOutage.Hash)
	_, err = kvAfterLoss.Put(recoveryKey, recoveryPayload)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, recordAfterOutage.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == recordAfterOutage.Hash
	}, 20*time.Second, 200*time.Millisecond, "session was not recreated after temporary outage")
}

func TestConversationReconcilerRecoversAfterJetStreamRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest integration in short mode")
	}
	if reason := envtestSkipReason; reason != "" {
		t.Skip(reason)
	}
	if testEnvConfig == nil {
		t.Skip("envtest assets unavailable")
	}

	port := getFreePort(t)
	storeDir := t.TempDir()
	srv := startJetStreamServerWithStore(t, port, storeDir)
	clientURL := fmt.Sprintf("nats://127.0.0.1:%d", port)

	stdLogger := logrus.StandardLogger()
	origHooks := stdLogger.Hooks
	stdLogger.ReplaceHooks(make(logrus.LevelHooks))
	hook := logrustest.NewLocal(stdLogger)
	t.Cleanup(func() {
		stdLogger.ReplaceHooks(origHooks)
		hook.Reset()
	})

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

	namespace := fmt.Sprintf("conversation-restart-%d", time.Now().UnixNano())
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := ConversationConfig{
		NATSURL:      clientURL,
		KVBucket:     fmt.Sprintf("conversation-restart-%d", time.Now().UnixNano()),
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

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))
	require.NotNil(t, tracked, "conversation reconciler should establish a NATS connection")

	writer, err := nats.Connect(clientURL, nats.Name("conversation-restart-writer"))
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
		Hash:            fmt.Sprintf("restart-%d", time.Now().UnixNano()),
		Session:         "restart-session",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/restart/root:latest",
		WorkerImage:     "ghcr.io/restart/worker:latest",
		DispatcherImage: "ghcr.io/restart/dispatcher:latest",
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
		return session.Spec.Hash == record.Hash
	}, 20*time.Second, 200*time.Millisecond, "session was not created from conversation record")

	require.NoError(t, kv.Delete(key))
	require.Eventually(t, func() bool {
		_, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 20*time.Second, 200*time.Millisecond, "stale session was not deleted after KV entry removal")

	require.NoError(t, writer.Drain())
	srv.Shutdown()

	require.NoError(t, os.RemoveAll(storeDir))
	require.NoError(t, os.MkdirAll(storeDir, 0o755))

	startJetStreamServerWithStore(t, port, storeDir)

	recoveryWriter, err := nats.Connect(clientURL, nats.Name("conversation-restart-writer-recovery"))
	require.NoError(t, err)
	t.Cleanup(recoveryWriter.Close)

	jsRecovery, err := recoveryWriter.JetStream()
	require.NoError(t, err)

	var kvAfterRestart nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kvAfterRestart, err = jsRecovery.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after restart", cfg.KVBucket)

	recordAfterRestart := &conversation.Record{
		Hash:            fmt.Sprintf("restart-recovery-%d", time.Now().UnixNano()),
		Session:         "restart-session-recovery",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/restart/root:latest",
		WorkerImage:     "ghcr.io/restart/worker:latest",
		DispatcherImage: "ghcr.io/restart/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	recoveryPayload, err := recordAfterRestart.Marshal()
	require.NoError(t, err)

	recoveryKey := fmt.Sprintf("nats_ttl_%s", recordAfterRestart.Hash)
	_, err = kvAfterRestart.Put(recoveryKey, recoveryPayload)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, recordAfterRestart.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == recordAfterRestart.Hash
	}, 20*time.Second, 200*time.Millisecond, "session was not recreated after JetStream restart")
}

func TestConversationReconcilerRestoresBucketAfterRepeatedOfflineDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest integration in short mode")
	}
	if reason := envtestSkipReason; reason != "" {
		t.Skip(reason)
	}
	if testEnvConfig == nil {
		t.Skip("envtest assets unavailable")
	}

	port := getFreePort(t)
	storeDir := t.TempDir()
	srv := startJetStreamServerWithStore(t, port, storeDir)
	clientURL := fmt.Sprintf("nats://127.0.0.1:%d", port)

	stdLogger := logrus.StandardLogger()
	origHooks := stdLogger.Hooks
	stdLogger.ReplaceHooks(make(logrus.LevelHooks))
	hook := logrustest.NewLocal(stdLogger)
	t.Cleanup(func() {
		stdLogger.ReplaceHooks(origHooks)
		hook.Reset()
	})

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

	namespace := fmt.Sprintf("conversation-offline-delete-%d", time.Now().UnixNano())
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := ConversationConfig{
		NATSURL:      clientURL,
		KVBucket:     fmt.Sprintf("conversation-offline-delete-%d", time.Now().UnixNano()),
		PollInterval: 5 * time.Millisecond,
	}

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))

	sessionClient := manager.Kold.Session()

	connectWriter := func(name string) (*nats.Conn, nats.JetStreamContext) {
		conn, err := nats.Connect(clientURL, nats.Name(name))
		require.NoError(t, err)
		js, err := conn.JetStream()
		require.NoError(t, err)
		return conn, js
	}

	writer, js := connectWriter("conversation-offline-delete-writer-initial")
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()

	var kv nats.KeyValue
	require.Eventually(t, func() bool {
		var err error
		kv, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "kv bucket %s was not created", cfg.KVBucket)

	bootstrap := &conversation.Record{
		Hash:            fmt.Sprintf("offline-bootstrap-%d", time.Now().UnixNano()),
		Session:         "offline-bootstrap-session",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/offline/root:latest",
		WorkerImage:     "ghcr.io/offline/worker:latest",
		DispatcherImage: "ghcr.io/offline/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	bootstrapPayload, err := bootstrap.Marshal()
	require.NoError(t, err)

	bootstrapKey := fmt.Sprintf("nats_ttl_%s", bootstrap.Hash)
	_, err = kv.Put(bootstrapKey, bootstrapPayload)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		session, err := sessionClient.Get(namespace, bootstrap.SessionName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		return session.Spec.Hash == bootstrap.Hash
	}, 20*time.Second, 200*time.Millisecond, "bootstrap session was not created from conversation record")

	require.NoError(t, kv.Delete(bootstrapKey))
	require.Eventually(t, func() bool {
		_, err := sessionClient.Get(namespace, bootstrap.SessionName(), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 20*time.Second, 200*time.Millisecond, "bootstrap session was not deleted after KV removal")

	expectRepeatedLog := func(substr string, want int) {
		require.Eventually(t, func() bool {
			count := 0
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, substr) {
					count++
				}
			}
			return count >= want
		}, 15*time.Second, 50*time.Millisecond, "expected at least %d occurrences of %q", want, substr)
	}

	offlineCycles := 2
	for cycle := 0; cycle < offlineCycles; cycle++ {
		require.NoError(t, writer.Drain())
		writer.Close()
		writer = nil

		srv.Shutdown()

		require.NoError(t, os.RemoveAll(storeDir))
		require.NoError(t, os.MkdirAll(storeDir, 0o755))

		srv = startJetStreamServerWithStore(t, port, storeDir)

		writer, js = connectWriter(fmt.Sprintf("conversation-offline-delete-writer-%d", cycle))

		var kvAfterRestart nats.KeyValue
		require.Eventually(t, func() bool {
			var err error
			kvAfterRestart, err = js.KeyValue(cfg.KVBucket)
			return err == nil
		}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after offline deletion #%d", cfg.KVBucket, cycle+1)

		record := &conversation.Record{
			Hash:            fmt.Sprintf("offline-delete-%d-%d", cycle, time.Now().UnixNano()),
			Session:         fmt.Sprintf("offline-delete-session-%d", cycle),
			Namespace:       namespace,
			Model:           fmt.Sprintf("%s/model", namespace),
			RootImage:       "ghcr.io/offline/root:latest",
			WorkerImage:     "ghcr.io/offline/worker:latest",
			DispatcherImage: "ghcr.io/offline/dispatcher:latest",
			NATS: conversation.NATSConfig{
				URL: "nats://demo:4222",
			},
		}
		payload, err := record.Marshal()
		require.NoError(t, err)

		key := fmt.Sprintf("nats_ttl_%s", record.Hash)
		_, err = kvAfterRestart.Put(key, payload)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			session, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
			if err != nil {
				return false
			}
			return session.Spec.Hash == record.Hash
		}, 20*time.Second, 200*time.Millisecond, "session was not recreated after offline deletion #%d", cycle+1)

		require.NoError(t, kvAfterRestart.Delete(key))
		require.Eventually(t, func() bool {
			_, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}, 20*time.Second, 200*time.Millisecond, "stale session was not deleted after offline deletion #%d", cycle+1)
	}

	expectRepeatedLog("conversation bucket missing; reconnecting", offlineCycles)
	expectRepeatedLog("conversation reconciler reconnected to NATS", offlineCycles)
}

func TestConversationReconcilerMaintainsRecoveryTimeAcrossOutages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest integration in short mode")
	}
	if reason := envtestSkipReason; reason != "" {
		t.Skip(reason)
	}
	if testEnvConfig == nil {
		t.Skip("envtest assets unavailable")
	}

	port := getFreePort(t)
	storeDir := t.TempDir()
	srv := startJetStreamServerWithStore(t, port, storeDir)
	clientURL := fmt.Sprintf("nats://127.0.0.1:%d", port)

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

	namespace := fmt.Sprintf("conversation-recovery-%d", time.Now().UnixNano())
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kube.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	cfg := ConversationConfig{
		NATSURL:      clientURL,
		KVBucket:     fmt.Sprintf("conversation-recovery-%d", time.Now().UnixNano()),
		PollInterval: 5 * time.Millisecond,
	}

	var tracked *trackingConn
	var blockDialer atomic.Bool
	cfg.dialer = func(url string, opts ...nats.Option) (natsConnection, error) {
		if blockDialer.Load() {
			return nil, fmt.Errorf("synthetic outage dialer blocked")
		}
		conn, err := nats.Connect(url, opts...)
		if err != nil {
			return nil, err
		}
		tracked = newTrackingConn(conn)
		return tracked, nil
	}

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))
	require.NotNil(t, tracked, "conversation reconciler should establish a NATS connection")

	connectWriter := func(name string) (*nats.Conn, nats.JetStreamContext) {
		conn, err := nats.Connect(clientURL, nats.Name(name))
		require.NoError(t, err)
		js, err := conn.JetStream()
		require.NoError(t, err)
		return conn, js
	}

	writer, js := connectWriter("conversation-recovery-writer")
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()

	require.Eventually(t, func() bool {
		_, err := js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "kv bucket %s was not created", cfg.KVBucket)

	sessionClient := manager.Kold.Session()
	waitSession := func(record *conversation.Record) {
		require.Eventually(t, func() bool {
			session, err := sessionClient.Get(namespace, record.SessionName(), metav1.GetOptions{})
			if err != nil {
				return false
			}
			return session.Spec.Hash == record.Hash
		}, 20*time.Second, 200*time.Millisecond, "session %s was not created", record.Session)
	}
	require.NoError(t, js.DeleteKeyValue(cfg.KVBucket), "delete bucket for synthetic outage measurement")
	blockDialer.Store(true)
	go func() {
		time.Sleep(500 * time.Millisecond)
		blockDialer.Store(false)
	}()

	var kvAfterSynthetic nats.KeyValue
	syntheticStart := time.Now()
	require.Eventually(t, func() bool {
		var err error
		kvAfterSynthetic, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after synthetic outage", cfg.KVBucket)
	syntheticDuration := time.Since(syntheticStart)

	syntheticRecord := &conversation.Record{
		Hash:            fmt.Sprintf("recovery-synthetic-%d", time.Now().UnixNano()),
		Session:         "recovery-synthetic-session",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/recovery/root:latest",
		WorkerImage:     "ghcr.io/recovery/worker:latest",
		DispatcherImage: "ghcr.io/recovery/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	syntheticPayload, err := syntheticRecord.Marshal()
	require.NoError(t, err)

	syntheticKey := fmt.Sprintf("nats_ttl_%s", syntheticRecord.Hash)
	_, err = kvAfterSynthetic.Put(syntheticKey, syntheticPayload)
	require.NoError(t, err)
	waitSession(syntheticRecord)

	require.NoError(t, kvAfterSynthetic.Delete(syntheticKey))

	require.NoError(t, writer.Drain())
	writer.Close()
	writer = nil

	srv.Shutdown()

	require.NoError(t, os.RemoveAll(storeDir))
	require.NoError(t, os.MkdirAll(storeDir, 0o755))

	srv = startJetStreamServerWithStore(t, port, storeDir)

	writer, js = connectWriter("conversation-recovery-writer-restart")

	var kvAfterRestart nats.KeyValue
	restartStart := time.Now()
	require.Eventually(t, func() bool {
		var err error
		kvAfterRestart, err = js.KeyValue(cfg.KVBucket)
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "kv bucket %s was not recreated after restart", cfg.KVBucket)
	restartDuration := time.Since(restartStart)

	restartRecord := &conversation.Record{
		Hash:            fmt.Sprintf("recovery-restart-%d", time.Now().UnixNano()),
		Session:         "recovery-restart-session",
		Namespace:       namespace,
		Model:           fmt.Sprintf("%s/model", namespace),
		RootImage:       "ghcr.io/recovery/root:latest",
		WorkerImage:     "ghcr.io/recovery/worker:latest",
		DispatcherImage: "ghcr.io/recovery/dispatcher:latest",
		NATS: conversation.NATSConfig{
			URL: "nats://demo:4222",
		},
	}
	restartPayload, err := restartRecord.Marshal()
	require.NoError(t, err)

	restartKey := fmt.Sprintf("nats_ttl_%s", restartRecord.Hash)
	_, err = kvAfterRestart.Put(restartKey, restartPayload)
	require.NoError(t, err)
	waitSession(restartRecord)

	require.NoError(t, kvAfterRestart.Delete(restartKey))

	t.Logf("synthetic outage recovery duration: %s", syntheticDuration)
	t.Logf("JetStream restart recovery duration: %s", restartDuration)

	// Session 53 telemetry (synthetic ~1.2s, restart ~2.2s) shows both phases
	// complete well inside the 15s guard, so we keep a single test instead of
	// splitting into subtests until we observe slower platforms.
	require.LessOrEqual(t, syntheticDuration, 15*time.Second, "synthetic outage recovery exceeded 15s")
	require.LessOrEqual(t, restartDuration, 15*time.Second, "restart outage recovery exceeded 15s")

	diff := syntheticDuration - restartDuration
	if diff < 0 {
		diff = -diff
	}
	require.LessOrEqual(t, diff, 5*time.Second, "recovery durations diverged too much")
}

func TestStartConversationReconcilerStopsWhenContextCancelled(t *testing.T) {
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
	t.Cleanup(cancel)

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

	cfg := ConversationConfig{
		NATSURL:      srv.ClientURL(),
		KVBucket:     fmt.Sprintf("conversation-envtest-stop-%d", time.Now().UnixNano()),
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

	require.NoError(t, StartConversationReconciler(ctx, manager, cfg))

	require.Eventually(t, func() bool {
		return tracked != nil
	}, 5*time.Second, 50*time.Millisecond, "reconciler never established a NATS connection")

	cancel()

	require.Eventually(t, func() bool {
		return tracked != nil && tracked.Drained()
	}, 5*time.Second, 50*time.Millisecond, "nats connection was not drained after cancellation")
}

func startJetStreamServerWithStore(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		StoreDir:  storeDir,
		Host:      "127.0.0.1",
		Port:      port,
	}

	srv, err := server.NewServer(opts)
	require.NoError(t, err)

	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats server not ready on port %d", port)
	}

	t.Cleanup(func() {
		srv.Shutdown()
	})

	return srv
}

func getFreePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("expected TCPAddr for listener")
	}

	return addr.Port
}
