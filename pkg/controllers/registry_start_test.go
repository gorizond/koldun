package controllers

import (
	"context"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/tokens"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestStartRegistrySyncPublishesModelAndToken(t *testing.T) {
	ns := startRegistryJetStreamServer(t)

	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	secretCtrl := newControllerStub[*corev1.Secret, *corev1.SecretList](schema.GroupVersionKind{})

	mgr := &Manager{
		Kold: &fakeKoldInterface{model: modelCtrl},
		Core: fakeCoreControllers{secret: secretCtrl},
	}

	cfg := RegistryConfig{
		NATSURL:      ns.ClientURL(),
		ModelsBucket: "models",
		TokensBucket: "tokens",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, StartRegistrySync(ctx, mgr, cfg))

	modelHandler := modelCtrl.lastOnChange()
	require.NotNil(t, modelHandler)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "model"},
		Spec:       v1.ModelSpec{ReplicaPower: 2},
		Status: v1.ModelStatus{
			OutputPVCName:       "pvc",
			ConversionSizeBytes: 64,
			Conditions:          []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
		},
	}

	_, err := modelHandler("demo/model", model)
	require.NoError(t, err)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer func() { _ = nc.Drain(); nc.Close() }()

	js, err := nc.JetStream()
	require.NoError(t, err)

	modelKV, err := js.KeyValue("models")
	require.NoError(t, err)

	entry, err := modelKV.Get("model/demo/model")
	require.NoError(t, err)
	require.NotEmpty(t, entry.Value())

	secretHandler := secretCtrl.lastOnChange()
	require.NotNil(t, secretHandler)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "demo",
			Name:      "token",
			Labels:    map[string]string{tokens.LabelToken: "true"},
		},
		Data: map[string][]byte{
			tokens.DataHashKey:     []byte("ABC123"),
			tokens.DataMetadataKey: []byte(`{"scope":"read"}`),
		},
	}

	_, err = secretHandler("demo/token", secret)
	require.NoError(t, err)

	tokenKV, err := js.KeyValue("tokens")
	require.NoError(t, err)

	_, err = tokenKV.Get("token/abc123")
	require.NoError(t, err)
}

func TestStartRegistrySyncSkipsWhenNATSURLBlank(t *testing.T) {
	ctx := context.Background()
	mgr := &Manager{}

	err := StartRegistrySync(ctx, mgr, RegistryConfig{NATSURL: "   "})
	require.NoError(t, err)
}

func TestStartRegistrySyncConnectError(t *testing.T) {
	ctx := context.Background()
	mgr := &Manager{}

	err := StartRegistrySync(ctx, mgr, RegistryConfig{NATSURL: "not-a-valid-url"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "connect NATS for registry")
}

func TestStartRegistrySyncBucketErrors(t *testing.T) {
	ns := startRegistryJetStreamServer(t)

	mgr := &Manager{
		Kold: &fakeKoldInterface{model: newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})},
		Core: fakeCoreControllers{secret: newControllerStub[*corev1.Secret, *corev1.SecretList](schema.GroupVersionKind{})},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgBadModels := RegistryConfig{
		NATSURL:      ns.ClientURL(),
		ModelsBucket: "invalid bucket name",
		TokensBucket: "tokens",
	}
	err := StartRegistrySync(ctx, mgr, cfgBadModels)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ensure models bucket")

	cfgBadTokens := RegistryConfig{
		NATSURL:      ns.ClientURL(),
		ModelsBucket: "models",
		TokensBucket: "invalid bucket name",
	}
	err = StartRegistrySync(ctx, mgr, cfgBadTokens)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ensure tokens bucket")
}

func TestStartRegistrySyncDrainsConnectionOnCancel(t *testing.T) {
	ns := startRegistryJetStreamServer(t)

	modelCtrl := newControllerStub[*v1.Model, *v1.ModelList](schema.GroupVersionKind{})
	secretCtrl := newControllerStub[*corev1.Secret, *corev1.SecretList](schema.GroupVersionKind{})

	mgr := &Manager{
		Kold: &fakeKoldInterface{model: modelCtrl},
		Core: fakeCoreControllers{secret: secretCtrl},
	}

	cfg := RegistryConfig{
		NATSURL:      ns.ClientURL(),
		ModelsBucket: "models",
		TokensBucket: "tokens",
	}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, StartRegistrySync(ctx, mgr, cfg))

	require.Eventually(t, func() bool {
		connz, err := ns.Connz(nil)
		require.NoError(t, err)
		return connz.Total == 1
	}, time.Second, 10*time.Millisecond)

	cancel()

	require.Eventually(t, func() bool {
		connz, err := ns.Connz(nil)
		require.NoError(t, err)
		return connz.Total == 0
	}, time.Second, 10*time.Millisecond)
}
