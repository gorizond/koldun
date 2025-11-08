package controllers

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/registry"
	"github.com/gorizond/koldun/pkg/tokens"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestModelKey(t *testing.T) {

	require.Equal(t, "default/model", modelKey(" ", "model"))
	require.Equal(t, "ns/demo", modelKey("ns", " demo "))
}

func startRegistryJetStreamServer(t *testing.T) *server.Server {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server not ready")
	}

	t.Cleanup(func() {
		ns.Shutdown()
	})

	return ns
}

func TestEnsureBucketCreatesAndReuses(t *testing.T) {
	ns := startRegistryJetStreamServer(t)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() { nc.Drain(); nc.Close() })

	js, err := nc.JetStream()
	require.NoError(t, err)

	name := "registry-test-bucket"

	kv, err := ensureBucket(js, name)
	require.NoError(t, err)
	require.NotNil(t, kv)

	// Second invocation should return the existing bucket without error
	kv2, err := ensureBucket(js, name)
	require.NoError(t, err)
	require.NotNil(t, kv2)

	info, err := kv.Status()
	require.NoError(t, err)
	require.Equal(t, name, info.Bucket())
}

func TestIgnoreNotFound(t *testing.T) {

	require.NoError(t, ignoreNotFound(nil))
	require.NoError(t, ignoreNotFound(nats.ErrKeyNotFound))

	customErr := errors.New("boom")
	require.Equal(t, customErr, ignoreNotFound(customErr))
}

func TestModelReady(t *testing.T) {

	readyCondition := metav1.Condition{Type: conditionReady, Status: metav1.ConditionTrue}

	tests := []struct {
		name  string
		model *v1.Model
		ready bool
	}{
		{
			name:  "nil",
			model: nil,
			ready: false,
		},
		{
			name: "missing pvc",
			model: &v1.Model{
				Status: v1.ModelStatus{
					ConversionSizeBytes: 1,
					Conditions:          []metav1.Condition{readyCondition},
				},
			},
			ready: false,
		},
		{
			name: "missing size",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName: "output-pvc",
					Conditions:    []metav1.Condition{readyCondition},
				},
			},
			ready: false,
		},
		{
			name: "missing condition",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "output-pvc",
					ConversionSizeBytes: 1,
				},
			},
			ready: false,
		},
		{
			name: "ready",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "output-pvc",
					ConversionSizeBytes: 1,
					Conditions:          []metav1.Condition{readyCondition},
				},
			},
			ready: true,
		},
		{
			name: "ready with human size",
			model: &v1.Model{
				Status: v1.ModelStatus{
					OutputPVCName:       "output-pvc",
					ConversionSizeHuman: "1Gi",
					Conditions:          []metav1.Condition{readyCondition},
				},
			},
			ready: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.ready, modelReady(tt.model))
		})
	}
}

func TestRegistrySyncPutModelAndDelete(t *testing.T) {

	kv := &fakeMemoryKV{
		payload: make(map[string][]byte),
	}

	sync := &registrySync{
		cfg:      RegistryConfig{ModelPrefix: "model/"},
		log:      logrus.New().WithField("component", "registry-test"),
		modelsKV: kv,
	}

	entry := &registry.Model{
		Namespace:           "demo",
		Name:                "model",
		DisplayName:         "demo-model",
		ConversionSizeBytes: 42,
	}

	require.NoError(t, sync.putModel(entry))

	require.Contains(t, kv.payload, "model/demo/model")

	stored := kv.payload["model/demo/model"]
	var decoded registry.Model
	require.NoError(t, json.Unmarshal(stored, &decoded))
	require.Equal(t, *entry, decoded)
	require.Equal(t, []string{"model/demo/model"}, kv.putCalls)

	require.NoError(t, sync.deleteModel("demo", "model"))
	require.Equal(t, []string{"model/demo/model"}, kv.deleteCalls)

	require.NoError(t, sync.deleteModel("demo", "missing"), "ignore missing keys")
}

func TestRegistrySyncPutModelHandlesKVErrors(t *testing.T) {

	kv := &fakeMemoryKV{putErr: errors.New("kv failure")}

	sync := &registrySync{
		cfg:      RegistryConfig{ModelPrefix: "model/"},
		log:      logrus.New().WithField("component", "registry-test"),
		modelsKV: kv,
	}

	err := sync.putModel(&registry.Model{Namespace: "demo", Name: "model"})
	require.EqualError(t, err, "kv failure")
	require.Empty(t, kv.putCalls)
}

func TestRegistrySyncPutModelHandlesMarshalErrors(t *testing.T) {
	origMarshal := jsonMarshal
	t.Cleanup(func() { jsonMarshal = origMarshal })

	jsonMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal failure") }

	sync := &registrySync{cfg: RegistryConfig{ModelPrefix: "model/"}, log: logrus.New().WithField("component", "registry-test"), modelsKV: &fakeMemoryKV{}}
	err := sync.putModel(&registry.Model{Namespace: "demo", Name: "model"})
	require.EqualError(t, err, "marshal failure")
}

func TestRegistrySyncPutTokenAndDelete(t *testing.T) {

	kv := &fakeMemoryKV{
		payload: make(map[string][]byte),
	}

	sync := &registrySync{
		cfg:      RegistryConfig{TokenPrefix: "token/"},
		log:      logrus.New().WithField("component", "registry-test"),
		tokensKV: kv,
	}

	token := &registry.Token{
		Hash:      "ABC123",
		Disabled:  true,
		Namespace: "demo",
		Metadata:  map[string]string{"role": "reader"},
	}

	require.NoError(t, sync.putToken(token))
	require.Contains(t, kv.payload, "token/abc123")

	require.NoError(t, sync.deleteToken("ABC123"))
	require.Equal(t, []string{"token/abc123"}, kv.deleteCalls)

	require.NoError(t, sync.deleteToken("missing"), "ignore missing keys")
}

func TestRegistrySyncPutTokenHandlesKVErrors(t *testing.T) {

	kv := &fakeMemoryKV{putErr: errors.New("kv failure")}

	sync := &registrySync{
		cfg:      RegistryConfig{TokenPrefix: "token/"},
		log:      logrus.New().WithField("component", "registry-test"),
		tokensKV: kv,
	}

	token := &registry.Token{Hash: "abc123"}
	err := sync.putToken(token)
	require.EqualError(t, err, "kv failure")
	require.Empty(t, kv.putCalls)
}

func TestRegistrySyncPutTokenHandlesMarshalErrors(t *testing.T) {
	origMarshal := jsonMarshal
	t.Cleanup(func() { jsonMarshal = origMarshal })

	jsonMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal failure") }

	sync := &registrySync{cfg: RegistryConfig{TokenPrefix: "token/"}, log: logrus.New().WithField("component", "registry-test"), tokensKV: &fakeMemoryKV{}}
	err := sync.putToken(&registry.Token{Hash: "abc123"})
	require.EqualError(t, err, "marshal failure")
}

func TestRegistrySyncPutTokenRejectsInvalidHash(t *testing.T) {

	sync := &registrySync{
		cfg:      RegistryConfig{TokenPrefix: "token/"},
		log:      logrus.New().WithField("component", "registry-test"),
		tokensKV: &fakeMemoryKV{},
	}

	token := &registry.Token{
		Hash: "$1:9770a2e01028d699",
	}

	err := sync.putToken(token)
	require.EqualError(t, err, `invalid token hash: "$1:9770a2e01028d699"`)
}

func TestRegistrySyncDeleteTokenSkipsInvalidHash(t *testing.T) {

	kv := &fakeMemoryKV{
		payload: make(map[string][]byte),
	}

	sync := &registrySync{
		cfg:      RegistryConfig{TokenPrefix: "token/"},
		log:      logrus.New().WithField("component", "registry-test"),
		tokensKV: kv,
	}

	require.NoError(t, sync.deleteToken("$1:9770a2e01028d699"))
	require.Empty(t, kv.deleteCalls, "invalid hashes should be ignored")
}

func TestRegistrySyncOnModelChange(t *testing.T) {

	t.Run("nil model ignored", func(t *testing.T) {
		sync := &registrySync{}

		result, err := sync.onModelChange("demo/model", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("ready model published", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: make(map[string][]byte),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{ModelPrefix: "model/"},
			log:      logrus.New().WithField("component", "registry-test"),
			modelsKV: kv,
		}

		model := &v1.Model{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "model",
			},
			Spec: v1.ModelSpec{
				ReplicaPower: 3,
			},
			Status: v1.ModelStatus{
				OutputPVCName:       "output-pvc",
				ConversionSizeBytes: 128,
				Conditions: []metav1.Condition{
					{Type: conditionReady, Status: metav1.ConditionTrue},
				},
			},
		}

		result, err := sync.onModelChange("demo/model", model)
		require.NoError(t, err)
		require.Equal(t, model, result)
		require.Equal(t, []string{"model/demo/model"}, kv.putCalls)
	})

	t.Run("not ready model removed", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: map[string][]byte{
				"model/demo/model": []byte(`{"namespace":"demo","name":"model"}`),
			},
		}

		sync := &registrySync{
			cfg:      RegistryConfig{ModelPrefix: "model/"},
			log:      logrus.New().WithField("component", "registry-test"),
			modelsKV: kv,
		}

		model := &v1.Model{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "model",
			},
			Status: v1.ModelStatus{},
		}

		result, err := sync.onModelChange("demo/model", model)
		require.NoError(t, err)
		require.Equal(t, model, result)
		require.Equal(t, []string{"model/demo/model"}, kv.deleteCalls)
	})

	t.Run("not ready delete error tolerated", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: map[string][]byte{
				"model/demo/model": []byte(`{"namespace":"demo","name":"model"}`),
			},
			deleteErr: errors.New("kv delete failure"),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{ModelPrefix: "model/"},
			log:      logrus.New().WithField("component", "registry-test"),
			modelsKV: kv,
		}

		model := &v1.Model{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "model",
			},
		}

		result, err := sync.onModelChange("demo/model", model)
		require.NoError(t, err)
		require.Equal(t, model, result)
		require.Equal(t, []string{"model/demo/model"}, kv.deleteCalls)
	})

	t.Run("error bubbles up", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: make(map[string][]byte),
			putErr:  errors.New("kv put failure"),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{ModelPrefix: "model/"},
			log:      logrus.New().WithField("component", "registry-test"),
			modelsKV: kv,
		}

		model := &v1.Model{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "model",
			},
			Status: v1.ModelStatus{
				OutputPVCName:       "output",
				ConversionSizeHuman: "1Gi",
				Conditions: []metav1.Condition{
					{Type: conditionReady, Status: metav1.ConditionTrue},
				},
			},
		}

		_, err := sync.onModelChange("demo/model", model)
		require.EqualError(t, err, "kv put failure")
	})
}

func TestRegistrySyncOnModelRemove(t *testing.T) {

	sync := &registrySync{
		cfg: RegistryConfig{ModelPrefix: "model/"},
		log: logrus.New().WithField("component", "registry-test"),
		modelsKV: &fakeMemoryKV{
			payload: map[string][]byte{
				"model/demo/model": []byte(`{"namespace":"demo","name":"model"}`),
			},
		},
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "demo",
			Name:      "model",
		},
	}

	result, err := sync.onModelRemove("demo/model", model)
	require.NoError(t, err)
	require.Equal(t, model, result)

	require.Equal(t, []string{"model/demo/model"}, sync.modelsKV.(*fakeMemoryKV).deleteCalls)

	nilModel, err := sync.onModelRemove("demo/model", nil)
	require.NoError(t, err)
	require.Nil(t, nilModel)
}

func TestRegistrySyncOnModelRemoveHandlesDeleteErrors(t *testing.T) {
	kv := &fakeMemoryKV{
		payload:   map[string][]byte{"model/demo/model": []byte(`{}`)},
		deleteErr: errors.New("delete failure"),
	}

	sync := &registrySync{
		cfg:      RegistryConfig{ModelPrefix: "model/"},
		log:      logrus.New().WithField("component", "registry-test"),
		modelsKV: kv,
	}

	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "model"}}
	result, err := sync.onModelRemove("demo/model", model)
	require.NoError(t, err)
	require.Equal(t, model, result)
	require.Equal(t, []string{"model/demo/model"}, kv.deleteCalls)
}

func TestRegistrySyncOnSecretChange(t *testing.T) {

	t.Run("non token secret removes hash", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: map[string][]byte{
				"token/abc123": []byte(`{"hash":"abc123"}`),
			},
		}

		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: kv,
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "token",
				Labels: map[string]string{
					"unrelated": "true",
				},
			},
			Data: map[string][]byte{
				tokens.DataHashKey: []byte("ABC123"),
			},
		}

		result, err := sync.onSecretChange("key", secret)
		require.Nil(t, result)
		require.NoError(t, err)
		require.Equal(t, []string{"token/abc123"}, kv.deleteCalls)
	})

	t.Run("non token delete error tolerated", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: map[string][]byte{
				"token/abc123": []byte(`{"hash":"abc123"}`),
			},
			deleteErr: errors.New("kv delete failure"),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: kv,
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "token",
			},
			Data: map[string][]byte{
				tokens.DataHashKey: []byte("ABC123"),
			},
		}

		result, err := sync.onSecretChange("key", secret)
		require.Nil(t, result)
		require.NoError(t, err)
		require.Equal(t, []string{"token/abc123"}, kv.deleteCalls)
	})

	t.Run("nil secret ignored", func(t *testing.T) {
		sync := &registrySync{}
		secret, err := sync.onSecretChange("key", nil)
		require.Nil(t, secret)
		require.NoError(t, err)
	})

	t.Run("missing hash ignored", func(t *testing.T) {
		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: &fakeMemoryKV{},
		}

		secret, err := sync.onSecretChange("key", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "token",
				Labels: map[string]string{
					tokens.LabelToken: "true",
				},
			},
			Data: map[string][]byte{},
		})
		require.Nil(t, secret)
		require.NoError(t, err)
	})

	t.Run("deletion removes token", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: make(map[string][]byte),
		}
		kv.payload["token/abc123"] = []byte(`{"hash":"abc123"}`)

		ts := metav1.NewTime(time.Now())

		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: kv,
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "demo",
				Name:              "token",
				Labels:            map[string]string{tokens.LabelToken: "true"},
				DeletionTimestamp: &ts,
			},
			Data: map[string][]byte{
				tokens.DataHashKey: []byte("ABC123"),
			},
		}

		result, err := sync.onSecretChange("key", secret)
		require.Nil(t, result)
		require.NoError(t, err)
		require.Equal(t, []string{"token/abc123"}, kv.deleteCalls)
	})

	t.Run("publish token entry", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: make(map[string][]byte),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: kv,
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "demo",
				Name:        "token",
				Labels:      map[string]string{tokens.LabelToken: "true"},
				Annotations: map[string]string{tokens.AnnotationMetadataPrefix + "env": "prod"},
			},
			Data: map[string][]byte{
				tokens.DataHashKey:     []byte("ABC123"),
				tokens.DataMetadataKey: []byte(`{"role":"writer"}`),
			},
		}

		result, err := sync.onSecretChange("key", secret)
		require.Nil(t, result)
		require.NoError(t, err)
		require.Equal(t, []string{"token/abc123"}, kv.putCalls)

		stored := kv.payload["token/abc123"]
		var entry registry.Token
		require.NoError(t, json.Unmarshal(stored, &entry))
		require.Equal(t, "abc123", entry.Hash)
		require.Equal(t, map[string]string{"role": "writer", "env": "prod"}, entry.Metadata)
	})

	t.Run("put error returned", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: make(map[string][]byte),
			putErr:  errors.New("kv put failed"),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: kv,
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "token",
				Labels:    map[string]string{tokens.LabelToken: "true"},
			},
			Data: map[string][]byte{
				tokens.DataHashKey: []byte("ABC123"),
			},
		}

		_, err := sync.onSecretChange("key", secret)
		require.EqualError(t, err, "kv put failed")
	})

	t.Run("extract registry token error ignored", func(t *testing.T) {
		kv := &fakeMemoryKV{
			payload: make(map[string][]byte),
		}

		sync := &registrySync{
			cfg:      RegistryConfig{TokenPrefix: "token/"},
			log:      logrus.New().WithField("component", "registry-test"),
			tokensKV: kv,
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "demo",
				Name:      "token",
				Labels:    map[string]string{tokens.LabelToken: "true"},
			},
			Data: map[string][]byte{
				tokens.DataHashKey:     []byte("ABC123"),
				tokens.DataMetadataKey: []byte("{invalid"),
			},
		}

		result, err := sync.onSecretChange("key", secret)
		require.Nil(t, result)
		require.NoError(t, err)
		require.Empty(t, kv.putCalls)
	})
}

// Ensure fakeMemoryKV satisfies interface expectations in this package too.
var _ nats.KeyValue = (*fakeMemoryKV)(nil)
