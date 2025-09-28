package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/gorizond/koldun/pkg/registry"
	"github.com/nats-io/nats.go"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/sirupsen/logrus"
)

// RegistryConfig configures publication of Models and Tokens to the shared NATS registry.
type RegistryConfig struct {
	NATSURL      string
	ModelsBucket string
	TokensBucket string
	ModelPrefix  string
	TokenPrefix  string
}

type registrySync struct {
	cfg    RegistryConfig
	log    *logrus.Entry
	models generic.ControllerInterface[*v1.Model, *v1.ModelList]
	tokens generic.ControllerInterface[*v1.Token, *v1.TokenList]

	conn     *nats.Conn
	js       nats.JetStreamContext
	modelsKV nats.KeyValue
	tokensKV nats.KeyValue
}

func StartRegistrySync(ctx context.Context, m *Manager, cfg RegistryConfig) error {
	if strings.TrimSpace(cfg.NATSURL) == "" {
		logrus.Info("registry sync disabled: operator-nats-url not set")
		return nil
	}
	if cfg.ModelsBucket == "" {
		cfg.ModelsBucket = registry.DefaultModelBucket
	}
	if cfg.TokensBucket == "" {
		cfg.TokensBucket = registry.DefaultTokenBucket
	}
	if cfg.ModelPrefix == "" {
		cfg.ModelPrefix = registry.DefaultModelPrefix
	}
	if cfg.TokenPrefix == "" {
		cfg.TokenPrefix = registry.DefaultTokenPrefix
	}

	conn, err := nats.Connect(cfg.NATSURL, nats.Name("koldun-registry"))
	if err != nil {
		return fmt.Errorf("connect NATS for registry: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return fmt.Errorf("jetstream context: %w", err)
	}

	modelsKV, err := ensureBucket(js, cfg.ModelsBucket)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ensure models bucket: %w", err)
	}
	tokensKV, err := ensureBucket(js, cfg.TokensBucket)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ensure tokens bucket: %w", err)
	}

	sync := &registrySync{
		cfg:      cfg,
		log:      logrus.StandardLogger().WithField("component", "registry-sync"),
		models:   m.Kold.Model(),
		tokens:   m.Kold.Token(),
		conn:     conn,
		js:       js,
		modelsKV: modelsKV,
		tokensKV: tokensKV,
	}

	sync.models.OnChange(ctx, "registry-sync-model", sync.onModelChange)
	sync.models.OnRemove(ctx, "registry-sync-model-remove", sync.onModelRemove)
	sync.tokens.OnChange(ctx, "registry-sync-token", sync.onTokenChange)
	sync.tokens.OnRemove(ctx, "registry-sync-token-remove", sync.onTokenRemove)

	go func() {
		<-ctx.Done()
		sync.conn.Drain()
	}()

	return nil
}

func ensureBucket(js nats.JetStreamContext, name string) (nats.KeyValue, error) {
	kv, err := js.KeyValue(name)
	if err == nats.ErrBucketNotFound {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  name,
			History: 1,
		})
	}
	return kv, err
}

func (r *registrySync) onModelChange(key string, model *v1.Model) (*v1.Model, error) {
	if model == nil {
		return nil, nil
	}

	if !modelReady(model) {
		if err := r.deleteModel(model.Namespace, model.Name); err != nil {
			r.log.WithError(err).WithField("model", modelKey(model.Namespace, model.Name)).Warn("remove model from registry")
		}
		return model, nil
	}

	entry := registry.Model{
		Namespace:           model.Namespace,
		Name:                model.Name,
		DisplayName:         model.Name,
		ConversionSizeBytes: model.Status.ConversionSizeBytes,
		ConversionSizeHuman: model.Status.ConversionSizeHuman,
		OutputPVCName:       model.Status.OutputPVCName,
		ReplicaPower:        model.Spec.ReplicaPower,
	}

	if err := r.putModel(&entry); err != nil {
		r.log.WithError(err).WithField("model", modelKey(model.Namespace, model.Name)).Error("publish model registry entry")
		return model, err
	}
	return model, nil
}

func (r *registrySync) onModelRemove(key string, model *v1.Model) (*v1.Model, error) {
	if model == nil {
		return nil, nil
	}
	if err := r.deleteModel(model.Namespace, model.Name); err != nil {
		r.log.WithError(err).WithField("model", modelKey(model.Namespace, model.Name)).Warn("remove model from registry")
	}
	return model, nil
}

func (r *registrySync) onTokenChange(key string, token *v1.Token) (*v1.Token, error) {
	if token == nil {
		return nil, nil
	}

	entry := registry.Token{
		Hash:      strings.ToLower(strings.TrimSpace(token.Spec.Hash)),
		Disabled:  token.Spec.Disabled,
		Namespace: token.Namespace,
		Metadata:  token.Spec.Metadata,
	}

	if entry.Hash == "" {
		r.log.WithField("token", token.Name).Warn("token hash empty; skipping registry publish")
		return token, nil
	}

	if err := r.putToken(&entry); err != nil {
		r.log.WithError(err).WithField("token", entry.Hash).Error("publish token registry entry")
		return token, err
	}
	return token, nil
}

func (r *registrySync) onTokenRemove(key string, token *v1.Token) (*v1.Token, error) {
	if token == nil {
		return nil, nil
	}
	hash := strings.ToLower(strings.TrimSpace(token.Spec.Hash))
	if hash == "" {
		return token, nil
	}
	if err := r.deleteToken(hash); err != nil {
		r.log.WithError(err).WithField("token", hash).Warn("remove token from registry")
	}
	return token, nil
}

func (r *registrySync) putModel(model *registry.Model) error {
	key := r.cfg.ModelPrefix + modelKey(model.Namespace, model.Name)
	payload, err := json.Marshal(model)
	if err != nil {
		return err
	}
	_, err = r.modelsKV.Put(key, payload)
	return err
}

func (r *registrySync) deleteModel(namespace, name string) error {
	key := r.cfg.ModelPrefix + modelKey(namespace, name)
	return ignoreNotFound(r.modelsKV.Delete(key))
}

func (r *registrySync) putToken(token *registry.Token) error {
	key := r.cfg.TokenPrefix + strings.ToLower(strings.TrimSpace(token.Hash))
	payload, err := json.Marshal(token)
	if err != nil {
		return err
	}
	_, err = r.tokensKV.Put(key, payload)
	return err
}

func (r *registrySync) deleteToken(hash string) error {
	key := r.cfg.TokenPrefix + strings.ToLower(strings.TrimSpace(hash))
	return ignoreNotFound(r.tokensKV.Delete(key))
}

func ignoreNotFound(err error) error {
	if err == nil || errors.Is(err, nats.ErrKeyNotFound) {
		return nil
	}
	return err
}

func modelKey(namespace, name string) string {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = "default"
	}
	return fmt.Sprintf("%s/%s", ns, strings.TrimSpace(name))
}

func modelReady(model *v1.Model) bool {
	if model == nil {
		return false
	}
	if strings.TrimSpace(model.Status.OutputPVCName) == "" {
		return false
	}
	if model.Status.ConversionSizeBytes <= 0 && strings.TrimSpace(model.Status.ConversionSizeHuman) == "" {
		return false
	}
	return isConditionTrue(model.Status.Conditions, conditionReady)
}
