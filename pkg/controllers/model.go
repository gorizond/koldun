package controllers

import (
	"context"
	"fmt"

    v1 "github.com/gorizond/kold/pkg/apis/kold.gorizond.io/v1"
    "github.com/rancher/wrangler/v3/pkg/apply"
    "github.com/rancher/wrangler/v3/pkg/generic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type modelHandler struct {
	ctx    context.Context
	apply  apply.Apply
	models generic.ControllerInterface[*v1.Model, *v1.ModelList]
}

func registerModelController(ctx context.Context, m *Manager) error {
	handler := &modelHandler{
		ctx:    ctx,
		apply:  m.Apply(ctx),
		models: m.Kold.Model(),
	}

	handler.models.OnChange(ctx, "kold-model-controller", handler.onChange)
	handler.models.OnRemove(ctx, "kold-model-controller", handler.onRemove)
	return nil
}

func (h *modelHandler) onChange(key string, obj *v1.Model) (*v1.Model, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureMetadataConfigMap(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *modelHandler) onRemove(key string, obj *v1.Model) (*v1.Model, error) {
	return obj, nil
}

func (h *modelHandler) ensureMetadataConfigMap(obj *v1.Model) error {
	metadataName := fmt.Sprintf("%s-metadata", obj.Name)
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      metadataName,
			Namespace: obj.Namespace,
			Labels: map[string]string{
				labelComponent: componentModel,
			},
		},
		Data: map[string]string{
			"sourceURI": obj.Spec.SourceURI,
			"localPath": obj.Spec.LocalPath,
		},
	}

	if obj.Spec.CacheSpec != nil {
		cm.Data["cacheEndpoint"] = obj.Spec.CacheSpec.Endpoint
		cm.Data["cacheBucket"] = obj.Spec.CacheSpec.Bucket
		if obj.Spec.CacheSpec.SecretRef != nil {
			cm.Data["cacheSecret"] = obj.Spec.CacheSpec.SecretRef.Name
		}
	}

	return h.apply.WithOwner(obj).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(cm)
}

func (h *modelHandler) ensureStatus(obj *v1.Model) (*v1.Model, error) {
	updated := obj.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	ready := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "CachePrepared",
		Message: "Model metadata cached",
	}

	changed := setCondition(&updated.Status.Conditions, ready)
	if updated.Status.ObservedGeneration != updated.Generation {
		updated.Status.ObservedGeneration = updated.Generation
		changed = true
	}

	if !changed {
		return obj, nil
	}

	return h.models.UpdateStatus(updated)
}
