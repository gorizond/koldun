package controllers

import (
	"fmt"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// model_config.go
// Contains functions for managing ConfigMaps related to Models.
// This includes metadata ConfigMaps for passing configuration to Jobs,
// and script ConfigMaps for embedding Python scripts needed during download/conversion.

// ensureMetadataConfigMap creates or updates a ConfigMap containing model metadata
// that is used by download and conversion Jobs. It includes source URL, local path,
// object storage configuration, and credentials references.
func (h *modelHandler) ensureMetadataConfigMap(obj *v1.Model) error {
	metadataName := fmt.Sprintf("%s-metadata", obj.Name)
	klog.V(1).Infof("Model %s/%s: ensuring metadata ConfigMap %s", obj.Namespace, obj.Name, metadataName)
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
				labelModelName: obj.Name,
			},
		},
		Data: map[string]string{
			"sourceUrl": obj.Spec.SourceURL,
			"localPath": obj.Spec.LocalPath,
		},
	}

	if storage := obj.Spec.ObjectStorage; storage != nil {
		cm.Data["objectStorageEndpoint"] = storage.Endpoint
		cm.Data["objectStorageBucketForSource"] = storage.BucketForSource
		cm.Data["objectStorageBucketForConvert"] = storage.BucketForConvert
		// Preserve legacy keys for backward compatibility with existing consumers.
		cm.Data["cacheEndpoint"] = storage.Endpoint
		cm.Data["cacheBucket"] = storage.BucketForSource
		if storage.SecretRef != nil {
			cm.Data["objectStorageSecret"] = storage.SecretRef.Name
			cm.Data["cacheSecret"] = storage.SecretRef.Name
		}
	}

	err := h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("koldun-model-metadata-%s", obj.Name)).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(cm)

	if err != nil {
		klog.Errorf("Model %s/%s: failed to create metadata ConfigMap %s: %v", obj.Namespace, obj.Name, metadataName, err)
		return err
	}

	klog.V(1).Infof("Model %s/%s: successfully created metadata ConfigMap %s", obj.Namespace, obj.Name, metadataName)
	return nil
}

// ensureScriptConfigMap creates or updates a ConfigMap containing the Python download script
// that is used by download Jobs to fetch model files from various sources (HTTP, HuggingFace, etc.).
func (h *modelHandler) ensureScriptConfigMap(obj *v1.Model) error {
	scriptName := fmt.Sprintf("%s-download-script", obj.Name)
	klog.V(1).Infof("Model %s/%s: ensuring script ConfigMap %s", obj.Namespace, obj.Name, scriptName)
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      scriptName,
			Namespace: obj.Namespace,
			Labels: map[string]string{
				labelComponent: componentModel,
				labelModelName: obj.Name,
			},
		},
		Data: map[string]string{
			"download.py": DownloadPy,
		},
	}

	err := h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("koldun-model-script-%s", obj.Name)).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(cm)

	if err != nil {
		klog.Errorf("Model %s/%s: failed to create script ConfigMap %s: %v", obj.Namespace, obj.Name, scriptName, err)
		return err
	}

	klog.V(1).Infof("Model %s/%s: successfully created script ConfigMap %s", obj.Namespace, obj.Name, scriptName)
	return nil
}
