package controllers

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
)

const defaultWriterURL = "https://raw.githubusercontent.com/gorizond/koldun/main/converter/writer.py"

// model_jobs.go
// Contains functions for managing Kubernetes Jobs related to Model operations.
// This includes download jobs, conversion jobs, sizing jobs, and their lifecycle management.

func (h *modelHandler) ensureDownloadJob(obj *v1.Model) error {
	storage := obj.Spec.ObjectStorage
	sourceURL := strings.TrimSpace(obj.Spec.SourceURL)
	if storage == nil || strings.TrimSpace(storage.BucketForSource) == "" || sourceURL == "" {
		klog.V(4).Infof("Model %s/%s: skipping download job creation - missing objectStorage or source URL", obj.Namespace, obj.Name)
		return nil
	}

	spec := effectiveDownloadSpec(obj.Spec.Download)
	jobName := jobNameForModel(obj)
	expectedGeneration := fmt.Sprintf("%d", obj.Generation)

	klog.V(1).Infof("Model %s/%s: ensureDownloadJob called - Status.DownloadState=%s, Status.ObservedGeneration=%d, obj.Generation=%d",
		obj.Namespace, obj.Name, obj.Status.DownloadState, obj.Status.ObservedGeneration, obj.Generation)

	// Check if generation has changed recently - this might be causing the issue
	if obj.Status.ObservedGeneration != 0 && obj.Generation > obj.Status.ObservedGeneration+1 {
		klog.Warningf("Model %s/%s: generation jumped from %d to %d - this might cause job recreation",
			obj.Namespace, obj.Name, obj.Status.ObservedGeneration, obj.Generation)
	}

	if strings.EqualFold(obj.Status.DownloadState, "Succeeded") && obj.Status.ObservedGeneration == obj.Generation {
		klog.V(4).Infof("Model %s/%s: download already succeeded for generation %d", obj.Namespace, obj.Name, obj.Generation)
		return nil
	}

	klog.V(1).Infof("Model %s/%s: ensuring download job %s for generation %d", obj.Namespace, obj.Name, jobName, obj.Generation)

	if existing, err := h.jobs.Cache().Get(obj.Namespace, jobName); err == nil && existing != nil {
		klog.V(1).Infof("Model %s/%s: found existing job %s - DeletionTimestamp=%v, Status.Active=%d, Status.Succeeded=%d, Status.Failed=%d",
			obj.Namespace, obj.Name, jobName, existing.DeletionTimestamp, existing.Status.Active, existing.Status.Succeeded, existing.Status.Failed)

		if existing.DeletionTimestamp != nil {
			// Job is being deleted, wait for it to be fully removed before creating a new one
			klog.V(1).Infof("Model %s/%s: job %s is being deleted, waiting for completion", obj.Namespace, obj.Name, jobName)
			return nil
		}
		currentGen := existing.Annotations[annotationModelGeneration]
		if currentGen == "" {
			currentGen = existing.Labels[annotationModelGeneration]
		}
		klog.V(1).Infof("Model %s/%s: existing job generation=%s, expected=%s", obj.Namespace, obj.Name, currentGen, expectedGeneration)

		// If job has no generation annotation, it's an old job that should be deleted
		if currentGen == "" {
			klog.V(1).Infof("Model %s/%s: job %s has no generation annotation, will delete it", obj.Namespace, obj.Name, jobName)
			if !isJobFinished(existing) {
				klog.V(1).Infof("Model %s/%s: job %s is not finished but has no generation, waiting for it to finish", obj.Namespace, obj.Name, jobName)
				return nil
			}
			// Job is finished but has no generation, delete it
			klog.V(1).Infof("Model %s/%s: deleting job %s with no generation annotation", obj.Namespace, obj.Name, jobName)
			propagation := metav1.DeletePropagationBackground
			if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
				return fmt.Errorf("failed to delete job without generation: %w", err)
			}
			// Persist annotation to avoid immediate recreation and exit reconcile to honor cooldown
			h.persistModelAnnotation(obj, annotationJobDeletedAt, time.Now().Format(time.RFC3339))
			return nil
		}

		if currentGen == expectedGeneration {
			// Job is already up to date with the current generation
			klog.V(1).Infof("Model %s/%s: job %s already exists with correct generation %s", obj.Namespace, obj.Name, jobName, currentGen)
			return nil
		}
		if !isJobFinished(existing) {
			// Job is still running, don't interrupt it
			klog.V(1).Infof("Model %s/%s: job %s is still running with generation %s, not interrupting", obj.Namespace, obj.Name, jobName, currentGen)
			return nil
		}
		// Job is finished but has wrong generation, delete it
		klog.V(1).Infof("Model %s/%s: deleting finished job %s with outdated generation %s (expected %s)", obj.Namespace, obj.Name, jobName, currentGen, expectedGeneration)
		propagation := metav1.DeletePropagationBackground
		if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			return fmt.Errorf("failed to delete outdated job: %w", err)
		}
		// Persist annotation to avoid immediate recreation and exit reconcile to honor cooldown
		h.persistModelAnnotation(obj, annotationJobDeletedAt, time.Now().Format(time.RFC3339))
		return nil
	}

	// Track whether a recent job creation was recorded to avoid immediate recreation.
	recentDownloadCreation := false
	hasDownloadCreationCondition := false
	var timeSinceCreation time.Duration
	if obj.Status.DownloadJobName == jobName && obj.Status.DownloadJobName != "" {
		for _, cond := range obj.Status.Conditions {
			if cond.Type != conditionDownloaded || cond.Status != metav1.ConditionFalse {
				continue
			}
			if cond.Reason != "JobCreated" && cond.Reason != "JobPending" {
				continue
			}
			hasDownloadCreationCondition = true
			if !cond.LastTransitionTime.IsZero() {
				timeSinceCreation = time.Since(cond.LastTransitionTime.Time)
				if cond.LastTransitionTime.Time.Add(60 * time.Second).After(time.Now()) {
					recentDownloadCreation = true
				}
			} else {
				recentDownloadCreation = true
			}
			break
		}
	}

	// Additional check: if we already have a job with this name and it's not failed, don't recreate
	klog.V(1).Infof("Model %s/%s: checking Status.DownloadJobName=%s, Status.DownloadState=%s",
		obj.Namespace, obj.Name, obj.Status.DownloadJobName, obj.Status.DownloadState)

	if obj.Status.DownloadJobName == jobName && obj.Status.DownloadJobName != "" {
		if !strings.EqualFold(obj.Status.DownloadState, "Failed") && recentDownloadCreation {
			// Job exists and is not failed, no need to recreate
			klog.V(1).Infof("Model %s/%s: job %s already exists in status and is not failed (created %v ago), skipping recreation",
				obj.Namespace, obj.Name, jobName, timeSinceCreation)
			return nil
		}
		if !strings.EqualFold(obj.Status.DownloadState, "Failed") && !hasDownloadCreationCondition {
			klog.V(1).Infof("Model %s/%s: job %s tracked in status without creation condition, assuming still running", obj.Namespace, obj.Name, jobName)
			return nil
		}
	}

	// Final safety check: ensure we don't create a job if one already exists and is running
	if existing, err := h.jobs.Cache().Get(obj.Namespace, jobName); err == nil && existing != nil {
		klog.V(1).Infof("Model %s/%s: final safety check - found existing job %s", obj.Namespace, obj.Name, jobName)
		if !isJobFinished(existing) {
			klog.V(1).Infof("Model %s/%s: job %s already exists and is not finished, skipping creation", obj.Namespace, obj.Name, jobName)
			return nil
		}
	}

	// Additional check: if we recently created a job, don't create another one immediately
	// This prevents rapid recreation cycles
	klog.V(1).Infof("Model %s/%s: checking recent job creation conditions", obj.Namespace, obj.Name)
	if obj.Status.DownloadJobName == jobName && hasDownloadCreationCondition && recentDownloadCreation {
		klog.V(1).Infof("Model %s/%s: job %s was recently created (%v ago), skipping immediate recreation",
			obj.Namespace, obj.Name, jobName, timeSinceCreation)
		return nil
	}

	// Check if we recently deleted a job and shouldn't create another one immediately
	klog.V(1).Infof("Model %s/%s: checking recent job deletion annotation", obj.Namespace, obj.Name)
	if deletedAtStr, exists := obj.Annotations[annotationJobDeletedAt]; exists {
		klog.V(1).Infof("Model %s/%s: found deletion annotation: %s", obj.Namespace, obj.Name, deletedAtStr)
		if deletedAt, err := time.Parse(time.RFC3339, deletedAtStr); err == nil {
			// Don't create a new job within 60 seconds of deleting the previous one (increased from 30)
			if time.Since(deletedAt) < 60*time.Second {
				klog.V(1).Infof("Model %s/%s: job %s was recently deleted (%v ago), waiting before recreation",
					obj.Namespace, obj.Name, jobName, time.Since(deletedAt))
				return nil
			}
		}
	}

	// Ensure the script ConfigMap exists before creating the job
	// Note: ConfigMaps are already ensured in onChange() before ensureDownloadJob() is called
	klog.V(1).Infof("Model %s/%s: ConfigMaps should already exist from onChange()", obj.Namespace, obj.Name)

	// Add a small delay to ensure ConfigMaps are fully created
	// Note: This delay might cause issues in fast reconciliation loops
	// time.Sleep(100 * time.Millisecond)

	klog.V(1).Infof("Model %s/%s: all checks passed, proceeding to create job %s", obj.Namespace, obj.Name, jobName)
	labels := map[string]string{
		labelComponent: componentModel,
		labelModelName: obj.Name,
	}
	if dllama := labelValue(obj.Labels, labelDllamaName); dllama != "" {
		labels[labelDllamaName] = dllama
	}

	objectKey := modelObjectKey(obj)
	if objectKey == "" {
		objectKey = fmt.Sprintf("models/%s", obj.Name)
	}

	backoffLimit := int32(10)
	// Keep finished jobs/pods longer to inspect failures; default 5 minutes for Kind clusters
	ttl := int32(300)
	if v, ok := obj.Annotations["koldun.gorizond.io/ttl-seconds"]; ok {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			ttl = int32(secs)
		}
	}

	annotations := map[string]string{
		annotationModelGeneration: expectedGeneration,
	}

	// Build container responsible for performing the download
	downloadContainer := h.buildDownloadContainer(obj, spec, sourceURL, objectKey, expectedGeneration)
	downloadContainer.VolumeMounts = append(downloadContainer.VolumeMounts, corev1.VolumeMount{
		Name:      "script",
		MountPath: "/opt/script",
		ReadOnly:  true,
	})
	containers := []corev1.Container{downloadContainer}

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: batchv1.SchemeGroupVersion.String(),
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   obj.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "script",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: fmt.Sprintf("%s-download-script", obj.Name),
									},
									Optional: pointer.Bool(false),
								},
							},
						},
					},
					Containers: containers,
				},
			},
		},
	}

	// Ensure default service account can list secrets if needed.
	klog.V(2).Infof("Model %s/%s: creating download job %s", obj.Namespace, obj.Name, jobName)
	return h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("koldun-model-job-%s", obj.Name)).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(job)
}

// persistModelAnnotation updates model metadata annotations via standard Update (not UpdateStatus)
// so that cooldown markers like job-deleted-at are actually saved in the API server.

func (h *modelHandler) persistModelAnnotation(obj *v1.Model, key, value string) {
	updated := obj.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[key] = value
	if _, err := h.models.Update(updated); err != nil {
		klog.Errorf("Model %s/%s: failed to persist annotation %s=%s: %v", obj.Namespace, obj.Name, key, value, err)
	} else {
		klog.V(1).Infof("Model %s/%s: persisted annotation %s=%s", obj.Namespace, obj.Name, key, value)
	}
}

func (h *modelHandler) ensureConversionJob(obj *v1.Model) error {
	if obj.Spec.Conversion == nil {
		return nil
	}
	storage := obj.Spec.ObjectStorage
	if storage == nil || strings.TrimSpace(storage.BucketForSource) == "" {
		klog.V(2).Infof("Model %s/%s: conversion requested but objectStorage.bucketForSource missing", obj.Namespace, obj.Name)
		return nil
	}
	if strings.TrimSpace(storage.BucketForConvert) == "" {
		klog.V(2).Infof("Model %s/%s: conversion requested but objectStorage.bucketForConvert missing", obj.Namespace, obj.Name)
		return nil
	}
	if !strings.EqualFold(obj.Status.DownloadState, "Succeeded") || obj.Status.ObservedGeneration != obj.Generation {
		klog.V(2).Infof("Model %s/%s: waiting for download success before starting conversion (state=%s, observedGen=%d, gen=%d)",
			obj.Namespace, obj.Name, obj.Status.DownloadState, obj.Status.ObservedGeneration, obj.Generation)
		return nil
	}

	spec := effectiveConversionSpec(obj.Spec.Conversion)
	jobName := conversionJobName(obj)
	expectedGeneration := fmt.Sprintf("%d", obj.Generation)

	if strings.EqualFold(obj.Status.ConversionState, "Succeeded") && obj.Status.ObservedGeneration == obj.Generation {
		klog.V(2).Infof("Model %s/%s: conversion already succeeded for generation %s, skipping", obj.Namespace, obj.Name, expectedGeneration)
		return nil
	}

	if existing, err := h.jobs.Cache().Get(obj.Namespace, jobName); err == nil && existing != nil {
		if existing.DeletionTimestamp != nil {
			return nil
		}
		currentGen := existing.Annotations[annotationModelGeneration]
		if currentGen == "" {
			currentGen = existing.Labels[annotationModelGeneration]
		}
		if currentGen == expectedGeneration {
			if !isJobFinished(existing) {
				return nil
			}
			return nil
		}
		if !isJobFinished(existing) {
			return nil
		}
		klog.V(1).Infof("Model %s/%s: deleting finished conversion job %s with outdated generation %s (expected %s)",
			obj.Namespace, obj.Name, jobName, currentGen, expectedGeneration)
		propagation := metav1.DeletePropagationBackground
		if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			return fmt.Errorf("failed to delete outdated conversion job: %w", err)
		}
		return nil
	}

	if obj.Status.ConversionJobName == jobName && !strings.EqualFold(obj.Status.ConversionState, "Failed") && obj.Status.ConversionState != "" {
		klog.V(2).Infof("Model %s/%s: conversion job %s already tracked in status (state=%s), skipping recreation",
			obj.Namespace, obj.Name, jobName, obj.Status.ConversionState)
		return nil
	}

	inputKey := modelObjectKey(obj)
	if inputKey == "" {
		inputKey = fmt.Sprintf("models/%s", obj.Name)
	}
	_, convBucket, convKey, convURI := conversionPaths(obj, spec, inputKey)
	weightsType := spec.WeightsFloatType

	labels := map[string]string{
		labelComponent: componentModel,
		labelModelName: obj.Name,
	}
	if dllama := labelValue(obj.Labels, labelDllamaName); dllama != "" {
		labels[labelDllamaName] = dllama
	}

	annotations := map[string]string{annotationModelGeneration: expectedGeneration}

	toolsImage := spec.ToolsImage
	if toolsImage == "" {
		toolsImage = defaultToolsImage
	}

	backoffLimit := int32(5)
	ttl := int32(300)

	// S3 CSI: ensure static PV and PVC, then define volumes
	s3Bucket := storage.BucketForSource
	s3Prefix := strings.TrimLeft(inputKey, "/")
	if b, k, ok := parseS3Path(obj.Spec.LocalPath); ok {
		if strings.TrimSpace(b) != "" {
			s3Bucket = b
		}
		if strings.TrimSpace(k) != "" {
			s3Prefix = strings.TrimLeft(k, "/")
		}
	}
	pvName := fmt.Sprintf("%s-s3-pv", obj.Name)
	pvcName := fmt.Sprintf("%s-s3-pvc", obj.Name)
	volHandle := fmt.Sprintf("%s/%s", s3Bucket, s3Prefix)
	// Defaults; may be overridden by Model.Spec.PV
	pvCapStr := "10Gi"
	pvClass := "csi-s3"
	pvAccess := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	pvReclaim := corev1.PersistentVolumeReclaimRetain
	csiDriver := "ru.yandex.s3.csi"
	volAttrs := map[string]string{}
	if obj.Spec.PV != nil {
		if strings.TrimSpace(obj.Spec.PV.Capacity) != "" {
			pvCapStr = obj.Spec.PV.Capacity
		}
		if strings.TrimSpace(obj.Spec.PV.StorageClassName) != "" {
			pvClass = obj.Spec.PV.StorageClassName
		}
		if strings.TrimSpace(obj.Spec.PV.ReclaimPolicy) != "" {
			switch strings.ToLower(obj.Spec.PV.ReclaimPolicy) {
			case "delete":
				pvReclaim = corev1.PersistentVolumeReclaimDelete
			case "recycle":
				pvReclaim = corev1.PersistentVolumeReclaimRecycle
			default:
				pvReclaim = corev1.PersistentVolumeReclaimRetain
			}
		}
		if strings.TrimSpace(obj.Spec.PV.CSIDriver) != "" {
			csiDriver = obj.Spec.PV.CSIDriver
		}
		if obj.Spec.PV.AccessModes != nil && len(obj.Spec.PV.AccessModes) > 0 {
			pvAccess = []corev1.PersistentVolumeAccessMode{}
			for _, m := range obj.Spec.PV.AccessModes {
				switch strings.ToLower(m) {
				case "readwritemany":
					pvAccess = append(pvAccess, corev1.ReadWriteMany)
				case "readwriteonce":
					pvAccess = append(pvAccess, corev1.ReadWriteOnce)
				case "readonlymany":
					pvAccess = append(pvAccess, corev1.ReadOnlyMany)
				}
			}
			if len(pvAccess) == 0 {
				pvAccess = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			}
		}
		if obj.Spec.PV.VolumeAttributes != nil {
			for k, v := range obj.Spec.PV.VolumeAttributes {
				volAttrs[k] = v
			}
		}
		if strings.TrimSpace(obj.Spec.PV.CSIMounter) != "" {
			volAttrs["mounter"] = obj.Spec.PV.CSIMounter
		}
		if strings.TrimSpace(obj.Spec.PV.CSIOptions) != "" {
			volAttrs["options"] = obj.Spec.PV.CSIOptions
		}
	}
	pvCap := resource.MustParse(pvCapStr)
	fsMode := corev1.PersistentVolumeFilesystem
	pv := &corev1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{
			Name:   pvName,
			Labels: labels,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: pvCap},
			AccessModes:                   pvAccess,
			PersistentVolumeReclaimPolicy: pvReclaim,
			StorageClassName:              pvClass,
			VolumeMode:                    &fsMode,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       csiDriver,
					VolumeHandle: volHandle,
					VolumeAttributes: func() map[string]string {
						m := map[string]string{"bucket": s3Bucket, "prefix": s3Prefix, "capacity": pvCap.String()}
						for k, v := range volAttrs {
							m[k] = v
						}
						return m
					}(),
				},
			},
		},
	}
	// Default CSI secret refs: from Model.Spec.PV if set, otherwise csi-s3-secret/kube-system
	secName := "csi-s3-secret"
	secNs := "kube-system"
	if obj.Spec.PV != nil {
		if strings.TrimSpace(obj.Spec.PV.CSISecretName) != "" {
			secName = obj.Spec.PV.CSISecretName
		}
		if strings.TrimSpace(obj.Spec.PV.CSISecretNamespace) != "" {
			secNs = obj.Spec.PV.CSISecretNamespace
		}
	}
	pv.Spec.PersistentVolumeSource.CSI.ControllerPublishSecretRef = &corev1.SecretReference{Name: secName, Namespace: secNs}
	pv.Spec.PersistentVolumeSource.CSI.NodePublishSecretRef = &corev1.SecretReference{Name: secName, Namespace: secNs}
	pv.Spec.PersistentVolumeSource.CSI.NodeStageSecretRef = &corev1.SecretReference{Name: secName, Namespace: secNs}
	// Pre-bind PV to PVC for static provisioning
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: obj.Namespace, Name: pvcName}
	if err := h.apply.WithSetOwnerReference(false, false).WithSetID(fmt.Sprintf("kold-model-s3-pv-%s", obj.Name)).ApplyObjects(pv); err != nil {
		return fmt.Errorf("failed to apply PV: %w", err)
	}
	reqCapStr := pvCapStr
	if obj.Spec.PV != nil && strings.TrimSpace(obj.Spec.PV.PVCCapacity) != "" {
		reqCapStr = obj.Spec.PV.PVCCapacity
	}
	reqCap := resource.MustParse(reqCapStr)
	emptyClass := ""
	if obj.Spec.PV != nil && strings.TrimSpace(obj.Spec.PV.PVCStorageClassName) != "" {
		emptyClass = obj.Spec.PV.PVCStorageClassName
	}
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: obj.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: func() []corev1.PersistentVolumeAccessMode {
				if obj.Spec.PV != nil && len(obj.Spec.PV.PVCAccessModes) > 0 {
					out := []corev1.PersistentVolumeAccessMode{}
					for _, m := range obj.Spec.PV.PVCAccessModes {
						switch strings.ToLower(m) {
						case "readwritemany":
							out = append(out, corev1.ReadWriteMany)
						case "readwriteonce":
							out = append(out, corev1.ReadWriteOnce)
						case "readonlymany":
							out = append(out, corev1.ReadOnlyMany)
						}
					}
					if len(out) > 0 {
						return out
					}
				}
				return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			}(),
			VolumeName:       pvName,
			StorageClassName: &emptyClass,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: reqCap}},
		},
	}
	if err := h.apply.WithOwner(obj).WithSetOwnerReference(true, false).WithDefaultNamespace(obj.Namespace).WithSetID(fmt.Sprintf("kold-model-s3-pvc-%s", obj.Name)).ApplyObjects(pvc); err != nil {
		return fmt.Errorf("failed to apply PVC: %w", err)
	}

	outputPVCName := ""
	outputMountPath := "/mnt/s3-output"
	convPrefix := strings.TrimLeft(convKey, "/")
	if convBucket != "" {
		outputPVName := fmt.Sprintf("%s-s3-output-pv", obj.Name)
		outputPVCName = fmt.Sprintf("%s-s3-output-pvc", obj.Name)
		outputVolHandle := convBucket
		if convPrefix != "" {
			outputVolHandle = fmt.Sprintf("%s/%s", convBucket, convPrefix)
		}
		outputPV := &corev1.PersistentVolume{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
			ObjectMeta: metav1.ObjectMeta{
				Name:   outputPVName,
				Labels: labels,
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: pvCap},
				AccessModes:                   pvAccess,
				PersistentVolumeReclaimPolicy: pvReclaim,
				StorageClassName:              pvClass,
				VolumeMode:                    &fsMode,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       csiDriver,
						VolumeHandle: outputVolHandle,
						VolumeAttributes: func() map[string]string {
							attrs := map[string]string{
								"bucket":   convBucket,
								"capacity": pvCap.String(),
							}
							if convPrefix != "" {
								attrs["prefix"] = convPrefix
							}
							for k, v := range volAttrs {
								if k == "bucket" || k == "prefix" {
									continue
								}
								attrs[k] = v
							}
							return attrs
						}(),
					},
				},
			},
		}
		outputPV.Spec.PersistentVolumeSource.CSI.ControllerPublishSecretRef = &corev1.SecretReference{Name: secName, Namespace: secNs}
		outputPV.Spec.PersistentVolumeSource.CSI.NodePublishSecretRef = &corev1.SecretReference{Name: secName, Namespace: secNs}
		outputPV.Spec.PersistentVolumeSource.CSI.NodeStageSecretRef = &corev1.SecretReference{Name: secName, Namespace: secNs}
		outputPV.Spec.ClaimRef = &corev1.ObjectReference{Namespace: obj.Namespace, Name: outputPVCName}
		if err := h.apply.WithSetOwnerReference(false, false).WithSetID(fmt.Sprintf("kold-model-s3-output-pv-%s", obj.Name)).ApplyObjects(outputPV); err != nil {
			return fmt.Errorf("failed to apply output PV: %w", err)
		}

		outputPVC := &corev1.PersistentVolumeClaim{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      outputPVCName,
				Namespace: obj.Namespace,
				Labels:    labels,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: func() []corev1.PersistentVolumeAccessMode {
					if obj.Spec.PV != nil && len(obj.Spec.PV.PVCAccessModes) > 0 {
						out := []corev1.PersistentVolumeAccessMode{}
						for _, m := range obj.Spec.PV.PVCAccessModes {
							switch strings.ToLower(m) {
							case "readwritemany":
								out = append(out, corev1.ReadWriteMany)
							case "readwriteonce":
								out = append(out, corev1.ReadWriteOnce)
							case "readonlymany":
								out = append(out, corev1.ReadOnlyMany)
							}
						}
						if len(out) > 0 {
							return out
						}
					}
					return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
				}(),
				VolumeName:       outputPVName,
				StorageClassName: &emptyClass,
				Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: reqCap}},
			},
		}
		if err := h.apply.WithOwner(obj).WithSetOwnerReference(true, false).WithDefaultNamespace(obj.Namespace).WithSetID(fmt.Sprintf("kold-model-s3-output-pvc-%s", obj.Name)).ApplyObjects(outputPVC); err != nil {
			return fmt.Errorf("failed to apply output PVC: %w", err)
		}

	}

	volumes := []corev1.Volume{
		{
			Name: "workspace",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: "s3",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
			},
		},
	}

	if outputPVCName != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "s3-output",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: outputPVCName},
			},
		})
	}

	workspaceMount := corev1.VolumeMount{Name: "workspace", MountPath: "/workspace"}
	mountsForWorkdir := []corev1.VolumeMount{workspaceMount}

	// Init container: fetch converter scripts from GitHub
	converterVersion := spec.ConverterVersion
	writerURL := spec.WriterURL
	if writerURL == "" {
		writerURL = defaultWriterURL
	}

	fetchLines := []string{
		"set -euo pipefail",
		"apk add --no-cache wget curl",
		"mkdir -p /workspace/converter",
		"cd /workspace/converter",
		fmt.Sprintf("wget -q -O convert-hf.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/convert-hf.py", converterVersion),
	}
	if spec.WriterConfigMap == "" {
		fetchLines = append(fetchLines, fmt.Sprintf("wget -q -O writer.py %s", writerURL))
	}
	fetchLines = append(fetchLines,
		fmt.Sprintf("wget -q -O convert-tokenizer-hf.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/convert-tokenizer-hf.py", converterVersion),
		fmt.Sprintf("wget -q -O tokenizer-writer.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/tokenizer-writer.py", converterVersion),
	)
	fetchCmd := strings.Join(fetchLines, "\n")

	fetchScripts := corev1.Container{
		Name:         "fetch-converter",
		Image:        toolsImage,
		Command:      []string{"/bin/sh", "-c"},
		Args:         []string{fetchCmd},
		VolumeMounts: mountsForWorkdir,
	}

	// No mount propagation required for PVC

	// Use S3 mounted path as working directory for reading inputs
	s3WorkDir := filepath.Join("/mnt/s3", inputKey)
	mainContainer := h.buildConversionContainer(obj, spec, s3WorkDir, inputKey, convBucket, convKey, convURI, weightsType, expectedGeneration)
	// mount S3 PVC into main with read-only access
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{Name: "s3", MountPath: "/mnt/s3"})
	if spec.WriterConfigMap != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "writer-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: spec.WriterConfigMap},
				},
			},
		})
		mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{
			Name:      "writer-config",
			MountPath: "/workspace/converter/writer.py",
			SubPath:   "writer.py",
		})
	}
	if outputPVCName != "" {
		mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{Name: "s3-output", MountPath: "/mnt/s3-output"})
		mainContainer.Env = append(mainContainer.Env,
			corev1.EnvVar{Name: "CONVERSION_OUTPUT_PATH", Value: outputMountPath},
			corev1.EnvVar{Name: "CONVERSION_OUTPUT_PREFIX", Value: convPrefix},
		)
	}
	mainContainer.SecurityContext = &corev1.SecurityContext{}

	// Optionally warm the workspace dir to ensure it exists
	initContainers := []corev1.Container{fetchScripts}

	// no rclone sidecar when using S3 CSI PVC

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: batchv1.SchemeGroupVersion.String(),
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   obj.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:  corev1.RestartPolicyNever,
					InitContainers: initContainers,
					Containers:     []corev1.Container{mainContainer},
					Volumes:        volumes,
				},
			},
		},
	}

	klog.V(1).Infof("Model %s/%s: creating conversion job %s", obj.Namespace, obj.Name, jobName)
	return h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("koldun-model-convert-%s", obj.Name)).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(job)
}

func (h *modelHandler) ensureSizingJob(obj *v1.Model) error {
	if obj.Spec.Conversion == nil {
		return nil
	}
	if !strings.EqualFold(obj.Status.ConversionState, "Succeeded") || obj.Status.ObservedGeneration != obj.Generation {
		return nil
	}

	outputPVC := strings.TrimSpace(obj.Status.OutputPVCName)
	if outputPVC == "" {
		return nil
	}

	spec := effectiveConversionSpec(obj.Spec.Conversion)
	toolsImage := spec.ToolsImage
	if toolsImage == "" {
		toolsImage = defaultToolsImage
	}

	jobName := sizeJobName(obj)
	expectedGeneration := fmt.Sprintf("%d", obj.Generation)
	forceTokenRaw, forceRequested := obj.Annotations[annotationForceSizeRerun]
	forceToken := normalizeForceToken(forceTokenRaw, forceRequested, obj.ResourceVersion)
	processedToken := strings.TrimSpace(obj.Status.ConversionSizeForceToken)

	alreadySucceeded := obj.Status.ConversionSizeGeneration == obj.Generation &&
		strings.EqualFold(obj.Status.ConversionSizeState, "Succeeded") &&
		obj.Status.ConversionSizeJobName == jobName
	if alreadySucceeded && (!forceRequested || forceTokenMatches(forceToken, processedToken, obj.ResourceVersion)) {
		return nil
	}

	if existing, err := h.jobs.Cache().Get(obj.Namespace, jobName); err == nil && existing != nil {
		if existing.DeletionTimestamp != nil {
			return nil
		}

		currentGen := existing.Annotations[annotationModelGeneration]
		if currentGen == "" {
			currentGen = existing.Labels[annotationModelGeneration]
		}
		jobForceToken := strings.TrimSpace(existing.Annotations[annotationForceSizeRerun])
		finished := isJobFinished(existing)

		if forceToken != "" && forceToken != jobForceToken {
			propagation := metav1.DeletePropagationBackground
			if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
				return fmt.Errorf("failed to delete sizing job for force rerun: %w", err)
			}
			return nil
		}

		if currentGen == expectedGeneration {
			if !finished {
				return nil
			}

			state := strings.ToLower(obj.Status.ConversionSizeState)
			switch state {
			case "succeeded":
				if obj.Status.ConversionSizeGeneration == obj.Generation && (forceToken == "" || forceTokenMatches(forceToken, processedToken, obj.ResourceVersion)) {
					propagation := metav1.DeletePropagationBackground
					if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
						return fmt.Errorf("failed to delete completed sizing job: %w", err)
					}
				}
			case "failed":
				propagation := metav1.DeletePropagationBackground
				if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
					return fmt.Errorf("failed to delete failed sizing job: %w", err)
				}
			case "pending":
				// Job finished but status has not yet picked up the termination message; leave the job
				// in place so ensureStatus can read the measurement without thrashing the workload.
			default:
				// For other states, keep the job so Kubernetes can report updated status.
			}
			return nil
		}

		if finished {
			propagation := metav1.DeletePropagationBackground
			if err := h.jobs.Delete(obj.Namespace, jobName, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
				return fmt.Errorf("failed to delete outdated sizing job: %w", err)
			}
		}
		return nil
	}

	labels := map[string]string{
		labelComponent: componentModel,
		labelModelName: obj.Name,
	}
	if dllama := labelValue(obj.Labels, labelDllamaName); dllama != "" {
		labels[labelDllamaName] = dllama
	}

	annotations := map[string]string{annotationModelGeneration: expectedGeneration}
	if forceToken != "" {
		annotations[annotationForceSizeRerun] = forceToken
	}

	scriptLines := []string{
		"set -euo pipefail",
		"TARGET=${TARGET_DIR:-/mnt/output}",
		"if [ ! -d \"${TARGET}\" ]; then",
		"  printf '{\"bytes\":0,\"human\":\"0\"}\n' > /dev/termination-log",
		"  echo 'Converted artifacts size: 0 (directory missing)'",
		"  exit 0",
		"fi",
		"BYTES=$(du -sb \"${TARGET}\" | awk '{print $1}')",
		"HUMAN=$(du -sh \"${TARGET}\" | awk '{print $1}')",
		"printf '{\"bytes\":%s,\"human\":\"%s\"}\n' \"${BYTES}\" \"${HUMAN}\" > /dev/termination-log",
		"echo \"Converted artifacts size: ${HUMAN} (${BYTES} bytes)\"",
	}

	command := []string{"/bin/sh", "-c"}
	args := []string{strings.Join(scriptLines, "\n")}

	volumeName := "output"
	volume := corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: outputPVC, ReadOnly: true},
		},
	}

	container := corev1.Container{
		Name:                     "measure-size",
		Image:                    toolsImage,
		Command:                  command,
		Args:                     args,
		Env:                      []corev1.EnvVar{{Name: "TARGET_DIR", Value: "/mnt/output"}},
		VolumeMounts:             []corev1.VolumeMount{{Name: volumeName, MountPath: "/mnt/output", ReadOnly: true}},
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}

	backoffLimit := int32(1)

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: batchv1.SchemeGroupVersion.String(),
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   obj.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{container},
					Volumes:       []corev1.Volume{volume},
				},
			},
		},
	}

	klog.V(1).Infof("Model %s/%s: creating sizing job %s", obj.Namespace, obj.Name, jobName)
	return h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("koldun-model-size-%s", obj.Name)).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(job)
}

func jobNameForModel(model *v1.Model) string {
	return truncateName(model.Name+jobSuffixDownload, 63)
}

func conversionJobName(model *v1.Model) string {
	return truncateName(model.Name+jobSuffixConvert, 63)
}

func sizeJobName(model *v1.Model) string {
	return truncateName(model.Name+jobSuffixSize, 63)
}

func summarizeJob(job *batchv1.Job, condType string) (string, metav1.Condition) {
	cond := metav1.Condition{
		Type:    condType,
		Status:  metav1.ConditionFalse,
		Reason:  "JobPending",
		Message: "Job is pending",
	}
	state := "Pending"

	successMessage := "Job completed successfully"
	switch condType {
	case conditionDownloaded:
		successMessage = "Model download completed"
	case conditionConverted:
		successMessage = "Conversion job completed"
	case conditionSized:
		successMessage = "Sizing job completed"
	}

	for _, jc := range job.Status.Conditions {
		if jc.Type == batchv1.JobFailed && jc.Status == corev1.ConditionTrue {
			cond.Status = metav1.ConditionFalse
			cond.Reason = "JobFailed"
			cond.Message = jc.Message
			state = "Failed"
			return state, cond
		}
		if jc.Type == batchv1.JobComplete && jc.Status == corev1.ConditionTrue {
			cond.Status = metav1.ConditionTrue
			cond.Reason = "JobSucceeded"
			cond.Message = successMessage
			state = "Succeeded"
			return state, cond
		}
	}

	if job.Status.Failed > 0 {
		cond.Reason = "JobFailed"
		cond.Message = "Job failed"
		state = "Failed"
		return state, cond
	}
	if job.Status.Succeeded > 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "JobSucceeded"
		cond.Message = successMessage
		state = "Succeeded"
		return state, cond
	}
	if job.Status.Active > 0 {
		cond.Reason = "JobRunning"
		cond.Message = "Job is running"
		state = "Running"
		return state, cond
	}

	return state, cond
}

type sizeMeasurement struct {
	Bytes int64  `json:"bytes"`
	Human string `json:"human"`
}

func modelNameFromJob(jobName string) string {
	if strings.HasSuffix(jobName, jobSuffixDownload) {
		name := strings.TrimSuffix(jobName, jobSuffixDownload)
		if name != jobName {
			return name
		}
	}
	if strings.HasSuffix(jobName, jobSuffixConvert) {
		name := strings.TrimSuffix(jobName, jobSuffixConvert)
		if name != jobName {
			return name
		}
	}
	return ""
}

func isJobFinished(job *batchv1.Job) bool {
	klog.V(1).Infof("Job %s/%s: checking if finished - Conditions=%d, Succeeded=%d, Failed=%d, Active=%d, DeletionTimestamp=%v",
		job.Namespace, job.Name, len(job.Status.Conditions), job.Status.Succeeded, job.Status.Failed, job.Status.Active, job.DeletionTimestamp)

	// Check job conditions first
	for _, cond := range job.Status.Conditions {
		if cond.Status == corev1.ConditionTrue {
			if cond.Type == batchv1.JobComplete || cond.Type == batchv1.JobFailed {
				klog.V(1).Infof("Job %s/%s: finished with condition %s", job.Namespace, job.Name, cond.Type)
				return true
			}
		}
	}

	// Check if job has succeeded or failed based on pod counts
	if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
		klog.V(1).Infof("Job %s/%s: finished with Succeeded=%d, Failed=%d", job.Namespace, job.Name, job.Status.Succeeded, job.Status.Failed)
		return true
	}

	// Additional check: if job is marked for deletion, consider it finished
	if job.DeletionTimestamp != nil {
		klog.V(1).Infof("Job %s/%s: marked for deletion", job.Namespace, job.Name)
		return true
	}

	klog.V(1).Infof("Job %s/%s: not finished", job.Namespace, job.Name)
	return false
}
