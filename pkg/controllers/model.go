package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	corectlv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
)

const (
	// Default images & parameters
	defaultDownloadImage   = "python:3.11-alpine"
	defaultConversionImage = "python:3.11-alpine"
	defaultToolsImage      = "alpine:3.18"
	defaultWeightsType     = "q40"

	jobSuffixDownload = "-download"
	jobSuffixConvert  = "-convert"
	jobSuffixSize     = "-size"

	// Annotation to track when a job was last deleted to prevent immediate recreation
	annotationJobDeletedAt = "koldun.gorizond.io/job-deleted-at"
	// annotationForceSizeRerun allows users to request re-running the sizing job
	annotationForceSizeRerun = "koldun.gorizond.io/force-size-rerun"
)

type modelHandler struct {
	ctx    context.Context
	apply  apply.Apply
	models generic.ControllerInterface[*v1.Model, *v1.ModelList]
	jobs   generic.ControllerInterface[*batchv1.Job, *batchv1.JobList]
	pvcs   corectlv1.PersistentVolumeClaimController
	pvs    corectlv1.PersistentVolumeController
	pods   corectlv1.PodController
}

func registerModelController(ctx context.Context, m *Manager) error {
	handler := &modelHandler{
		ctx:    ctx,
		apply:  m.Apply(ctx),
		models: m.Kold.Model(),
		jobs:   m.Batch.Job(),
		pvcs:   m.Core.PersistentVolumeClaim(),
		pvs:    m.Core.PersistentVolume(),
		pods:   m.Core.Pod(),
	}

	handler.models.OnChange(ctx, "koldun-model-controller", handler.onChange)
	handler.models.OnRemove(ctx, "koldun-model-controller", handler.onRemove)
	handler.jobs.OnChange(ctx, "koldun-model-job-watch", handler.onRelatedJob)
	handler.jobs.OnRemove(ctx, "koldun-model-job-remove", handler.onRelatedJob)
	return nil
}

func (h *modelHandler) onChange(key string, obj *v1.Model) (*v1.Model, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	klog.V(1).Infof("Model %s/%s: onChange triggered for generation %d", obj.Namespace, obj.Name, obj.Generation)

	if err := h.ensureMetadataConfigMap(obj); err != nil {
		return obj, err
	}
	if err := h.ensureScriptConfigMap(obj); err != nil {
		return obj, err
	}
	if err := h.ensureDownloadJob(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure download job: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}

	// Ensure conversion job after download logic. This will noop until download succeeds.
	if err := h.ensureConversionJob(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure conversion job: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}
	if err := h.ensureSizingJob(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure sizing job: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *modelHandler) onRemove(key string, obj *v1.Model) (*v1.Model, error) {
	if obj == nil {
		return nil, nil
	}
	// Delete PVC then PV created for this model (input and conversion buckets)
	pvNames := []string{
		fmt.Sprintf("%s-s3-pv", obj.Name),
		fmt.Sprintf("%s-s3-output-pv", obj.Name),
	}
	pvcNames := []string{
		fmt.Sprintf("%s-s3-pvc", obj.Name),
		fmt.Sprintf("%s-s3-output-pvc", obj.Name),
	}
	// Best-effort deletion; ignore not found
	for _, pvcName := range pvcNames {
		if err := h.pvcs.Delete(obj.Namespace, pvcName, &metav1.DeleteOptions{PropagationPolicy: func() *metav1.DeletionPropagation { p := metav1.DeletePropagationBackground; return &p }()}); err != nil {
			klog.V(2).Infof("delete PVC %s/%s: %v", obj.Namespace, pvcName, err)
		}
	}
	for _, pvName := range pvNames {
		if err := h.pvs.Delete(pvName, &metav1.DeleteOptions{PropagationPolicy: func() *metav1.DeletionPropagation { p := metav1.DeletePropagationBackground; return &p }()}); err != nil {
			klog.V(2).Infof("delete PV %s: %v", pvName, err)
		}
	}
	return obj, nil
}

func (h *modelHandler) onRelatedJob(key string, job *batchv1.Job) (*batchv1.Job, error) {
	if job == nil {
		// Job was deleted, only enqueue if it was finished
		namespace, jobName := splitKey(key)
		if namespace == "" || jobName == "" {
			return nil, nil
		}
		modelName := strings.TrimSuffix(jobName, "-download")
		if modelName == "" || modelName == jobName {
			return nil, nil
		}
		klog.V(3).Infof("Model %s/%s: job %s was deleted, enqueueing for status update", namespace, modelName, jobName)
		h.models.Enqueue(namespace, modelName)
		return nil, nil
	}

	if job.Labels[labelComponent] != componentModel {
		return job, nil
	}

	modelName := job.Labels[labelModelName]
	if modelName == "" {
		return job, nil
	}

	// Only enqueue model if job status has meaningfully changed
	// This prevents unnecessary recreations during job startup
	if h.shouldEnqueueModelForJob(job) {
		klog.V(3).Infof("Model %s/%s: job %s status changed, enqueueing for status update", job.Namespace, modelName, job.Name)
		h.models.Enqueue(job.Namespace, modelName)
	}

	return job, nil
}

// shouldEnqueueModelForJob determines if a job status change should trigger model reconciliation
func (h *modelHandler) shouldEnqueueModelForJob(job *batchv1.Job) bool {
	klog.V(1).Infof("Job %s/%s: checking if should enqueue model - Status.Active=%d, Status.Succeeded=%d, Status.Failed=%d, StartTime=%v",
		job.Namespace, job.Name, job.Status.Active, job.Status.Succeeded, job.Status.Failed, job.Status.StartTime)

	// Always enqueue for finished jobs (success or failure)
	if isJobFinished(job) {
		klog.V(1).Infof("Job %s/%s: finished, will enqueue model", job.Namespace, job.Name)
		return true
	}

	// Only enqueue for running jobs if they have been running for a while
	// This prevents immediate recreation during job startup
	if job.Status.Active > 0 {
		// Check if job has been running for more than 120 seconds (increased from 60)
		if job.Status.StartTime != nil {
			now := metav1.Now()
			duration := now.Time.Sub(job.Status.StartTime.Time)
			klog.V(1).Infof("Job %s/%s: running for %v seconds", job.Namespace, job.Name, duration.Seconds())
			if duration.Seconds() > 120 {
				klog.V(1).Infof("Job %s/%s: running for more than 120 seconds, will enqueue model", job.Namespace, job.Name)
				return true
			}
		}
		// If no start time, be conservative and don't enqueue immediately
		klog.V(1).Infof("Job %s/%s: no start time or running less than 120 seconds, will NOT enqueue model", job.Namespace, job.Name)
		return false
	}

	// For other states (pending, etc.), never enqueue
	// This prevents excessive reconciliation during job startup phase
	klog.V(1).Infof("Job %s/%s: not active, will NOT enqueue model", job.Namespace, job.Name)
	return false
}

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

func (h *modelHandler) ensureDownloadJob(obj *v1.Model) error {
	storage := obj.Spec.ObjectStorage
	if storage == nil || strings.TrimSpace(storage.BucketForSource) == "" || obj.Spec.SourceURL == "" {
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

	// Additional check: if we already have a job with this name and it's not failed, don't recreate
	klog.V(1).Infof("Model %s/%s: checking Status.DownloadJobName=%s, Status.DownloadState=%s",
		obj.Namespace, obj.Name, obj.Status.DownloadJobName, obj.Status.DownloadState)

	if obj.Status.DownloadJobName == jobName && obj.Status.DownloadJobName != "" {
		if !strings.EqualFold(obj.Status.DownloadState, "Failed") {
			// Job exists and is not failed, no need to recreate
			klog.V(1).Infof("Model %s/%s: job %s already exists in status and is not failed, skipping recreation", obj.Namespace, obj.Name, jobName)
			return nil
		}
	}

	// Final safety check: ensure we don't create a job if one already exists and is running
	if existing, err := h.jobs.Cache().Get(obj.Namespace, jobName); err == nil && existing != nil {
		klog.V(1).Infof("Model %s/%s: final safety check - found existing job %s", obj.Namespace, obj.Name, jobName)
		if existing.DeletionTimestamp == nil && !isJobFinished(existing) {
			// Job exists, is not being deleted, and is not finished - don't create another
			klog.V(1).Infof("Model %s/%s: job %s already exists and is not finished, skipping creation", obj.Namespace, obj.Name, jobName)
			return nil
		}
		// If job exists but is finished, we should have handled it above
		if !isJobFinished(existing) {
			klog.V(1).Infof("Model %s/%s: job %s exists but is not finished, skipping creation", obj.Namespace, obj.Name, jobName)
			return nil
		}
	}

	// Additional check: if we recently created a job, don't create another one immediately
	// This prevents rapid recreation cycles
	klog.V(1).Infof("Model %s/%s: checking recent job creation conditions", obj.Namespace, obj.Name)
	if obj.Status.DownloadJobName == jobName {
		// Check if we have a condition indicating recent job creation
		for _, cond := range obj.Status.Conditions {
			if cond.Type == conditionDownloaded && cond.Status == metav1.ConditionFalse {
				if cond.Reason == "JobCreated" || cond.Reason == "JobPending" {
					// Check if the condition was created recently (within last 60 seconds)
					if cond.LastTransitionTime.Time.Add(60 * time.Second).After(time.Now()) {
						klog.V(1).Infof("Model %s/%s: job %s was recently created (%v ago), skipping immediate recreation",
							obj.Namespace, obj.Name, jobName, time.Since(cond.LastTransitionTime.Time))
						return nil
					}
				}
			}
		}
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
	downloadContainer := h.buildDownloadContainer(obj, spec, obj.Spec.SourceURL, objectKey, expectedGeneration)
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
	workDir, convBucket, convKey, convURI := conversionPaths(obj, spec, inputKey)
	weightsType := spec.WeightsFloatType
	if weightsType == "" {
		weightsType = defaultWeightsType
	}

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

		// When using a dedicated output PVC, stage work directly on that mount
		workDir = outputMountPath
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
	// If workDir is outside of /workspace, also mount the same volume at workDir (only needed when not using dedicated output PVC)
	useAdditionalMount := outputPVCName == "" && !strings.HasPrefix(workDir, "/workspace")
	additionalMount := corev1.VolumeMount{Name: "workspace", MountPath: workDir}
	mountsForWorkdir := []corev1.VolumeMount{workspaceMount}
	if useAdditionalMount {
		mountsForWorkdir = append(mountsForWorkdir, additionalMount)
	}

	// Init container: fetch converter scripts from GitHub
	converterVersion := spec.ConverterVersion
	fetchCmd := strings.Join([]string{
		"set -euo pipefail",
		"apk add --no-cache wget curl",
		"mkdir -p /workspace/converter",
		"cd /workspace/converter",
		fmt.Sprintf("wget -q -O convert-hf.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/convert-hf.py", converterVersion),
		fmt.Sprintf("wget -q -O writer.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/writer.py", converterVersion),
		fmt.Sprintf("wget -q -O convert-tokenizer-hf.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/convert-tokenizer-hf.py", converterVersion),
		fmt.Sprintf("wget -q -O tokenizer-writer.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/%s/converter/tokenizer-writer.py", converterVersion),
	}, "\n")

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
	if useAdditionalMount {
		mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, additionalMount)
	}
	// mount S3 PVC into main with read-only access
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{Name: "s3", MountPath: "/mnt/s3"})
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
	forceToken := strings.TrimSpace(obj.Annotations[annotationForceSizeRerun])
	processedToken := strings.TrimSpace(obj.Status.ConversionSizeForceToken)

	alreadySucceeded := obj.Status.ConversionSizeGeneration == obj.Generation &&
		strings.EqualFold(obj.Status.ConversionSizeState, "Succeeded") &&
		obj.Status.ConversionSizeJobName == jobName
	if alreadySucceeded && (forceToken == "" || forceToken == processedToken) {
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
				if obj.Status.ConversionSizeGeneration == obj.Generation && (forceToken == "" || forceToken == processedToken) {
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
		"BYTES=$(du -sk \"${TARGET}\" | awk '{printf \"%d\", $1 * 1024}')",
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

func (h *modelHandler) buildDownloadContainer(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey, generation string) corev1.Container {
	storage := model.Spec.ObjectStorage
	sourceBucket := ""
	cacheEndpoint := ""
	if storage != nil {
		sourceBucket = storage.BucketForSource
		cacheEndpoint = storage.Endpoint
	}

	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: sourceBucket},
		{Name: "CACHE_OBJECT_KEY", Value: objectKey},
		{Name: "MODEL_GENERATION", Value: generation},
	}
	if strings.TrimSpace(cacheEndpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: cacheEndpoint})
	}
	env = append(env, corev1.EnvVar{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"})

	// Determine memory limit from spec (fallback to 128Mi) and expose to container
	memoryQuantity := resource.MustParse("128Mi")
	if spec.Memory != "" {
		if q, err := resource.ParseQuantity(spec.Memory); err == nil {
			memoryQuantity = q
		} else {
			klog.Warningf("Model %s/%s: invalid download.memory '%s', falling back to 128Mi: %v", model.Namespace, model.Name, spec.Memory, err)
		}
	}
	env = append(env,
		corev1.EnvVar{Name: "MEMORY_LIMIT", Value: memoryQuantity.String()},
		corev1.EnvVar{Name: "MEMORY_LIMIT_BYTES", Value: strconv.FormatInt(memoryQuantity.Value(), 10)},
		corev1.EnvVar{Name: "CHUNK_MAX_MIB", Value: strconv.Itoa(int(spec.ChunkMaxMiB))},
		corev1.EnvVar{Name: "CONCURRENCY", Value: strconv.Itoa(int(spec.Concurrency))},
	)

	if spec.HuggingFaceTokenSecretRef != nil {
		if spec.HuggingFaceTokenSecretRef.Namespace != "" && spec.HuggingFaceTokenSecretRef.Namespace != model.Namespace {
			// cross-namespace secret mounts are not permitted; skip with warning via env comment
		} else {
			env = append(env, corev1.EnvVar{
				Name: "HF_TOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: spec.HuggingFaceTokenSecretRef.Name},
						Key:                  "token",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

	// Pass SOURCE_URL and optional PIP proxy to container
	env = append(env, corev1.EnvVar{Name: "SOURCE_URL", Value: sourceURL})
	// Pass PIP_PROXY if set in spec
	if strings.TrimSpace(model.Spec.PipProxy) != "" {
		env = append(env, corev1.EnvVar{Name: "PIP_PROXY", Value: model.Spec.PipProxy})
	}

	if storage != nil && storage.SecretRef != nil {
		if storage.SecretRef.Namespace == "" || storage.SecretRef.Namespace == model.Namespace {
			envFrom := corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: storage.SecretRef.Name},
					Optional:             pointer.Bool(true),
				},
			}
			return corev1.Container{
				Name:    "model-downloader",
				Image:   spec.Image,
				Command: h.downloadCommand(spec),
				Args:    h.downloadArgs(model, spec, sourceURL, objectKey, generation),
				Env:     env,
				EnvFrom: []corev1.EnvFromSource{envFrom},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: memoryQuantity,
					},
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: memoryQuantity,
					},
				},
				ImagePullPolicy: corev1.PullIfNotPresent,
			}
		}
	}

	return corev1.Container{
		Name:    "model-downloader",
		Image:   spec.Image,
		Command: h.downloadCommand(spec),
		Args:    h.downloadArgs(model, spec, sourceURL, objectKey, generation),
		Env:     env,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
		},
		ImagePullPolicy: corev1.PullIfNotPresent,
	}
}

func (h *modelHandler) downloadCommand(spec *v1.ModelDownloadSpec) []string {
	if len(spec.Command) > 0 {
		return spec.Command
	}
	// run a shell to install deps and execute a small Python + boto3 upload workflow
	return []string{"/bin/sh", "-c"}
}

func (h *modelHandler) downloadArgs(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey, generation string) []string {
	if len(spec.Args) > 0 {
		return spec.Args
	}

	// Construct a script that:
	// 1) installs huggingface_hub and boto3,
	// 3) snapshots HF repo to /tmp/model, 4) uploads files directly to S3 via boto3
	_ = generation

	lines := []string{
		"set -euo pipefail",
		// Create pip.conf if PIP_PROXY set
		"if [ -n \"${PIP_PROXY:-}\" ]; then mkdir -p ~/.pip; cat > ~/.pip/pip.conf <<CONF\n[global]\nproxy = ${PIP_PROXY}\nindex-url = https://pypi.org/simple/\n\n[install]\ndefault-timeout = 500\ntrusted-host = pypi.python.org\n               pypi.org\n               files.pythonhosted.org\nCONF\nfi",
		"python -m pip install --no-cache-dir huggingface_hub boto3 botocore requests",
		"python -u /opt/script/download.py",
	}
	return []string{strings.Join(lines, "\n")}
}

func (h *modelHandler) buildConversionContainer(model *v1.Model, spec *v1.ModelConversionSpec, workDir, inputKey, outputBucket, outputKey, outputURI, weightsType, generation string) corev1.Container {
	storage := model.Spec.ObjectStorage
	sourceBucket := ""
	cacheEndpoint := ""
	if storage != nil {
		sourceBucket = storage.BucketForSource
		cacheEndpoint = storage.Endpoint
	}

	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: sourceBucket},
		{Name: "CACHE_OBJECT_KEY", Value: inputKey},
		{Name: "CONVERSION_BUCKET", Value: outputBucket},
		{Name: "CONVERSION_OBJECT_KEY", Value: outputKey},
		{Name: "CONVERSION_OUTPUT_URI", Value: outputURI},
		{Name: "CONVERSION_WEIGHTS_TYPE", Value: weightsType},
		{Name: "MODEL_GENERATION", Value: generation},
		{Name: "CONVERSION_WORK_DIR", Value: workDir},
		{Name: "PYTHONPATH", Value: "/workspace/converter"},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
	}
	if strings.TrimSpace(cacheEndpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: cacheEndpoint})
	}
	if strings.TrimSpace(model.Spec.PipProxy) != "" {
		env = append(env, corev1.EnvVar{Name: "PIP_PROXY", Value: model.Spec.PipProxy})
	}

	memoryQuantity := resource.MustParse("2Gi")
	if spec.Memory != "" {
		if q, err := resource.ParseQuantity(spec.Memory); err == nil {
			memoryQuantity = q
		} else {
			klog.Warningf("Model %s/%s: invalid conversion.memory '%s', falling back to 2Gi: %v", model.Namespace, model.Name, spec.Memory, err)
		}
	}

	image := spec.Image
	if image == "" {
		image = defaultConversionImage
	}

	container := corev1.Container{
		Name:            "model-converter",
		Image:           image,
		Command:         spec.Command,
		Args:            spec.Args,
		WorkingDir:      "/mnt/s3-output",
		Env:             env,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: memoryQuantity,
			},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
	}

	if len(container.Command) == 0 {
		container.Command = []string{"/bin/sh", "-c"}
	}
	if len(container.Args) == 0 {
		container.Args = h.conversionArgs(model, spec, model.Spec.SourceURL, inputKey, outputKey, weightsType)
	}

	if storage != nil && storage.SecretRef != nil {
		if storage.SecretRef.Namespace == "" || storage.SecretRef.Namespace == model.Namespace {
			container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: storage.SecretRef.Name},
					Optional:             pointer.Bool(true),
				},
			})
		}
	}

	return container
}

func (h *modelHandler) conversionArgs(model *v1.Model, spec *v1.ModelConversionSpec, sourceURL, inputKey, outputKey, weightsType string) []string {
	if len(spec.Args) > 0 {
		return spec.Args
	}

	_ = sourceURL
	_ = inputKey
	_ = outputKey

	cmdLines := []string{
		"set -euo pipefail",
		// create pip.conf if proxy provided (PIP_PROXY comes from spec.pipProxy via envFrom later if needed)
		"if [ -n \"${PIP_PROXY:-}\" ]; then mkdir -p ~/.pip; cat > ~/.pip/pip.conf <<CONF\n[global]\nproxy = ${PIP_PROXY}\nindex-url = https://pypi.org/simple/\n\n[install]\ndefault-timeout = 500\ntrusted-host = pypi.python.org\n               pypi.org\n               files.pythonhosted.org\nCONF\nfi",

		"pip install --no-cache-dir torch safetensors sentencepiece transformers datasets huggingface_hub boto3 requests gitpython",

		"python -u /workspace/converter/convert-hf.py /mnt/s3 ${CONVERSION_WEIGHTS_TYPE} ${MODEL_NAME}",
		"python -u /workspace/converter/convert-tokenizer-hf.py /mnt/s3 ${MODEL_NAME}",
	}

	return []string{strings.Join(cmdLines, "\n")}
}

func (h *modelHandler) ensureStatus(obj *v1.Model) (*v1.Model, error) {
	updated := obj.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	downloadCond := metav1.Condition{
		Type:    conditionDownloaded,
		Status:  metav1.ConditionFalse,
		Reason:  "JobNotCreated",
		Message: "Download job has not been created",
	}
	conversionCond := metav1.Condition{
		Type:    conditionConverted,
		Status:  metav1.ConditionFalse,
		Reason:  "ConversionNotRequested",
		Message: "Model spec.conversion is not configured",
	}
	sizeCond := metav1.Condition{
		Type:    conditionSized,
		Status:  metav1.ConditionFalse,
		Reason:  "SizingNotRequested",
		Message: "Model conversion sizing is not configured",
	}
	readyCond := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "DownloadPending",
		Message: "Model download is pending",
	}

	downloadState := "Pending"
	conversionState := "NotRequested"
	sizeState := "NotRequested"

	downloadJobName := jobNameForModel(obj)
	conversionJobName := conversionJobName(obj)
	sizeJobName := sizeJobName(obj)

	updated.Status.DownloadJobName = ""
	updated.Status.ConversionJobName = ""
	updated.Status.ConversionSizeJobName = ""
	updated.Status.ConversionSizeState = ""
	updated.Status.ConversionSizeBytes = 0
	updated.Status.ConversionSizeHuman = ""
	updated.Status.ConversionSizeGeneration = 0
	updated.Status.ConversionSizeForceToken = ""
	updated.Status.OutputPVCName = ""

	forceToken := strings.TrimSpace(obj.Annotations[annotationForceSizeRerun])

	storage := obj.Spec.ObjectStorage
	if storage == nil || strings.TrimSpace(storage.BucketForSource) == "" || obj.Spec.SourceURL == "" {
		downloadCond.Reason = "ConfigurationMissing"
		downloadCond.Message = "Model requires sourceUrl and objectStorage.bucketForSource"
	} else {
		if job, err := h.jobs.Cache().Get(obj.Namespace, downloadJobName); err == nil && job != nil {
			updated.Status.DownloadJobName = job.Name
			downloadState, downloadCond = summarizeJob(job, conditionDownloaded)

			if downloadState == "Pending" && job.Status.Active == 0 && job.Status.Succeeded == 0 && job.Status.Failed == 0 {
				downloadCond.Reason = "JobCreated"
				downloadCond.Message = "Download job has been created and is pending execution"
			}
		} else {
			// Preserve last known terminal state when the Job no longer exists (e.g. TTL expired)
			prevState := strings.ToLower(obj.Status.DownloadState)
			if obj.Status.ObservedGeneration == obj.Generation && prevState == strings.ToLower("Succeeded") {
				downloadCond.Status = metav1.ConditionTrue
				downloadCond.Reason = "JobSucceeded"
				downloadCond.Message = "Model download completed"
				downloadState = "Succeeded"
			} else if obj.Status.ObservedGeneration == obj.Generation && prevState == strings.ToLower("Failed") {
				downloadCond.Status = metav1.ConditionFalse
				downloadCond.Reason = "JobFailed"
				downloadCond.Message = "Model download job failed"
				downloadState = "Failed"
			} else {
				downloadCond.Reason = "JobPending"
				downloadCond.Message = "Waiting for download job to appear"
			}
		}
	}

	updated.Status.DownloadState = downloadState

	if obj.Spec.Conversion != nil {
		conversionState = "Pending"
		conversionCond.Reason = "WaitingForDownload"
		conversionCond.Message = "Conversion waits until download completes"

		downloadSucceeded := strings.EqualFold(downloadState, "Succeeded") && obj.Status.ObservedGeneration == obj.Generation
		if downloadSucceeded {
			if job, err := h.jobs.Cache().Get(obj.Namespace, conversionJobName); err == nil && job != nil {
				updated.Status.ConversionJobName = job.Name
				conversionState, conversionCond = summarizeJob(job, conditionConverted)
			} else {
				prevState := strings.ToLower(obj.Status.ConversionState)
				if obj.Status.ObservedGeneration == obj.Generation && prevState == strings.ToLower("Succeeded") {
					conversionCond.Status = metav1.ConditionTrue
					conversionCond.Reason = "JobSucceeded"
					conversionCond.Message = "Conversion job completed"
					conversionState = "Succeeded"
				} else if obj.Status.ObservedGeneration == obj.Generation && prevState == strings.ToLower("Failed") {
					conversionCond.Status = metav1.ConditionFalse
					conversionCond.Reason = "JobFailed"
					conversionCond.Message = "Conversion job failed"
					conversionState = "Failed"
				} else {
					conversionCond.Reason = "JobPending"
					conversionCond.Message = "Waiting for conversion job to appear"
				}
			}
		} else {
			conversionCond.Status = metav1.ConditionFalse
			conversionCond.Reason = "WaitingForDownload"
			conversionCond.Message = "Conversion waits until download completes"
		}
	} else {
		conversionCond.Status = metav1.ConditionFalse
		conversionCond.Reason = "ConversionNotRequested"
		conversionCond.Message = "Model spec.conversion is not configured"
	}

	updated.Status.ConversionState = conversionState
	if obj.Spec.Conversion != nil && strings.EqualFold(conversionState, "Succeeded") {
		updated.Status.OutputPVCName = fmt.Sprintf("%s-s3-output-pvc", obj.Name)
	}

	if obj.Spec.Conversion != nil {
		if strings.EqualFold(conversionState, "Succeeded") {
			sizeState = "Pending"
			sizeCond.Reason = "SizingPending"
			sizeCond.Message = "Sizing job pending"

			if job, err := h.jobs.Cache().Get(obj.Namespace, sizeJobName); err == nil && job != nil {
				updated.Status.ConversionSizeJobName = job.Name
				sizeState, sizeCond = summarizeJob(job, conditionSized)

				if strings.EqualFold(sizeState, "Succeeded") {
					measurement, err := h.collectSizeMeasurement(obj.Namespace, job.Name)
					if err != nil {
						sizeCond.Status = metav1.ConditionFalse
						sizeCond.Reason = "ResultCollectionFailed"
						sizeCond.Message = fmt.Sprintf("Failed to read sizing result: %v", err)
						sizeState = "Failed"
					} else if measurement == nil {
						sizeCond.Status = metav1.ConditionFalse
						sizeCond.Reason = "ResultPending"
						sizeCond.Message = "Sizing job completed; waiting for termination message"
						sizeState = "Pending"
					} else {
						sizeCond.Status = metav1.ConditionTrue
						sizeCond.Reason = "SizingSucceeded"
						sizeCond.Message = fmt.Sprintf("Converted artifacts size: %s", measurement.Human)
						updated.Status.ConversionSizeBytes = measurement.Bytes
						updated.Status.ConversionSizeHuman = measurement.Human
						updated.Status.ConversionSizeGeneration = obj.Generation
						updated.Status.ConversionSizeForceToken = forceToken
					}
				}
			} else {
				prevGenMatch := obj.Status.ConversionSizeGeneration == obj.Generation
				switch strings.ToLower(obj.Status.ConversionSizeState) {
				case "succeeded":
					if prevGenMatch {
						sizeCond.Status = metav1.ConditionTrue
						sizeCond.Reason = "SizingSucceeded"
						sizeCond.Message = fmt.Sprintf("Converted artifacts size: %s", obj.Status.ConversionSizeHuman)
						sizeState = "Succeeded"
						updated.Status.ConversionSizeBytes = obj.Status.ConversionSizeBytes
						updated.Status.ConversionSizeHuman = obj.Status.ConversionSizeHuman
						updated.Status.ConversionSizeGeneration = obj.Status.ConversionSizeGeneration
						updated.Status.ConversionSizeJobName = obj.Status.ConversionSizeJobName
						updated.Status.ConversionSizeForceToken = obj.Status.ConversionSizeForceToken
					} else {
						sizeCond.Reason = "SizingPending"
						sizeCond.Message = "Waiting for sizing job to appear"
						sizeState = "Pending"
					}
				case "failed":
					if prevGenMatch {
						sizeCond.Status = metav1.ConditionFalse
						sizeCond.Reason = "SizingFailed"
						sizeCond.Message = "Sizing job failed"
						sizeState = "Failed"
						updated.Status.ConversionSizeForceToken = obj.Status.ConversionSizeForceToken
					} else {
						sizeCond.Reason = "SizingPending"
						sizeCond.Message = "Waiting for sizing job to appear"
						sizeState = "Pending"
					}
				default:
					sizeCond.Reason = "SizingPending"
					sizeCond.Message = "Waiting for sizing job to appear"
					sizeState = "Pending"
				}
			}
		} else if strings.EqualFold(conversionState, "Failed") {
			sizeCond.Status = metav1.ConditionFalse
			sizeCond.Reason = "ConversionNotSucceeded"
			sizeCond.Message = "Conversion must succeed before sizing"
			sizeState = "NotRequested"
		} else {
			sizeCond.Status = metav1.ConditionFalse
			sizeCond.Reason = "WaitingForConversion"
			sizeCond.Message = "Sizing waits until conversion succeeds"
			sizeState = "Pending"
		}
	} else {
		sizeCond.Status = metav1.ConditionFalse
		sizeCond.Reason = "SizingNotRequested"
		sizeCond.Message = "Model conversion sizing is not configured"
	}

	updated.Status.ConversionSizeState = sizeState

	if obj.Spec.Conversion != nil {
		if downloadCond.Status == metav1.ConditionTrue && conversionCond.Status == metav1.ConditionTrue {
			readyCond.Status = metav1.ConditionTrue
			readyCond.Reason = "ConversionReady"
			readyCond.Message = "Converted artifacts available in cache"
		} else if downloadCond.Status != metav1.ConditionTrue {
			readyCond.Status = metav1.ConditionFalse
			readyCond.Reason = downloadCond.Reason
			readyCond.Message = downloadCond.Message
		} else {
			readyCond.Status = metav1.ConditionFalse
			readyCond.Reason = conversionCond.Reason
			readyCond.Message = conversionCond.Message
		}
	} else {
		if downloadCond.Status == metav1.ConditionTrue {
			readyCond.Status = metav1.ConditionTrue
			readyCond.Reason = "ArtifactsReady"
			readyCond.Message = "Model artifacts available in cache"
		} else {
			readyCond.Status = metav1.ConditionFalse
			readyCond.Reason = downloadCond.Reason
			readyCond.Message = downloadCond.Message
		}
	}

	changed := false
	if setCondition(&updated.Status.Conditions, downloadCond) {
		changed = true
	}
	if obj.Spec.Conversion != nil || hasCondition(updated.Status.Conditions, conditionConverted) {
		if setCondition(&updated.Status.Conditions, conversionCond) {
			changed = true
		}
	}
	if obj.Spec.Conversion != nil || hasCondition(updated.Status.Conditions, conditionSized) {
		if setCondition(&updated.Status.Conditions, sizeCond) {
			changed = true
		}
	}
	if setCondition(&updated.Status.Conditions, readyCond) {
		changed = true
	}

	if updated.Status.ObservedGeneration != updated.Generation {
		updated.Status.ObservedGeneration = updated.Generation
		changed = true
	}
	if obj.Status.DownloadJobName != updated.Status.DownloadJobName || obj.Status.DownloadState != updated.Status.DownloadState {
		changed = true
	}
	if obj.Status.ConversionJobName != updated.Status.ConversionJobName || obj.Status.ConversionState != updated.Status.ConversionState {
		changed = true
	}
	if obj.Status.ConversionSizeJobName != updated.Status.ConversionSizeJobName ||
		obj.Status.ConversionSizeState != updated.Status.ConversionSizeState ||
		obj.Status.ConversionSizeBytes != updated.Status.ConversionSizeBytes ||
		obj.Status.ConversionSizeHuman != updated.Status.ConversionSizeHuman ||
		obj.Status.ConversionSizeGeneration != updated.Status.ConversionSizeGeneration ||
		obj.Status.ConversionSizeForceToken != updated.Status.ConversionSizeForceToken {
		changed = true
	}
	if obj.Status.OutputPVCName != updated.Status.OutputPVCName {
		changed = true
	}

	if obj.Annotations != nil && updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	if obj.Annotations != nil {
		for k, v := range obj.Annotations {
			if updated.Annotations[k] != v {
				if updated.Annotations == nil {
					updated.Annotations = make(map[string]string)
				}
				updated.Annotations[k] = v
				changed = true
			}
		}
	}

	if !changed {
		return obj, nil
	}

	return h.models.UpdateStatus(updated)
}

func effectiveDownloadSpec(spec *v1.ModelDownloadSpec) *v1.ModelDownloadSpec {
	if spec == nil {
		return &v1.ModelDownloadSpec{
			Image:       defaultDownloadImage,
			Memory:      "128Mi",
			ChunkMaxMiB: 64,
			Concurrency: 1,
		}
	}
	out := spec.DeepCopy()
	if out.Image == "" {
		out.Image = defaultDownloadImage
	}
	if out.Memory == "" {
		out.Memory = "128Mi"
	}
	if out.ChunkMaxMiB <= 0 {
		out.ChunkMaxMiB = 64
	}
	if out.Concurrency <= 0 {
		out.Concurrency = 1
	}
	return out
}

func effectiveConversionSpec(spec *v1.ModelConversionSpec) *v1.ModelConversionSpec {
	if spec == nil {
		return &v1.ModelConversionSpec{
			Image:            defaultConversionImage,
			WeightsFloatType: defaultWeightsType,
			Memory:           "2Gi",
		}
	}
	out := spec.DeepCopy()
	if out.Image == "" {
		out.Image = defaultConversionImage
	}
	if out.WeightsFloatType == "" {
		out.WeightsFloatType = defaultWeightsType
	}
	if out.Memory == "" {
		out.Memory = "2Gi"
	}
	if out.ConverterVersion == "" {
		out.ConverterVersion = "v0.16.2"
	}
	return out
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

func (h *modelHandler) collectSizeMeasurement(namespace, jobName string) (*sizeMeasurement, error) {
	selector := labels.SelectorFromSet(map[string]string{"job-name": jobName})
	pods, err := h.pods.Cache().List(namespace, selector)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Terminated != nil && strings.TrimSpace(status.State.Terminated.Message) != "" {
				return parseSizeMeasurement(status.State.Terminated.Message)
			}
		}
	}
	return nil, nil
}

func parseSizeMeasurement(payload string) (*sizeMeasurement, error) {
	var result sizeMeasurement
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func modelObjectKey(model *v1.Model) string {
	pathValue := strings.TrimSpace(model.Spec.LocalPath)
	if pathValue == "" {
		return ""
	}
	if _, key, ok := parseS3Path(pathValue); ok {
		return strings.TrimLeft(key, "/")
	}
	return strings.TrimLeft(pathValue, "/")
}

func conversionPaths(model *v1.Model, spec *v1.ModelConversionSpec, defaultInputKey string) (workDir string, bucket string, key string, uri string) {
	workDir = "/workspace/hf"
	storage := model.Spec.ObjectStorage
	if storage != nil {
		bucket = strings.TrimSpace(storage.BucketForConvert)
		if bucket == "" {
			bucket = storage.BucketForSource
		}
	}
	baseKey := strings.TrimLeft(path.Join(defaultInputKey, "converted", spec.WeightsFloatType), "/")
	if spec.WeightsFloatType == "" {
		baseKey = strings.TrimLeft(path.Join(defaultInputKey, "converted", defaultWeightsType), "/")
	}

	if strings.TrimSpace(spec.OutputPath) != "" {
		if b, k, ok := parseS3Path(spec.OutputPath); ok {
			if b != "" {
				bucket = b
			}
			if k != "" {
				baseKey = strings.TrimLeft(k, "/")
			} else {
				baseKey = ""
			}
		} else {
			clean := spec.OutputPath
			if !strings.HasPrefix(clean, "/") {
				clean = filepath.Join("/workspace", clean)
			}
			workDir = filepath.Clean(clean)
		}
	}

	key = strings.TrimLeft(baseKey, "/")
	if key != "" {
		key = strings.TrimLeft(path.Join(key, model.Name), "/")
	} else {
		key = strings.TrimLeft(path.Join(defaultInputKey, "converted", model.Name), "/")
	}

	trimmedKey := strings.TrimLeft(key, "/")
	if bucket != "" && trimmedKey != "" {
		uri = fmt.Sprintf("s3://%s/%s", bucket, trimmedKey)
	} else if bucket != "" {
		uri = fmt.Sprintf("s3://%s", bucket)
	}
	return workDir, bucket, trimmedKey, uri
}

func splitKey(key string) (string, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
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

func parseS3Path(value string) (bucket string, key string, ok bool) {
	if !strings.HasPrefix(value, "s3://") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(value, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return "", "", false
	}
	bucket = parts[0]
	if len(parts) > 1 {
		key = strings.TrimLeft(parts[1], "/")
	}
	return bucket, key, true
}

func hasCondition(conditions []metav1.Condition, condType string) bool {
	for _, cond := range conditions {
		if cond.Type == condType {
			return true
		}
	}
	return false
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
