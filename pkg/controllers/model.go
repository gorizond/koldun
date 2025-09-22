package controllers

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "github.com/gorizond/kold/pkg/apis/kold.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
)

const (
	// Default images & parameters
	defaultDownloadImage   = "python:3.11-alpine"
	defaultConversionImage = "python:3.11-alpine"
	defaultToolsImage      = "alpine:3.18"
	defaultRcloneImage     = "rclone/rclone:1.67"
	defaultGoofysImage     = "ghcr.io/kahing/goofys:latest"
	defaultWeightsType     = "q40"

	jobSuffixDownload = "-download"
	jobSuffixConvert  = "-convert"

	// Annotation to track when a job was last deleted to prevent immediate recreation
	annotationJobDeletedAt = "kold.gorizond.io/job-deleted-at"
)

type modelHandler struct {
	ctx    context.Context
	apply  apply.Apply
	models generic.ControllerInterface[*v1.Model, *v1.ModelList]
	jobs   generic.ControllerInterface[*batchv1.Job, *batchv1.JobList]
}

func registerModelController(ctx context.Context, m *Manager) error {
	handler := &modelHandler{
		ctx:    ctx,
		apply:  m.Apply(ctx),
		models: m.Kold.Model(),
		jobs:   m.Batch.Job(),
	}

	handler.models.OnChange(ctx, "kold-model-controller", handler.onChange)
	handler.models.OnRemove(ctx, "kold-model-controller", handler.onRemove)
	handler.jobs.OnChange(ctx, "kold-model-job-watch", handler.onRelatedJob)
	handler.jobs.OnRemove(ctx, "kold-model-job-remove", handler.onRelatedJob)
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

	return h.ensureStatus(obj)
}

func (h *modelHandler) onRemove(key string, obj *v1.Model) (*v1.Model, error) {
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

	if obj.Spec.CacheSpec != nil {
		cm.Data["cacheEndpoint"] = obj.Spec.CacheSpec.Endpoint
		cm.Data["cacheBucket"] = obj.Spec.CacheSpec.Bucket
		if obj.Spec.CacheSpec.SecretRef != nil {
			cm.Data["cacheSecret"] = obj.Spec.CacheSpec.SecretRef.Name
		}
	}

	err := h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("kold-model-metadata-%s", obj.Name)).
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
		WithSetID(fmt.Sprintf("kold-model-script-%s", obj.Name)).
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
	if obj.Spec.CacheSpec == nil || obj.Spec.CacheSpec.Bucket == "" || obj.Spec.SourceURL == "" {
		klog.V(4).Infof("Model %s/%s: skipping download job creation - missing cache spec or source URL", obj.Namespace, obj.Name)
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
	if v, ok := obj.Annotations["kold.gorizond.io/ttl-seconds"]; ok {
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
		WithSetID(fmt.Sprintf("kold-model-job-%s", obj.Name)).
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
	if obj.Spec.CacheSpec == nil || obj.Spec.CacheSpec.Bucket == "" {
		klog.V(2).Infof("Model %s/%s: conversion requested but cacheSpec.bucket missing", obj.Namespace, obj.Name)
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
    // goofys image for S3 FUSE mount
    goofysImage := spec.GoofysImage
    if strings.TrimSpace(goofysImage) == "" {
        goofysImage = defaultGoofysImage
    }

	backoffLimit := int32(5)
	ttl := int32(300)

	// Shared volumes
	volumes := []corev1.Volume{
		{
			Name: "workspace",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: "s3mnt",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: "devfuse",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/dev/fuse",
					Type: func() *corev1.HostPathType { t := corev1.HostPathCharDev; return &t }(),
				},
			},
		},
	}

	workspaceMount := corev1.VolumeMount{Name: "workspace", MountPath: "/workspace"}
	// If workDir is outside of /workspace, also mount the same volume at workDir
	useAdditionalMount := !strings.HasPrefix(workDir, "/workspace")
	additionalMount := corev1.VolumeMount{Name: "workspace", MountPath: workDir}
	mountsForWorkdir := []corev1.VolumeMount{workspaceMount}
	if useAdditionalMount {
		mountsForWorkdir = append(mountsForWorkdir, additionalMount)
	}

	// Init container: fetch converter scripts from GitHub
	fetchCmd := strings.Join([]string{
		"set -euo pipefail",
		"apk add --no-cache wget curl",
		"mkdir -p /workspace/converter",
		"cd /workspace/converter",
		"wget -q -O convert-hf.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/main/converter/convert-hf.py",
		"wget -q -O writer.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/main/converter/writer.py",
		"wget -q -O convert-tokenizer-hf.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/main/converter/convert-tokenizer-hf.py",
		"wget -q -O tokenizer-writer.py https://raw.githubusercontent.com/b4rtaz/distributed-llama/main/converter/tokenizer-writer.py",
	}, "\n")

	fetchScripts := corev1.Container{
		Name:         "fetch-converter",
		Image:        toolsImage,
		Command:      []string{"/bin/sh", "-c"},
		Args:         []string{fetchCmd},
		VolumeMounts: mountsForWorkdir,
	}

	// Sidecar: mount S3 bucket via goofys (read-only usage by main container)
	goofysArgs := []string{"-f", "--no-implicit-dir", "--stat-cache-ttl=1h", "--type-cache-ttl=1h"}
	if ep := strings.TrimSpace(obj.Spec.CacheSpec.Endpoint); ep != "" {
		goofysArgs = append(goofysArgs, "--endpoint", ep)
	}
	goofysArgs = append(goofysArgs, obj.Spec.CacheSpec.Bucket, "/mnt/s3")

	goofysContainer := corev1.Container{
		Name:  "goofys",
		Image: goofysImage,
		Args:  goofysArgs,
		Env: []corev1.EnvVar{
			{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: pointer.Bool(true),
			Capabilities:             &corev1.Capabilities{Add: []corev1.Capability{"SYS_ADMIN"}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "s3mnt", MountPath: "/mnt/s3"},
			{Name: "devfuse", MountPath: "/dev/fuse"},
		},
	}

	if obj.Spec.CacheSpec.SecretRef != nil {
		if obj.Spec.CacheSpec.SecretRef.Namespace == "" || obj.Spec.CacheSpec.SecretRef.Namespace == obj.Namespace {
			envFrom := corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: obj.Spec.CacheSpec.SecretRef.Name},
					Optional:             pointer.Bool(true),
				},
			}
			goofysContainer.EnvFrom = append(goofysContainer.EnvFrom, envFrom)
		}
	}

	// Use S3 mounted path as working directory for reading inputs
	s3WorkDir := filepath.Join("/mnt/s3", inputKey)
	mainContainer := h.buildConversionContainer(obj, spec, s3WorkDir, inputKey, convBucket, convKey, convURI, weightsType, expectedGeneration)
	if useAdditionalMount {
		mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, additionalMount)
	}
	// mount S3 to main container as read-only
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{Name: "s3mnt", MountPath: "/mnt/s3", ReadOnly: true})

	// Optionally warm the workspace dir to ensure it exists
	initContainers := []corev1.Container{fetchScripts}

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
					Containers:     []corev1.Container{mainContainer, goofysContainer},
					Volumes:        volumes,
				},
			},
		},
	}

	klog.V(1).Infof("Model %s/%s: creating conversion job %s", obj.Namespace, obj.Name, jobName)
	return h.apply.WithOwner(obj).
		WithSetID(fmt.Sprintf("kold-model-convert-%s", obj.Name)).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(job)
}

func (h *modelHandler) buildDownloadContainer(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey, generation string) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: model.Spec.CacheSpec.Bucket},
		{Name: "CACHE_OBJECT_KEY", Value: objectKey},
		{Name: "MODEL_GENERATION", Value: generation},
	}
	if model.Spec.CacheSpec.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: model.Spec.CacheSpec.Endpoint})
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

	// Pass SOURCE_URL to container
	env = append(env, corev1.EnvVar{Name: "SOURCE_URL", Value: sourceURL})

	if model.Spec.CacheSpec.SecretRef != nil {
		if model.Spec.CacheSpec.SecretRef.Namespace == "" || model.Spec.CacheSpec.SecretRef.Namespace == model.Namespace {
			envFrom := corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: model.Spec.CacheSpec.SecretRef.Name},
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

	bucket := model.Spec.CacheSpec.Bucket
	endpoint := model.Spec.CacheSpec.Endpoint

	// Construct a script that:
	// 1) installs huggingface_hub and boto3,
	// 3) snapshots HF repo to /tmp/model, 4) uploads files directly to S3 via boto3
	_ = bucket
	_ = endpoint
	_ = generation

	cmd := "python -m pip install --no-cache-dir huggingface_hub boto3 botocore requests\n" +
		"python -u /opt/script/download.py\n"

	return []string{cmd}
}

func (h *modelHandler) buildConversionContainer(model *v1.Model, spec *v1.ModelConversionSpec, workDir, inputKey, outputBucket, outputKey, outputURI, weightsType, generation string) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: model.Spec.CacheSpec.Bucket},
		{Name: "CACHE_OBJECT_KEY", Value: inputKey},
		{Name: "CACHE_ENDPOINT", Value: model.Spec.CacheSpec.Endpoint},
		{Name: "CONVERSION_BUCKET", Value: outputBucket},
		{Name: "CONVERSION_OBJECT_KEY", Value: outputKey},
		{Name: "CONVERSION_OUTPUT_URI", Value: outputURI},
		{Name: "CONVERSION_WEIGHTS_TYPE", Value: weightsType},
		{Name: "MODEL_GENERATION", Value: generation},
		{Name: "CONVERSION_WORK_DIR", Value: workDir},
		{Name: "PYTHONPATH", Value: "/workspace/converter"},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
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

	if model.Spec.CacheSpec.SecretRef != nil {
		if model.Spec.CacheSpec.SecretRef.Namespace == "" || model.Spec.CacheSpec.SecretRef.Namespace == model.Namespace {
			container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: model.Spec.CacheSpec.SecretRef.Name},
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
		"cd /workspace",
		"pip install --no-cache-dir torch safetensors sentencepiece transformers datasets huggingface_hub boto3 requests gitpython",
		"export MODEL_DIR=${CONVERSION_WORK_DIR}",
		"export CONVERTER_DIR=/workspace/converter",
		"cd ${CONVERTER_DIR}",
		"python -u convert-hf.py ${MODEL_DIR} ${CONVERSION_WEIGHTS_TYPE} ${MODEL_NAME}",
		"python -u convert-tokenizer-hf.py ${MODEL_DIR} ${MODEL_NAME}",
		"cd /workspace",
		"python -u <<'PY'\nimport os\nimport boto3\nfrom botocore.config import Config\n\nbucket = os.environ[\"CONVERSION_BUCKET\"]\nkey_prefix = os.environ.get(\"CONVERSION_OBJECT_KEY\", \"\").strip('/')\nendpoint = os.environ.get(\"CACHE_ENDPOINT\") or None\nmodel_name = os.environ[\"MODEL_NAME\"]\nweights = os.environ[\"CONVERSION_WEIGHTS_TYPE\"]\nout_model = f'dllama_model_{model_name}_{weights}.m'\nout_tokenizer = f'dllama_tokenizer_{model_name}.t'\nclient = boto3.client(\"s3\", endpoint_url=endpoint, config=Config(s3={\"addressing_style\": \"path\"}))\n\ndef upload(local, suffix):\n    if not os.path.exists(local):\n        raise FileNotFoundError(local)\n    key = f\"{key_prefix}/{os.path.basename(local)}\" if key_prefix else os.path.basename(local)\n    print(f\"[kold] uploading {local} to s3://{bucket}/{key}\")\n    client.upload_file(local, bucket, key)\n\nupload(out_model, \"model\")\nupload(out_tokenizer, \"tokenizer\")\nPY",
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
	readyCond := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "DownloadPending",
		Message: "Model download is pending",
	}

	downloadState := "Pending"
	conversionState := "NotRequested"

	downloadJobName := jobNameForModel(obj)
	conversionJobName := conversionJobName(obj)

	updated.Status.DownloadJobName = ""
	updated.Status.ConversionJobName = ""

	if obj.Spec.CacheSpec == nil || obj.Spec.CacheSpec.Bucket == "" || obj.Spec.SourceURL == "" {
		downloadCond.Reason = "ConfigurationMissing"
		downloadCond.Message = "Model requires sourceUrl and cacheSpec.bucket"
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
	return out
}

func jobNameForModel(model *v1.Model) string {
	return truncateName(model.Name+jobSuffixDownload, 63)
}

func conversionJobName(model *v1.Model) string {
	return truncateName(model.Name+jobSuffixConvert, 63)
}

func summarizeJob(job *batchv1.Job, condType string) (string, metav1.Condition) {
	cond := metav1.Condition{
		Type:    condType,
		Status:  metav1.ConditionFalse,
		Reason:  "JobPending",
		Message: "Job is pending",
	}
	state := "Pending"

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
			cond.Message = "Model download completed"
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
		cond.Message = "Job completed successfully"
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
	bucket = model.Spec.CacheSpec.Bucket
	key = strings.TrimLeft(path.Join(defaultInputKey, "converted", spec.WeightsFloatType), "/")
	if spec.WeightsFloatType == "" {
		key = strings.TrimLeft(path.Join(defaultInputKey, "converted", defaultWeightsType), "/")
	}

	if strings.TrimSpace(spec.OutputPath) != "" {
		if b, k, ok := parseS3Path(spec.OutputPath); ok {
			if b != "" {
				bucket = b
			}
			if k != "" {
				key = strings.TrimLeft(k, "/")
			} else {
				key = ""
			}
		} else {
			clean := spec.OutputPath
			if !strings.HasPrefix(clean, "/") {
				clean = filepath.Join("/workspace", clean)
			}
			workDir = filepath.Clean(clean)
		}
	}

	if key == "" {
		key = strings.TrimLeft(path.Join(defaultInputKey, "converted"), "/")
	}

	uri = fmt.Sprintf("s3://%s/%s", bucket, strings.TrimLeft(key, "/"))
	return workDir, bucket, strings.TrimLeft(key, "/"), uri
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
