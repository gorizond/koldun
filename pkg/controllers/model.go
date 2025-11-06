package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	corectlv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	// Default images & parameters
	defaultDownloadImage   = "python:3.11-alpine"
	defaultConversionImage = "python:3.11-alpine"
	defaultToolsImage      = "alpine:3.18"
	defaultWeightsType     = "q40"
	bucketEnsureTimeout    = 30 * time.Second

	jobSuffixDownload = "-download"
	jobSuffixConvert  = "-convert"
	jobSuffixSize     = "-size"

	// Annotation to track when a job was last deleted to prevent immediate recreation
	annotationJobDeletedAt = "koldun.gorizond.io/job-deleted-at"
	// annotationForceSizeRerun allows users to request re-running the sizing job
	annotationForceSizeRerun = "koldun.gorizond.io/force-size-rerun"
)

type modelHandler struct {
	ctx           context.Context
	apply         apply.Apply
	models        generic.ControllerInterface[*v1.Model, *v1.ModelList]
	jobs          generic.ControllerInterface[*batchv1.Job, *batchv1.JobList]
	pvcs          corectlv1.PersistentVolumeClaimController
	pvs           corectlv1.PersistentVolumeController
	pods          corectlv1.PodController
	secrets       corectlv1.SecretController
	ensureBuckets bool
	minioFactory  minioFactory

	ensureMetadataFn    func(*v1.Model) error
	ensureScriptFn      func(*v1.Model) error
	ensureBucketsFn     func(*v1.Model) error
	ensureDownloadJobFn func(*v1.Model) error
	ensureConversionFn  func(*v1.Model) error
	ensureSizingFn      func(*v1.Model) error
	ensureStatusFn      func(*v1.Model) (*v1.Model, error)
}

func registerModelController(ctx context.Context, m *Manager) error {
	handler := &modelHandler{
		ctx:           ctx,
		apply:         m.Apply(ctx),
		models:        m.Kold.Model(),
		jobs:          m.Batch.Job(),
		pvcs:          m.Core.PersistentVolumeClaim(),
		pvs:           m.Core.PersistentVolume(),
		pods:          m.Core.Pod(),
		secrets:       m.Core.Secret(),
		ensureBuckets: m.EnsureObjectStorageBuckets(),
		minioFactory:  defaultMinioFactory,
	}

	handler.ensureMetadataFn = handler.ensureMetadataConfigMap
	handler.ensureScriptFn = handler.ensureScriptConfigMap
	handler.ensureBucketsFn = handler.ensureObjectStorageBuckets
	handler.ensureDownloadJobFn = handler.ensureDownloadJob
	handler.ensureConversionFn = handler.ensureConversionJob
	handler.ensureSizingFn = handler.ensureSizingJob
	handler.ensureStatusFn = handler.ensureStatus

	handler.models.OnChange(ctx, "koldun-model-controller", handler.onChange)
	handler.models.OnRemove(ctx, "koldun-model-controller", handler.onRemove)
	handler.jobs.OnChange(ctx, "koldun-model-job-watch", handler.onRelatedJob)
	handler.jobs.OnRemove(ctx, "koldun-model-job-remove", handler.onRelatedJob)
	return nil
}

func (h *modelHandler) ensureMetadata(obj *v1.Model) error {
	if h.ensureMetadataFn != nil {
		return h.ensureMetadataFn(obj)
	}
	return h.ensureMetadataConfigMap(obj)
}

func (h *modelHandler) ensureScript(obj *v1.Model) error {
	if h.ensureScriptFn != nil {
		return h.ensureScriptFn(obj)
	}
	return h.ensureScriptConfigMap(obj)
}

func (h *modelHandler) ensureBucketsForModel(obj *v1.Model) error {
	if h.ensureBucketsFn != nil {
		return h.ensureBucketsFn(obj)
	}
	return h.ensureObjectStorageBuckets(obj)
}

func (h *modelHandler) ensureDownload(obj *v1.Model) error {
	if h.ensureDownloadJobFn != nil {
		return h.ensureDownloadJobFn(obj)
	}
	return h.ensureDownloadJob(obj)
}

func (h *modelHandler) ensureConversion(obj *v1.Model) error {
	if h.ensureConversionFn != nil {
		return h.ensureConversionFn(obj)
	}
	return h.ensureConversionJob(obj)
}

func (h *modelHandler) ensureSizing(obj *v1.Model) error {
	if h.ensureSizingFn != nil {
		return h.ensureSizingFn(obj)
	}
	return h.ensureSizingJob(obj)
}

func (h *modelHandler) ensureStatusUpdate(obj *v1.Model) (*v1.Model, error) {
	if h.ensureStatusFn != nil {
		return h.ensureStatusFn(obj)
	}
	return h.ensureStatus(obj)
}

func (h *modelHandler) onChange(key string, obj *v1.Model) (*v1.Model, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	klog.V(1).Infof("Model %s/%s: onChange triggered for generation %d", obj.Namespace, obj.Name, obj.Generation)

	if err := h.ensureMetadata(obj); err != nil {
		return obj, err
	}
	if err := h.ensureScript(obj); err != nil {
		return obj, err
	}
	if err := h.ensureBucketsForModel(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure object storage buckets: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}
	if err := h.ensureDownload(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure download job: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}

	// Ensure conversion job after download logic. This will noop until download succeeds.
	if err := h.ensureConversion(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure conversion job: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}
	if err := h.ensureSizing(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure sizing job: %v", obj.Namespace, obj.Name, err)
		return obj, err
	}

	return h.ensureStatusUpdate(obj)
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
