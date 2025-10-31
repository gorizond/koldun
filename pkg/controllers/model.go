package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	corectlv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	if err := h.ensureObjectStorageBuckets(obj); err != nil {
		klog.Errorf("Model %s/%s: failed to ensure object storage buckets: %v", obj.Namespace, obj.Name, err)
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

	forceTokenRaw, forceAnnotationPresent := obj.Annotations[annotationForceSizeRerun]
	forceToken := normalizeForceToken(forceTokenRaw, forceAnnotationPresent, obj.ResourceVersion)
	shouldClearForceAnnotation := false

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
						if reuseExistingSizeMeasurement(obj, updated, &sizeCond) {
							sizeState = "Succeeded"
						} else {
							sizeCond.Status = metav1.ConditionFalse
							sizeCond.Reason = "ResultCollectionFailed"
							sizeCond.Message = fmt.Sprintf("Failed to read sizing result: %v", err)
							sizeState = "Failed"
						}
					} else if measurement == nil {
						if reuseExistingSizeMeasurement(obj, updated, &sizeCond) {
							sizeState = "Succeeded"
						} else {
							sizeCond.Status = metav1.ConditionFalse
							sizeCond.Reason = "ResultPending"
							sizeCond.Message = "Sizing job completed; waiting for termination message"
							sizeState = "Pending"
						}
					} else {
						sizeCond.Status = metav1.ConditionTrue
						sizeCond.Reason = "SizingSucceeded"
						sizeCond.Message = fmt.Sprintf("Converted artifacts size: %s", measurement.Human)
						updated.Status.ConversionSizeBytes = measurement.Bytes
						updated.Status.ConversionSizeHuman = measurement.Human
						updated.Status.ConversionSizeGeneration = obj.Generation
						updated.Status.ConversionSizeForceToken = forceToken
						if forceAnnotationPresent {
							shouldClearForceAnnotation = true
						}
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

	if shouldClearForceAnnotation {
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

	result, err := h.models.UpdateStatus(updated)
	if err != nil {
		return result, err
	}

	if shouldClearForceAnnotation {
		if _, ok := result.Annotations[annotationForceSizeRerun]; ok {
			cleared := result.DeepCopy()
			delete(cleared.Annotations, annotationForceSizeRerun)
			updatedModel, updateErr := h.models.Update(cleared)
			if updateErr != nil {
				return result, updateErr
			}
			return updatedModel, nil
		}
	}

	return result, nil
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

func reuseExistingSizeMeasurement(obj *v1.Model, updated *v1.Model, cond *metav1.Condition) bool {
	if obj == nil || updated == nil || cond == nil {
		return false
	}
	if obj.Status.ConversionSizeGeneration != obj.Generation {
		return false
	}

	human := strings.TrimSpace(obj.Status.ConversionSizeHuman)
	if human == "" && obj.Status.ConversionSizeBytes == 0 {
		return false
	}
	if human == "" {
		human = fmt.Sprintf("%d bytes", obj.Status.ConversionSizeBytes)
	}

	cond.Status = metav1.ConditionTrue
	cond.Reason = "SizingSucceeded"
	cond.Message = fmt.Sprintf("Converted artifacts size: %s", human)

	updated.Status.ConversionSizeBytes = obj.Status.ConversionSizeBytes
	updated.Status.ConversionSizeHuman = obj.Status.ConversionSizeHuman
	updated.Status.ConversionSizeGeneration = obj.Status.ConversionSizeGeneration
	updated.Status.ConversionSizeForceToken = obj.Status.ConversionSizeForceToken

	return true
}

func parseSizeMeasurement(payload string) (*sizeMeasurement, error) {
	var result sizeMeasurement
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func hasCondition(conditions []metav1.Condition, condType string) bool {
	for _, cond := range conditions {
		if cond.Type == condType {
			return true
		}
	}
	return false
}
