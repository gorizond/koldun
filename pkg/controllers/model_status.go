package controllers

import (
	"encoding/json"
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// model_status.go
// Contains functions for managing Model status updates and condition handling.
// This includes status synchronization, measurement collection, and condition checks.

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

	// Handle pre-converted models - skip download/conversion, mark as ready immediately
	if obj.Spec.PreConverted {
		downloadCond.Status = metav1.ConditionTrue
		downloadCond.Reason = "PreConverted"
		downloadCond.Message = "Model is pre-converted, no download required"
		downloadState = "Succeeded"
		conversionCond.Status = metav1.ConditionFalse
		conversionCond.Reason = "PreConverted"
		conversionCond.Message = "Model is pre-converted, no conversion required"
		conversionState = "NotRequested"
		readyCond.Status = metav1.ConditionTrue
		readyCond.Reason = "PreConverted"
		readyCond.Message = "Model is pre-converted and ready"

		// Set fields required for registry sync
		if strings.TrimSpace(obj.Spec.PreConvertedPVCName) == "" {
			readyCond.Status = metav1.ConditionFalse
			readyCond.Reason = "ConfigurationMissing"
			readyCond.Message = "PreConverted model requires preConvertedPVCName to be specified"
			updated.Status.ObservedGeneration = obj.Generation
			setCondition(&updated.Status.Conditions, downloadCond)
			setCondition(&updated.Status.Conditions, conversionCond)
			setCondition(&updated.Status.Conditions, sizeCond)
			setCondition(&updated.Status.Conditions, readyCond)
			return h.models.UpdateStatus(updated)
		}
		updated.Status.OutputPVCName = obj.Spec.PreConvertedPVCName
		updated.Status.ConversionSizeBytes = obj.Spec.PreConvertedSizeBytes
		if strings.TrimSpace(obj.Spec.PreConvertedSizeHuman) != "" {
			updated.Status.ConversionSizeHuman = obj.Spec.PreConvertedSizeHuman
		} else if obj.Spec.PreConvertedSizeBytes > 0 {
			// Calculate human-readable size if not provided
			updated.Status.ConversionSizeHuman = humanizeBytes(obj.Spec.PreConvertedSizeBytes)
		}
		updated.Status.ConversionSizeGeneration = obj.Generation

		updated.Status.DownloadState = downloadState
		updated.Status.ConversionState = conversionState
		updated.Status.ConversionSizeState = sizeState
		updated.Status.ObservedGeneration = obj.Generation
		setCondition(&updated.Status.Conditions, downloadCond)
		setCondition(&updated.Status.Conditions, conversionCond)
		setCondition(&updated.Status.Conditions, sizeCond)
		setCondition(&updated.Status.Conditions, readyCond)

		// Update status in API server
		return h.models.UpdateStatus(updated)
	}

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

// humanizeBytes converts bytes to a human-readable string (e.g., "1.5 GB")
func humanizeBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
