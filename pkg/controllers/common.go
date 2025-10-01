package controllers

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	labelDllamaName       = "koldun.gorizond.io/dllama"
	labelComponent        = "koldun.gorizond.io/component"
	labelRootName         = "koldun.gorizond.io/root"
	labelWorkerName       = "koldun.gorizond.io/worker"
	labelModelName        = "koldun.gorizond.io/model"
	labelConversationHash = "koldun.gorizond.io/hash"
	labelSessionName      = "koldun.gorizond.io/session"
	labelBackendName      = "koldun.gorizond.io/backend"

	componentModel      = "model"
	componentRoot       = "root"
	componentWorker     = "worker"
	componentBackend    = "backend"
	componentDispatcher = "dispatcher"

	annotationSlotKey                  = "koldun.gorizond.io/slot"
	annotationSessionQueuePrefix       = "koldun.gorizond.io/session-dllama-prefix"
	annotationSessionAssignmentsBucket = "koldun.gorizond.io/session-assignments-bucket"
	annotationSessionBacklogSubject    = "koldun.gorizond.io/session-backlog-subject"
	annotationSessionStateStream       = "koldun.gorizond.io/session-state-stream"

	conditionReady      = "Ready"
	conditionDownloaded = "Downloaded"
	conditionConverted  = "Converted"
	conditionSized      = "Sized"

	annotationModelGeneration = "koldun.gorizond.io/model-generation"
)

func setCondition(conditions *[]metav1.Condition, cond metav1.Condition) bool {
	if conditions == nil {
		return false
	}

	now := metav1.Now()
	for i := range *conditions {
		existing := &(*conditions)[i]
		if existing.Type == cond.Type {
			if existing.Status == cond.Status && existing.Reason == cond.Reason && existing.Message == cond.Message {
				return false
			}
			if cond.LastTransitionTime.IsZero() {
				cond.LastTransitionTime = now
			}
			*existing = cond
			return true
		}
	}

	if cond.LastTransitionTime.IsZero() {
		cond.LastTransitionTime = now
	}
	*conditions = append(*conditions, cond)
	return true
}

func isConditionTrue(conditions []metav1.Condition, condType string) bool {
	for _, condition := range conditions {
		if condition.Type == condType && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func labelValue(labels map[string]string, key string) string {
	if labels == nil {
		return ""
	}
	return labels[key]
}

func truncateName(base string, limit int) string {
	if len(base) <= limit {
		return base
	}
	if limit <= 0 {
		return ""
	}
	return base[:limit]
}

func sanitizeLabelValue(value string) string {
	trimmed := value
	if trimmed == "" {
		return trimmed
	}
	if len(trimmed) > validation.LabelValueMaxLength {
		trimmed = trimmed[:validation.LabelValueMaxLength]
	}
	return trimmed
}

func workerResourceName(dllamaName string) string {
	const maxLength = validation.LabelValueMaxLength - 11 // leave room for controller revision suffix
	base := fmt.Sprintf("%s-workers", dllamaName)
	if len(base) > maxLength {
		base = base[:maxLength]
	}
	return base
}

func dllamaNameForSession(sessionName string, ordinal int32) string {
	suffix := fmt.Sprintf("-%d", ordinal)
	base := fmt.Sprintf("%s-dllama", sessionName)
	max := validation.LabelValueMaxLength - len(suffix)
	if max < 1 {
		max = 1
	}
	if len(base) > max {
		base = base[:max]
	}
	return base + suffix
}
