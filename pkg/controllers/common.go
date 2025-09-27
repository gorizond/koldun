package controllers

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	labelDllamaName = "koldun.gorizond.io/dllama"
	labelComponent  = "koldun.gorizond.io/component"
	labelRootName   = "koldun.gorizond.io/root"
	labelWorkerName = "koldun.gorizond.io/worker"
	labelModelName  = "koldun.gorizond.io/model"

	componentModel  = "model"
	componentRoot   = "root"
	componentWorker = "worker"

	annotationSlotKey = "koldun.gorizond.io/slot"

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
