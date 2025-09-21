package controllers

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	labelDllamaName   = "kold.gorizond.io/dllama"
	labelComponent    = "kold.gorizond.io/component"
	labelRootName     = "kold.gorizond.io/root"
	labelWorkerName   = "kold.gorizond.io/worker"
	componentModel    = "model"
	componentRoot     = "root"
	componentWorker   = "worker"
	annotationSlotKey = "kold.gorizond.io/slot"

	conditionReady = "Ready"
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
			cond.LastTransitionTime = now
			*existing = cond
			return true
		}
	}

	cond.LastTransitionTime = now
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
