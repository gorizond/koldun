package controllers

import (
	"context"
	"fmt"
	"strconv"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

type workerHandler struct {
	ctx         context.Context
	apply       apply.Apply
	workers     generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
	deployments generic.ControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList]
}

func registerWorkerController(ctx context.Context, m *Manager) error {
	handler := &workerHandler{
		ctx:         ctx,
		apply:       m.Apply(ctx),
		workers:     m.Kold.Worker(),
		deployments: m.Apps.Deployment(),
	}

	handler.workers.OnChange(ctx, "koldun-worker-controller", handler.onChange)
	handler.workers.OnRemove(ctx, "koldun-worker-controller", handler.onRemove)
	handler.deployments.OnChange(ctx, "koldun-worker-deployment-watch", handler.onRelatedDeployment)
	return nil
}

func (h *workerHandler) onChange(key string, obj *v1.Worker) (*v1.Worker, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureDeployment(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *workerHandler) onRemove(key string, obj *v1.Worker) (*v1.Worker, error) {
	return obj, nil
}

func (h *workerHandler) onRelatedDeployment(key string, obj *appsv1.Deployment) (*appsv1.Deployment, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.Labels[labelComponent] != componentWorker {
		return obj, nil
	}
	workerName := obj.Labels[labelWorkerName]
	if workerName == "" {
		return obj, nil
	}
	h.workers.Enqueue(obj.Namespace, workerName)
	return obj, nil
}

func (h *workerHandler) ensureDeployment(worker *v1.Worker) error {
	name := worker.Name
	labels := map[string]string{
		labelComponent:  componentWorker,
		labelWorkerName: worker.Name,
	}
	if dllama := labelValue(worker.Labels, labelDllamaName); dllama != "" {
		labels[labelDllamaName] = dllama
	}

	dep := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   worker.Namespace,
			Labels:      labels,
			Annotations: map[string]string{annotationSlotKey: strconv.Itoa(int(worker.Spec.Slot))},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(1),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: map[string]string{annotationSlotKey: strconv.Itoa(int(worker.Spec.Slot))},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{h.workerContainer(worker)},
				},
			},
		},
	}

	return h.apply.WithOwner(worker).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(worker.Namespace).
		ApplyObjects(dep)
}

func (h *workerHandler) workerContainer(worker *v1.Worker) corev1.Container {
	args := []string{"--role", "worker", "--slot", fmt.Sprintf("%d", worker.Spec.Slot)}
	if worker.Spec.RootRef != "" {
		args = append(args, "--root", worker.Spec.RootRef)
	}
	if worker.Spec.ModelRef != "" {
		args = append(args, "--model", worker.Spec.ModelRef)
	}
	if len(worker.Spec.Args) > 0 {
		args = append(args, worker.Spec.Args...)
	}

	env := []corev1.EnvVar{
		{Name: "DLLAMA_ROLE", Value: "worker"},
		{Name: "DLLAMA_SLOT", Value: fmt.Sprintf("%d", worker.Spec.Slot)},
	}

	if worker.Spec.CacheSpec != nil {
		env = append(env,
			corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: worker.Spec.CacheSpec.Endpoint},
			corev1.EnvVar{Name: "CACHE_BUCKET", Value: worker.Spec.CacheSpec.Bucket},
		)
		if worker.Spec.CacheSpec.SecretRef != nil {
			env = append(env, corev1.EnvVar{
				Name: "CACHE_SECRET",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: worker.Spec.CacheSpec.SecretRef.Name},
						Key:                  "credentials",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

	return corev1.Container{
		Name:  "worker",
		Image: worker.Spec.Image,
		Args:  args,
		Env:   env,
	}
}

func (h *workerHandler) ensureStatus(worker *v1.Worker) (*v1.Worker, error) {
	updated := worker.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	condition := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "DeploymentNotReady",
		Message: "Worker deployment is not yet ready",
	}

	if dep, err := h.deployments.Cache().Get(worker.Namespace, worker.Name); err == nil {
		if dep.Status.ReadyReplicas >= 1 {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "DeploymentReady"
			condition.Message = "Worker deployment is ready"
		}
	}

	changed := setCondition(&updated.Status.Conditions, condition)
	if updated.Status.ObservedGeneration != updated.Generation {
		updated.Status.ObservedGeneration = updated.Generation
		changed = true
	}

	if !changed {
		return worker, nil
	}

	return h.workers.UpdateStatus(updated)
}
