package controllers

import (
	"context"
	"fmt"

    v1 "github.com/gorizond/kold/pkg/apis/kold.gorizond.io/v1"
    "github.com/rancher/wrangler/v3/pkg/apply"
    "github.com/rancher/wrangler/v3/pkg/generic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

type rootHandler struct {
	ctx         context.Context
	apply       apply.Apply
	roots       generic.ControllerInterface[*v1.Root, *v1.RootList]
	deployments generic.ControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList]
	services    generic.ControllerInterface[*corev1.Service, *corev1.ServiceList]
}

func registerRootController(ctx context.Context, m *Manager) error {
	handler := &rootHandler{
		ctx:         ctx,
		apply:       m.Apply(ctx),
		roots:       m.Kold.Root(),
		deployments: m.Apps.Deployment(),
		services:    m.Core.Service(),
	}

	handler.roots.OnChange(ctx, "kold-root-controller", handler.onChange)
	handler.roots.OnRemove(ctx, "kold-root-controller", handler.onRemove)

	handler.deployments.OnChange(ctx, "kold-root-deployment-watch", handler.onRelatedDeployment)
	handler.services.OnChange(ctx, "kold-root-service-watch", handler.onRelatedService)
	return nil
}

func (h *rootHandler) onChange(key string, obj *v1.Root) (*v1.Root, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureDeployment(obj); err != nil {
		return obj, err
	}
	if err := h.ensureService(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *rootHandler) onRemove(key string, obj *v1.Root) (*v1.Root, error) {
	return obj, nil
}

func (h *rootHandler) onRelatedDeployment(key string, obj *appsv1.Deployment) (*appsv1.Deployment, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.Labels[labelComponent] != componentRoot {
		return obj, nil
	}
	rootName := obj.Labels[labelRootName]
	if rootName == "" {
		return obj, nil
	}
	h.roots.Enqueue(obj.Namespace, rootName)
	return obj, nil
}

func (h *rootHandler) onRelatedService(key string, obj *corev1.Service) (*corev1.Service, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.Labels[labelComponent] != componentRoot {
		return obj, nil
	}
	rootName := obj.Labels[labelRootName]
	if rootName == "" {
		return obj, nil
	}
	h.roots.Enqueue(obj.Namespace, rootName)
	return obj, nil
}

func (h *rootHandler) ensureDeployment(root *v1.Root) error {
	dep := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      root.Name,
			Namespace: root.Namespace,
			Labels: map[string]string{
				labelComponent: componentRoot,
				labelRootName:  root.Name,
			},
		},
	}

	labels := map[string]string{
		labelComponent: componentRoot,
		labelRootName:  root.Name,
	}
	if dllamaName := labelValue(root.Labels, labelDllamaName); dllamaName != "" {
		dep.ObjectMeta.Labels[labelDllamaName] = dllamaName
		labels[labelDllamaName] = dllamaName
	}

	dep.Spec = appsv1.DeploymentSpec{
		Replicas: pointer.Int32(1),
		Selector: &metav1.LabelSelector{MatchLabels: labels},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					h.rootContainer(root),
				},
			},
		},
	}

	return h.apply.WithOwner(root).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(root.Namespace).
		ApplyObjects(dep)
}

func (h *rootHandler) ensureService(root *v1.Root) error {
	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      root.Name,
			Namespace: root.Namespace,
			Labels: map[string]string{
				labelComponent: componentRoot,
				labelRootName:  root.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				labelComponent: componentRoot,
				labelRootName:  root.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       8080,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
	}

	if dllamaName := labelValue(root.Labels, labelDllamaName); dllamaName != "" {
		svc.ObjectMeta.Labels[labelDllamaName] = dllamaName
		svc.Spec.Selector[labelDllamaName] = dllamaName
	}

	return h.apply.WithOwner(root).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(root.Namespace).
		ApplyObjects(svc)
}

func (h *rootHandler) rootContainer(root *v1.Root) corev1.Container {
	args := []string{"--role", "root"}
	if root.Spec.ModelRef != "" {
		args = append(args, "--model", root.Spec.ModelRef)
	}
	if len(root.Spec.Args) > 0 {
		args = append(args, root.Spec.Args...)
	}

	env := []corev1.EnvVar{
		{Name: "DLLAMA_ROLE", Value: "root"},
	}

	if root.Spec.CacheSpec != nil {
		env = append(env,
			corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: root.Spec.CacheSpec.Endpoint},
			corev1.EnvVar{Name: "CACHE_BUCKET", Value: root.Spec.CacheSpec.Bucket},
		)
		if root.Spec.CacheSpec.SecretRef != nil {
			env = append(env, corev1.EnvVar{
				Name: "CACHE_SECRET",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: root.Spec.CacheSpec.SecretRef.Name},
						Key:                  "credentials",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

	return corev1.Container{
		Name:  "root",
		Image: root.Spec.Image,
		Args:  args,
		Env:   env,
		Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
	}
}

func (h *rootHandler) ensureStatus(root *v1.Root) (*v1.Root, error) {
	updated := root.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	ready := metav1.Condition{
		Type:    conditionReady,
		Reason:  "DeploymentNotReady",
		Message: "Root deployment is not yet ready",
		Status:  metav1.ConditionFalse,
	}

	if dep, err := h.deployments.Cache().Get(root.Namespace, root.Name); err == nil {
		if dep.Status.ReadyReplicas >= 1 {
			ready.Status = metav1.ConditionTrue
			ready.Reason = "DeploymentReady"
			ready.Message = "Root deployment is ready"
		}
	}

	if svc, err := h.services.Cache().Get(root.Namespace, root.Name); err == nil {
		if len(svc.Spec.Ports) > 0 {
			port := svc.Spec.Ports[0].Port
			updated.Status.Endpoint = fmt.Sprintf("%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, port)
		}
	}

	changed := setCondition(&updated.Status.Conditions, ready)
	if updated.Status.ObservedGeneration != updated.Generation {
		updated.Status.ObservedGeneration = updated.Generation
		changed = true
	}

	if !changed && updated.Status.Endpoint == root.Status.Endpoint {
		return root, nil
	}

	return h.roots.UpdateStatus(updated)
}
