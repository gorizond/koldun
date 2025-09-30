package controllers

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/pointer"
)

type workerHandler struct {
	ctx          context.Context
	apply        apply.Apply
	dllamas      generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	workers      generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
	statefulsets generic.ControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList]
	services     generic.ControllerInterface[*corev1.Service, *corev1.ServiceList]
}

func registerWorkerController(ctx context.Context, m *Manager) error {
	handler := &workerHandler{
		ctx:          ctx,
		apply:        m.Apply(ctx),
		dllamas:      m.Kold.Dllama(),
		workers:      m.Kold.Worker(),
		statefulsets: m.Apps.StatefulSet(),
		services:     m.Core.Service(),
	}

	handler.workers.OnChange(ctx, "koldun-worker-controller", handler.onChange)
	handler.workers.OnRemove(ctx, "koldun-worker-controller", handler.onRemove)
	handler.statefulsets.OnChange(ctx, "koldun-worker-statefulset-watch", handler.onRelatedStatefulSet)
	handler.services.OnChange(ctx, "koldun-worker-service-watch", handler.onRelatedService)
	handler.dllamas.OnChange(ctx, "koldun-worker-dllama-watch", handler.onRelatedDllama)
	handler.dllamas.OnRemove(ctx, "koldun-worker-dllama-remove", handler.onRelatedDllama)
	return nil
}

func (h *workerHandler) onChange(key string, obj *v1.Worker) (*v1.Worker, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureStatefulSet(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *workerHandler) onRemove(key string, obj *v1.Worker) (*v1.Worker, error) {
	return obj, nil
}

func (h *workerHandler) onRelatedStatefulSet(key string, obj *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
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

func (h *workerHandler) onRelatedService(key string, obj *corev1.Service) (*corev1.Service, error) {
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

func (h *workerHandler) ensureStatefulSet(worker *v1.Worker) error {
	name := worker.Name
	labels := map[string]string{
		labelComponent:  componentWorker,
		labelWorkerName: worker.Name,
	}
	if dllama := labelValue(worker.Labels, labelDllamaName); dllama != "" {
		labels[labelDllamaName] = dllama
	}
	if hash := labelValue(worker.Labels, labelConversationHash); hash != "" {
		labels[labelConversationHash] = hash
	}

	replicas, threads := h.workerReplicaConfig(worker)
	if replicas <= 0 {
		replicas = 1
	}

	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   worker.Namespace,
			Labels:      labels,
			Annotations: map[string]string{annotationSlotKey: strconv.Itoa(int(worker.Spec.Slot))},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 labels,
			Ports: []corev1.ServicePort{
				{
					Name: "tcp",
					Port: 9999,
				},
			},
		},
	}

	sts := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   worker.Namespace,
			Labels:      labels,
			Annotations: map[string]string{annotationSlotKey: strconv.Itoa(int(worker.Spec.Slot))},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    pointer.Int32(replicas),
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: map[string]string{annotationSlotKey: strconv.Itoa(int(worker.Spec.Slot))},
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: pointer.Int64(0),
					Containers:                    []corev1.Container{h.workerContainer(worker, threads)},
				},
			},
		},
	}

	return h.apply.WithOwner(worker).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(worker.Namespace).
		ApplyObjects(svc, sts)
}

func (h *workerHandler) workerContainer(worker *v1.Worker, threads int32) corev1.Container {
	args := []string{"worker", "--port", "9999", "--nthreads", fmt.Sprintf("%d", threads)}
	if len(worker.Spec.Args) > 0 {
		args = append(args, worker.Spec.Args...)
	}

	env := []corev1.EnvVar{
		{Name: "DLLAMA_ROLE", Value: "worker"},
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

	if worker.Spec.NATS != nil {
		env = append(env, corev1.EnvVar{Name: "NATS_URL", Value: worker.Spec.NATS.URL})
		if worker.Spec.NATS.CredentialsSecret != nil {
			env = append(env, corev1.EnvVar{
				Name: "NATS_CREDS",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: worker.Spec.NATS.CredentialsSecret.Name},
						Key:                  "nats.creds",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

	return corev1.Container{
		Name:            "worker",
		Image:           worker.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"dllama"},
		Args:            args,
		Env:             env,
		Ports:           []corev1.ContainerPort{{ContainerPort: 9999}},
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
		Reason:  "StatefulSetNotReady",
		Message: "Worker statefulset is not yet ready",
	}

	if sts, err := h.statefulsets.Cache().Get(worker.Namespace, worker.Name); err == nil {
		replicas := pointer.Int32Deref(sts.Spec.Replicas, 1)
		if sts.Status.ReadyReplicas >= replicas {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "StatefulSetReady"
			condition.Message = "Worker statefulset is ready"
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

func (h *workerHandler) workerReplicaConfig(worker *v1.Worker) (replicas int32, threads int32) {
	dllamaName := labelValue(worker.Labels, labelDllamaName)
	if dllamaName == "" {
		return 1, 2
	}
	dllama, err := h.dllamas.Cache().Get(worker.Namespace, dllamaName)
	if err != nil {
		return 1, 2
	}
	replicas = workersForReplicaPower(dllama.Spec.ReplicaPower)
	if replicas <= 0 {
		replicas = 1
	}
	threads = dllama.Spec.ReplicaPower * 2
	if threads <= 0 {
		threads = 2
	}
	return replicas, threads
}

func (h *workerHandler) onRelatedDllama(key string, obj *v1.Dllama) (*v1.Dllama, error) {
	if obj == nil {
		return nil, nil
	}

	workers, err := h.workers.Cache().List(obj.Namespace, labels.SelectorFromSet(map[string]string{labelDllamaName: obj.Name}))
	if err != nil {
		return obj, err
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].Name < workers[j].Name })
	for _, worker := range workers {
		h.workers.Enqueue(worker.Namespace, worker.Name)
	}
	return obj, nil
}
