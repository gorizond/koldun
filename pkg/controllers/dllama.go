package controllers

import (
	"context"
	"fmt"

    v1 "github.com/gorizond/kold/pkg/apis/kold.gorizond.io/v1"
    "github.com/rancher/wrangler/v3/pkg/apply"
    "github.com/rancher/wrangler/v3/pkg/generic"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

type dllamaHandler struct {
	ctx     context.Context
	apply   apply.Apply
	dllamas generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	models  generic.ControllerInterface[*v1.Model, *v1.ModelList]
	roots   generic.ControllerInterface[*v1.Root, *v1.RootList]
	workers generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
}

func registerDllamaController(ctx context.Context, m *Manager) error {
	handler := &dllamaHandler{
		ctx:     ctx,
		apply:   m.Apply(ctx),
		dllamas: m.Kold.Dllama(),
		models:  m.Kold.Model(),
		roots:   m.Kold.Root(),
		workers: m.Kold.Worker(),
	}

	handler.dllamas.OnChange(ctx, "kold-dllama-controller", handler.onChange)
	handler.dllamas.OnRemove(ctx, "kold-dllama-controller", handler.onRemove)

	handler.models.OnChange(ctx, "kold-dllama-model-watch", handler.onRelatedModel)
	handler.roots.OnChange(ctx, "kold-dllama-root-watch", handler.onRelatedRoot)
	handler.workers.OnChange(ctx, "kold-dllama-worker-watch", handler.onRelatedWorker)
	return nil
}

func (h *dllamaHandler) onChange(key string, obj *v1.Dllama) (*v1.Dllama, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureTopology(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *dllamaHandler) onRemove(key string, obj *v1.Dllama) (*v1.Dllama, error) {
	return obj, nil
}

func (h *dllamaHandler) onRelatedModel(key string, obj *v1.Model) (*v1.Model, error) {
	if obj == nil {
		return nil, nil
	}
	if name := labelValue(obj.Labels, labelDllamaName); name != "" {
		h.dllamas.Enqueue(obj.Namespace, name)
	}
	return obj, nil
}

func (h *dllamaHandler) onRelatedRoot(key string, obj *v1.Root) (*v1.Root, error) {
	if obj == nil {
		return nil, nil
	}
	if name := labelValue(obj.Labels, labelDllamaName); name != "" {
		h.dllamas.Enqueue(obj.Namespace, name)
	}
	return obj, nil
}

func (h *dllamaHandler) onRelatedWorker(key string, obj *v1.Worker) (*v1.Worker, error) {
	if obj == nil {
		return nil, nil
	}
	if name := labelValue(obj.Labels, labelDllamaName); name != "" {
		h.dllamas.Enqueue(obj.Namespace, name)
	}
	return obj, nil
}

func (h *dllamaHandler) ensureTopology(dllama *v1.Dllama) error {
	objects := h.desiredObjects(dllama)
	setID := fmt.Sprintf("dllama-%s", dllama.Name)

	return h.apply.WithOwner(dllama).
		WithSetOwnerReference(true, false).
		WithSetID(setID).
		WithDefaultNamespace(dllama.Namespace).
		ApplyObjects(objects...)
}

func (h *dllamaHandler) desiredObjects(dllama *v1.Dllama) []runtime.Object {
	var objects []runtime.Object

	modelLabels := map[string]string{
		labelDllamaName: dllama.Name,
		labelComponent:  componentModel,
	}

	model := &v1.Model{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Model",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-model", dllama.Name),
			Namespace: dllama.Namespace,
			Labels:    modelLabels,
		},
		Spec: v1.ModelSpec{
			SourceURI:     dllama.Spec.ModelRef,
			LocalPath:     fmt.Sprintf("/models/%s", dllama.Spec.ModelRef),
			CacheSpec:     cloneCacheSpec(dllama.Spec.CacheSpec),
			LaunchOptions: append([]string{}, dllama.Spec.LaunchArgs...),
		},
	}
	objects = append(objects, model)

	rootLabels := map[string]string{
		labelDllamaName: dllama.Name,
		labelComponent:  componentRoot,
		labelRootName:   fmt.Sprintf("%s-root", dllama.Name),
	}

	root := &v1.Root{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Root",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-root", dllama.Name),
			Namespace: dllama.Namespace,
			Labels:    rootLabels,
		},
		Spec: v1.RootSpec{
			ModelRef:       dllama.Spec.ModelRef,
			Image:          dllama.Spec.RootImage,
			Args:           append([]string{}, dllama.Spec.LaunchArgs...),
			CacheSpec:      cloneCacheSpec(dllama.Spec.CacheSpec),
			WorkerSelector: map[string]string{labelDllamaName: dllama.Name},
		},
	}
	objects = append(objects, root)

	for _, worker := range h.desiredWorkers(dllama) {
		objects = append(objects, worker)
	}

	return objects
}

func (h *dllamaHandler) desiredWorkers(dllama *v1.Dllama) []*v1.Worker {
	var result []*v1.Worker
	workerCount := workersForReplicaPower(dllama.Spec.ReplicaPower)

	for i := int32(0); i < workerCount; i++ {
		workerLabels := map[string]string{
			labelDllamaName: dllama.Name,
			labelComponent:  componentWorker,
			labelWorkerName: fmt.Sprintf("%s-worker-%d", dllama.Name, i),
		}

		worker := &v1.Worker{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1.SchemeGroupVersion.String(),
				Kind:       "Worker",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-worker-%d", dllama.Name, i),
				Namespace: dllama.Namespace,
				Labels:    workerLabels,
			},
			Spec: v1.WorkerSpec{
				ModelRef:  dllama.Spec.ModelRef,
				Image:     dllama.Spec.WorkerImage,
				Args:      append([]string{}, dllama.Spec.LaunchArgs...),
				CacheSpec: cloneCacheSpec(dllama.Spec.CacheSpec),
				RootRef:   fmt.Sprintf("%s-root", dllama.Name),
				Slot:      i,
			},
		}
		result = append(result, worker)
	}

	return result
}

func workersForReplicaPower(power int32) int32 {
	if power <= 0 {
		return 0
	}
	if power >= 30 {
		power = 30
	}
	total := int32(1) << power
	if total <= 1 {
		return 0
	}
	return total - 1
}

func (h *dllamaHandler) ensureStatus(dllama *v1.Dllama) (*v1.Dllama, error) {
	updated := dllama.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	readyRoot := false
	if root, err := h.roots.Cache().Get(dllama.Namespace, fmt.Sprintf("%s-root", dllama.Name)); err == nil {
		readyRoot = isConditionTrue(root.Status.Conditions, conditionReady)
	}

	readyWorkers := int32(0)
	selector := labels.SelectorFromSet(map[string]string{labelDllamaName: dllama.Name})
	workers, _ := h.workers.Cache().List(dllama.Namespace, selector)
	for _, worker := range workers {
		if isConditionTrue(worker.Status.Conditions, conditionReady) {
			readyWorkers++
		}
	}

	updated.Status.ObservedGeneration = updated.Generation
	updated.Status.ReadyRoot = readyRoot
	updated.Status.ReadyWorkers = readyWorkers

	condition := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "TopologyNotReady",
		Message: "Root and worker resources are not ready",
	}

	expectedWorkers := workersForReplicaPower(dllama.Spec.ReplicaPower)
	if readyRoot && readyWorkers >= expectedWorkers {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "TopologyReady"
		condition.Message = "Root and workers are ready"
	}

	changed := setCondition(&updated.Status.Conditions, condition)
	if !changed && updated.Status.ReadyWorkers == dllama.Status.ReadyWorkers && updated.Status.ReadyRoot == dllama.Status.ReadyRoot {
		return dllama, nil
	}

	return h.dllamas.UpdateStatus(updated)
}

func cloneCacheSpec(spec *v1.CacheSpec) *v1.CacheSpec {
	if spec == nil {
		return nil
	}
	return spec.DeepCopy()
}
