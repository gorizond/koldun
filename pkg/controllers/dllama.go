package controllers

import (
	"context"
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	handler.dllamas.OnChange(ctx, "koldun-dllama-controller", handler.onChange)
	handler.dllamas.OnRemove(ctx, "koldun-dllama-controller", handler.onRemove)

	handler.models.OnChange(ctx, "koldun-dllama-model-watch", handler.onRelatedModel)
	handler.roots.OnChange(ctx, "koldun-dllama-root-watch", handler.onRelatedRoot)
	handler.workers.OnChange(ctx, "koldun-dllama-worker-watch", handler.onRelatedWorker)
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
	namespace, name := "", ""
	if obj != nil {
		namespace = obj.Namespace
		name = obj.Name
	} else {
		namespace, name = splitKey(key)
	}

	if namespace == "" || name == "" {
		return obj, nil
	}

	dllamas, err := h.dllamas.Cache().List("", labels.Everything())
	if err != nil {
		return obj, err
	}

	for _, dllama := range dllamas {
		if referencesModel(dllama, namespace, name) {
			h.dllamas.Enqueue(dllama.Namespace, dllama.Name)
		}
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
	setID := fmt.Sprintf("dllama-%s", dllama.Name)
	apply := h.apply.WithOwner(dllama).
		WithSetOwnerReference(true, false).
		WithSetID(setID).
		WithDefaultNamespace(dllama.Namespace)

	if !isModelKind(dllama.Spec.ModelRef.Kind) {
		return apply.ApplyObjects()
	}
	if dllama.Spec.ModelRef.APIGroup != "" && dllama.Spec.ModelRef.APIGroup != v1.GroupName {
		return apply.ApplyObjects()
	}
	if strings.TrimSpace(dllama.Spec.ModelRef.Name) == "" {
		return apply.ApplyObjects()
	}

	modelNamespace := referencedModelNamespace(dllama)
	model, err := h.models.Cache().Get(modelNamespace, dllama.Spec.ModelRef.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apply.ApplyObjects()
		}
		return err
	}

	if model.Status.OutputPVCName == "" {
		return apply.ApplyObjects()
	}

	objects := h.desiredObjects(dllama, model)
	return apply.ApplyObjects(objects...)
}

func (h *dllamaHandler) desiredObjects(dllama *v1.Dllama, model *v1.Model) []runtime.Object {
	var objects []runtime.Object

	if root := h.desiredRoot(dllama, model); root != nil {
		objects = append(objects, root)
	}

	for _, worker := range h.desiredWorkers(dllama, model) {
		objects = append(objects, worker)
	}

	return objects
}

func (h *dllamaHandler) desiredRoot(dllama *v1.Dllama, model *v1.Model) *v1.Root {
	rootLabels := map[string]string{
		labelDllamaName: dllama.Name,
		labelComponent:  componentRoot,
		labelRootName:   fmt.Sprintf("%s-root", dllama.Name),
		labelModelName:  model.Name,
	}

	return &v1.Root{
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
			ModelRef:       model.Status.OutputPVCName,
			Image:          dllama.Spec.RootImage,
			WorkerSelector: map[string]string{labelDllamaName: dllama.Name},
		},
	}
}

func (h *dllamaHandler) desiredWorkers(dllama *v1.Dllama, model *v1.Model) []*v1.Worker {
	var result []*v1.Worker
	workerCount := workersForReplicaPower(dllama.Spec.ReplicaPower)

	for i := int32(0); i < workerCount; i++ {
		workerLabels := map[string]string{
			labelDllamaName: dllama.Name,
			labelComponent:  componentWorker,
			labelWorkerName: fmt.Sprintf("%s-worker-%d", dllama.Name, i),
			labelModelName:  model.Name,
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
				ModelRef: model.Status.OutputPVCName,
				Image:    dllama.Spec.WorkerImage,
				RootRef:  fmt.Sprintf("%s-root", dllama.Name),
				Slot:     i,
			},
		}
		result = append(result, worker)
	}

	return result
}

const maxInt32 = int64(^uint32(0) >> 1)

func workersForReplicaPower(power int32) int32 {
	if power <= 0 {
		return 0
	}

	result := int64(power)*2 - 1
	if result > maxInt32 {
		return int32(maxInt32)
	}
	if result < 0 {
		return 0
	}
	return int32(result)
}

func (h *dllamaHandler) ensureStatus(dllama *v1.Dllama) (*v1.Dllama, error) {
	updated := dllama.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	condition := metav1.Condition{
		Type:   conditionReady,
		Status: metav1.ConditionFalse,
	}

	modelReady := false

	switch {
	case !isModelKind(dllama.Spec.ModelRef.Kind):
		condition.Reason = "InvalidModelReference"
		condition.Message = "spec.modelRef.kind must be Model"
	case dllama.Spec.ModelRef.APIGroup != "" && dllama.Spec.ModelRef.APIGroup != v1.GroupName:
		condition.Reason = "InvalidModelReference"
		condition.Message = fmt.Sprintf("spec.modelRef.apiGroup must be %s", v1.GroupName)
	case strings.TrimSpace(dllama.Spec.ModelRef.Name) == "":
		condition.Reason = "InvalidModelReference"
		condition.Message = "spec.modelRef.name must be provided"
	default:
		modelNamespace := referencedModelNamespace(dllama)
		model, err := h.models.Cache().Get(modelNamespace, dllama.Spec.ModelRef.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				condition.Reason = "ModelNotFound"
				condition.Message = fmt.Sprintf("Referenced model %s/%s not found", modelNamespace, dllama.Spec.ModelRef.Name)
			} else {
				condition.Reason = "ModelLookupFailed"
				condition.Message = fmt.Sprintf("Failed to fetch model %s/%s: %v", modelNamespace, dllama.Spec.ModelRef.Name, err)
			}
		} else if model.Status.OutputPVCName == "" {
			condition.Reason = "ModelNotReady"
			condition.Message = fmt.Sprintf("Referenced model %s/%s does not have status.outputPVCName yet", modelNamespace, model.Name)
		} else {
			modelReady = true
		}
	}

	readyRoot := false
	readyWorkers := int32(0)

	if modelReady {
		if root, err := h.roots.Cache().Get(dllama.Namespace, fmt.Sprintf("%s-root", dllama.Name)); err == nil {
			readyRoot = isConditionTrue(root.Status.Conditions, conditionReady)
		}

		selector := labels.SelectorFromSet(map[string]string{labelDllamaName: dllama.Name})
		workers, _ := h.workers.Cache().List(dllama.Namespace, selector)
		for _, worker := range workers {
			if isConditionTrue(worker.Status.Conditions, conditionReady) {
				readyWorkers++
			}
		}

		expectedWorkers := workersForReplicaPower(dllama.Spec.ReplicaPower)
		if readyRoot && readyWorkers >= expectedWorkers {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "TopologyReady"
			condition.Message = "Root and workers are ready"
		} else {
			condition.Reason = "TopologyNotReady"
			condition.Message = "Root and worker resources are not ready"
		}
	} else if condition.Reason == "" {
		condition.Reason = "ModelNotReady"
		condition.Message = "Referenced model is not ready"
	}

	updated.Status.ObservedGeneration = updated.Generation
	updated.Status.ReadyRoot = readyRoot
	updated.Status.ReadyWorkers = readyWorkers

	changed := setCondition(&updated.Status.Conditions, condition)
	if !changed && updated.Status.ReadyWorkers == dllama.Status.ReadyWorkers && updated.Status.ReadyRoot == dllama.Status.ReadyRoot {
		return dllama, nil
	}

	return h.dllamas.UpdateStatus(updated)
}

func isModelKind(kind string) bool {
	return strings.EqualFold(kind, "Model")
}

func referencedModelNamespace(dllama *v1.Dllama) string {
	if dllama.Spec.ModelRef.Namespace != "" {
		return dllama.Spec.ModelRef.Namespace
	}
	return dllama.Namespace
}

func referencesModel(dllama *v1.Dllama, modelNamespace, modelName string) bool {
	if dllama == nil {
		return false
	}
	if !isModelKind(dllama.Spec.ModelRef.Kind) {
		return false
	}
	if dllama.Spec.ModelRef.APIGroup != "" && dllama.Spec.ModelRef.APIGroup != v1.GroupName {
		return false
	}
	refNamespace := referencedModelNamespace(dllama)
	refName := strings.TrimSpace(dllama.Spec.ModelRef.Name)
	if refName == "" {
		return false
	}
	return refNamespace == modelNamespace && refName == modelName
}
