package controllers

import (
	"context"
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
)

type dllamaHandler struct {
	ctx          context.Context
	apply        apply.Apply
	dllamas      generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	models       generic.ControllerInterface[*v1.Model, *v1.ModelList]
	roots        generic.ControllerInterface[*v1.Root, *v1.RootList]
	workers      generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
	statefulsets generic.ControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList]
	ingresses    generic.ControllerInterface[*v1.Ingress, *v1.IngressList]
}

func registerDllamaController(ctx context.Context, m *Manager) error {
	handler := &dllamaHandler{
		ctx:          ctx,
		apply:        m.Apply(ctx),
		dllamas:      m.Kold.Dllama(),
		models:       m.Kold.Model(),
		roots:        m.Kold.Root(),
		workers:      m.Kold.Worker(),
		statefulsets: m.Apps.StatefulSet(),
		ingresses:    m.Kold.Ingress(),
	}

	handler.dllamas.OnChange(ctx, "koldun-dllama-controller", handler.onChange)
	handler.dllamas.OnRemove(ctx, "koldun-dllama-controller", handler.onRemove)

	handler.models.OnChange(ctx, "koldun-dllama-model-watch", handler.onRelatedModel)
	handler.roots.OnChange(ctx, "koldun-dllama-root-watch", handler.onRelatedRoot)
	handler.workers.OnChange(ctx, "koldun-dllama-worker-watch", handler.onRelatedWorker)
	handler.ingresses.OnChange(ctx, "koldun-dllama-ingress-watch", handler.onRelatedIngress)
	handler.statefulsets.OnChange(ctx, "koldun-dllama-statefulset-watch", handler.onRelatedStatefulSet)
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

func (h *dllamaHandler) onRelatedIngress(key string, obj *v1.Ingress) (*v1.Ingress, error) {
	// When an ingress changes, we need to update all dllamas that might be using its NATS URL
	dllamas, err := h.dllamas.Cache().List("", labels.Everything())
	if err != nil {
		return obj, err
	}

	// Trigger reconciliation for all dllamas since any of them might be using this ingress's NATS URL
	for _, dllama := range dllamas {
		h.dllamas.Enqueue(dllama.Namespace, dllama.Name)
	}

	return obj, nil
}

func (h *dllamaHandler) onRelatedStatefulSet(key string, obj *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
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
	conversationHashLabel := labelValue(dllama.Labels, labelConversationHash)
	conversationHashAnnotation := labelValue(dllama.Annotations, labelConversationHash)
	if conversationHashAnnotation == "" {
		conversationHashAnnotation = conversationHashLabel
	}

	rootLabels := map[string]string{
		labelDllamaName:       dllama.Name,
		labelComponent:        componentRoot,
		labelRootName:         fmt.Sprintf("%s-root", dllama.Name),
		labelModelName:        model.Name,
		labelConversationHash: conversationHashLabel,
	}
	if session := labelValue(dllama.Labels, labelSessionName); session != "" {
		rootLabels[labelSessionName] = session
	}

	natsConfig := dllama.Spec.NATS.ToRootConfig()
	// Copy NATS URL from ingress if dllama doesn't have its own NATS config
	if natsConfig == nil || natsConfig.URL == "" {
		if ingressNatsURL := h.getIngressNatsURL(dllama); ingressNatsURL != "" {
			if natsConfig == nil {
				natsConfig = &v1.RootNATSConfig{}
			}
			natsConfig.URL = ingressNatsURL
		}
	}

	rootAnnotations := map[string]string{labelConversationHash: conversationHashAnnotation}
	for _, key := range []string{
		annotationSessionQueuePrefix,
		annotationSessionAssignmentsBucket,
		annotationSessionBacklogSubject,
		annotationSessionStateStream,
	} {
		if value := labelValue(dllama.Annotations, key); strings.TrimSpace(value) != "" {
			rootAnnotations[key] = value
		}
	}

	root := &v1.Root{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Root",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-root", dllama.Name),
			Namespace:   dllama.Namespace,
			Labels:      rootLabels,
			Annotations: rootAnnotations,
		},
		Spec: v1.RootSpec{
			ModelRef:       model.Status.OutputPVCName,
			Image:          dllama.Spec.RootImage,
			WorkerSelector: map[string]string{labelDllamaName: dllama.Name},
			NATS:           natsConfig,
		},
	}
	if ratio := h.getIngressRootOverheadMaxRatio(dllama); ratio != nil {
		root.Spec.Memory = &v1.RootMemorySpec{OverheadMaxRatio: pointer.Float64(*ratio)}
	}
	return root
}

func (h *dllamaHandler) desiredWorkers(dllama *v1.Dllama, model *v1.Model) []*v1.Worker {
	workerName := workerResourceName(dllama.Name)
	workerLabels := map[string]string{
		labelDllamaName:       sanitizeLabelValue(dllama.Name),
		labelComponent:        componentWorker,
		labelWorkerName:       sanitizeLabelValue(workerName),
		labelModelName:        sanitizeLabelValue(model.Name),
		labelConversationHash: sanitizeLabelValue(labelValue(dllama.Labels, labelConversationHash)),
	}
	if session := labelValue(dllama.Labels, labelSessionName); session != "" {
		workerLabels[labelSessionName] = sanitizeLabelValue(session)
	}

	conversationHashAnnotation := labelValue(dllama.Annotations, labelConversationHash)
	if conversationHashAnnotation == "" {
		conversationHashAnnotation = labelValue(dllama.Labels, labelConversationHash)
	}

	natsConfig := dllama.Spec.NATS.ToWorkerConfig()
	// Copy NATS URL from ingress if dllama doesn't have its own NATS config
	if natsConfig == nil || natsConfig.URL == "" {
		if ingressNatsURL := h.getIngressNatsURL(dllama); ingressNatsURL != "" {
			if natsConfig == nil {
				natsConfig = &v1.WorkerNATSConfig{}
			}
			natsConfig.URL = ingressNatsURL
		}
	}

	worker := &v1.Worker{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       "Worker",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        workerName,
			Namespace:   dllama.Namespace,
			Labels:      workerLabels,
			Annotations: map[string]string{labelConversationHash: conversationHashAnnotation},
		},
		Spec: v1.WorkerSpec{
			ModelRef: model.Status.OutputPVCName,
			Image:    dllama.Spec.WorkerImage,
			RootRef:  fmt.Sprintf("%s-root", dllama.Name),
			Slot:     0,
			NATS:     natsConfig,
		},
	}

	return []*v1.Worker{worker}
}

func (h *dllamaHandler) readyReplicasForWorker(worker *v1.Worker) (int32, error) {
	if worker == nil {
		return 0, nil
	}

	sts, err := h.statefulsets.Cache().Get(worker.Namespace, worker.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	return sts.Status.ReadyReplicas, nil
}

const maxInt32 = int64(^uint32(0) >> 1)

func workersForReplicaPower(power int32) int32 {
	switch {
	case power <= 0:
		return 0
	case power >= 31:
		return int32(maxInt32)
	default:
		return int32((int64(1) << power) - 1)
	}
}

func (h *dllamaHandler) getIngressNatsURL(dllama *v1.Dllama) string {
	// Get all ingresses in the same namespace
	ingresses, err := h.ingresses.Cache().List(dllama.Namespace, labels.Everything())
	if err != nil {
		return ""
	}

	// Find the first ingress that has a NATS URL configured
	for _, ing := range ingresses {
		if ing.Spec.Backend.NATS.URL != "" {
			return ing.Spec.Backend.NATS.URL
		}
	}

	// If no ingress found in same namespace, try cluster-wide search
	allIngresses, err := h.ingresses.Cache().List("", labels.Everything())
	if err != nil {
		return ""
	}

	for _, ing := range allIngresses {
		if ing.Spec.Backend.NATS.URL != "" {
			return ing.Spec.Backend.NATS.URL
		}
	}

	return ""
}

func (h *dllamaHandler) getIngressRootOverheadMaxRatio(dllama *v1.Dllama) *float64 {
	lookup := func(ings []*v1.Ingress) *float64 {
		for _, ing := range ings {
			if ing.Spec.Backend.RootMemory != nil && ing.Spec.Backend.RootMemory.OverheadMaxRatio != nil {
				val := *ing.Spec.Backend.RootMemory.OverheadMaxRatio
				return &val
			}
		}
		return nil
	}

	if ingresses, err := h.ingresses.Cache().List(dllama.Namespace, labels.Everything()); err == nil {
		if v := lookup(ingresses); v != nil {
			return v
		}
	}
	if allIngresses, err := h.ingresses.Cache().List("", labels.Everything()); err == nil {
		if v := lookup(allIngresses); v != nil {
			return v
		}
	}
	return nil
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
		workers, err := h.workers.Cache().List(dllama.Namespace, selector)
		if err != nil {
			return dllama, err
		}
		for _, worker := range workers {
			readyReplicas, err := h.readyReplicasForWorker(worker)
			if err != nil {
				return dllama, err
			}
			readyWorkers += readyReplicas
		}

		expectedWorkers := workersForReplicaPower(dllama.Spec.ReplicaPower)
		if expectedWorkers <= 0 {
			expectedWorkers = 1
		}
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
