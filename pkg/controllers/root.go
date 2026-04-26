package controllers

import (
	"context"
	"fmt"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	validation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/pointer"
)

const (
	dllamaStartupProbePeriodSeconds  int32 = 1     // probe once per second during startup
	dllamaStartupProbeFailureSeconds int32 = 43200 // allow up to 12h before declaring startup failure

	statefulSetRevisionSuffixLength = 11 // "-" + 10 char hash
)

var (
	labelValueMaxFn = func() int { return validation.LabelValueMaxLength }
)

type rootHandler struct {
	ctx          context.Context
	apply        apply.Apply
	dllamas      generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	models       generic.ControllerInterface[*v1.Model, *v1.ModelList]
	roots        generic.ControllerInterface[*v1.Root, *v1.RootList]
	deployments  generic.ControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList]
	services     generic.ControllerInterface[*corev1.Service, *corev1.ServiceList]
	statefulsets generic.ControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList]
	workers      generic.ControllerInterface[*v1.Worker, *v1.WorkerList]

	workerStatusHook  func(*v1.Root) (bool, int32, []string, error)
	resolveDllamaHook func(*v1.Root) (*v1.Dllama, error)
	resolveModelHook  func(*v1.Dllama) (*v1.Model, error)
}

func registerRootController(ctx context.Context, m *Manager) error {
	handler := &rootHandler{
		ctx:          ctx,
		apply:        m.Apply(ctx),
		dllamas:      m.Kold.Dllama(),
		models:       m.Kold.Model(),
		roots:        m.Kold.Root(),
		deployments:  m.Apps.Deployment(),
		services:     m.Core.Service(),
		statefulsets: m.Apps.StatefulSet(),
		workers:      m.Kold.Worker(),
	}

	handler.roots.OnChange(ctx, "koldun-root-controller", handler.onChange)
	handler.roots.OnRemove(ctx, "koldun-root-controller", handler.onRemove)

	handler.services.OnChange(ctx, "koldun-root-service-watch", handler.onRelatedService)
	handler.statefulsets.OnChange(ctx, "koldun-root-statefulset-watch", handler.onRelatedStatefulSet)
	handler.statefulsets.OnRemove(ctx, "koldun-root-statefulset-remove", handler.onRelatedStatefulSet)
	handler.workers.OnChange(ctx, "koldun-root-worker-watch", handler.onRelatedWorker)
	handler.workers.OnRemove(ctx, "koldun-root-worker-remove", handler.onRelatedWorker)
	handler.dllamas.OnChange(ctx, "koldun-root-dllama-watch", handler.onRelatedDllama)
	handler.dllamas.OnRemove(ctx, "koldun-root-dllama-remove", handler.onRelatedDllama)
	handler.models.OnChange(ctx, "koldun-root-model-watch", handler.onRelatedModel)
	return nil
}

func rootStatefulSetName(name string) string {
	suffix := "-root"
	max := labelValueMaxFn() - statefulSetRevisionSuffixLength
	baseMax := max - len(suffix)
	if baseMax < 1 {
		baseMax = 1
	}
	base := strings.TrimSuffix(name, suffix)
	if len(base) <= baseMax {
		return base + suffix
	}
	lastDash := strings.LastIndex(base, "-")
	if lastDash <= 0 {
		return base[:baseMax] + suffix
	}
	tail := base[lastDash+1:]
	keep := baseMax - len(tail) - 1 // account for '-' separator
	if keep <= 0 {
		return base[:baseMax] + suffix
	}
	prefix := base[:keep]
	prefix = strings.TrimRight(prefix, "-")
	if prefix == "" {
		return base[:baseMax] + suffix
	}
	return fmt.Sprintf("%s-%s%s", prefix, tail, suffix)
}

func (h *rootHandler) onChange(key string, obj *v1.Root) (*v1.Root, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureService(obj); err != nil {
		return obj, err
	}
	if err := h.ensureStatefulSet(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *rootHandler) onRemove(key string, obj *v1.Root) (*v1.Root, error) {
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

func (h *rootHandler) onRelatedStatefulSet(key string, obj *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if obj == nil {
		return nil, nil
	}

	switch obj.Labels[labelComponent] {
	case componentRoot:
		rootName := obj.Labels[labelRootName]
		if rootName == "" {
			rootName = obj.Name
		}
		h.roots.Enqueue(obj.Namespace, rootName)
	case componentWorker:
		dllamaName := labelValue(obj.Labels, labelDllamaName)
		if dllamaName == "" {
			return obj, nil
		}
		h.roots.Enqueue(obj.Namespace, fmt.Sprintf("%s-root", dllamaName))
	}
	return obj, nil
}

func (h *rootHandler) onRelatedWorker(key string, obj *v1.Worker) (*v1.Worker, error) {
	if obj == nil {
		return nil, nil
	}
	rootName := strings.TrimSpace(obj.Spec.RootRef)
	if rootName == "" {
		if dllama := labelValue(obj.Labels, labelDllamaName); dllama != "" {
			rootName = fmt.Sprintf("%s-root", dllama)
		}
	}
	if rootName == "" {
		return obj, nil
	}
	h.roots.Enqueue(obj.Namespace, rootName)
	return obj, nil
}

func (h *rootHandler) ensureStatefulSet(root *v1.Root) error {
	allWorkersReady, _, workerEndpoints, err := h.workerStatus(root)
	if err != nil {
		return err
	}
	if !allWorkersReady {
		return nil
	}

	dllama, err := h.resolveDllama(root)
	if err != nil {
		return err
	}
	model, err := h.resolveModel(dllama)
	if err != nil {
		return err
	}

	weightsFloatType := ""
	if model.Spec.Conversion != nil {
		weightsFloatType = strings.TrimSpace(model.Spec.Conversion.WeightsFloatType)
	}
	// PreConverted models use q80 as default weightsFloatType if not specified
	if weightsFloatType == "" && model.Spec.PreConverted {
		weightsFloatType = "q80"
	}
	if weightsFloatType == "" {
		return fmt.Errorf("model %s/%s conversion.weightsFloatType is required", model.Namespace, model.Name)
	}

	// Use NThreads from Root spec, default to 1 for single-core inference.
	threads := root.Spec.NThreads
	if threads <= 0 {
		threads = 1
	}

	workerReplicas := workersForReplicaPower(dllama.Spec.ReplicaPower)
	if workerReplicas < 0 {
		workerReplicas = 0
	}

	var override *float64
	if root.Spec.Memory != nil && root.Spec.Memory.OverheadMaxRatio != nil {
		override = root.Spec.Memory.OverheadMaxRatio
	}
	rootMemory, workerMemory, haveMemoryPlan := calculateMemoryRequests(model.Status.ConversionSizeBytes, workerReplicas, override)

	quantType := ""
	if model.Spec.Conversion != nil {
		quantType = strings.TrimSpace(model.Spec.Conversion.ConvertWeights)
	}
	if quantType == "" {
		quantType = weightsFloatType
	}

	// Use launchOptions from Model if available, otherwise fallback to constructed paths
	modelFile, tokenizerFile := parseLaunchOptions(model.Spec.LaunchOptions)
	if modelFile == "" {
		modelFile = fmt.Sprintf("model/dllama_model_%s_%s.m", model.Name, quantType)
	}
	if tokenizerFile == "" {
		tokenizerFile = fmt.Sprintf("model/dllama_tokenizer_%s.t", model.Name)
	}

	if strings.TrimSpace(root.Spec.ModelRef) == "" {
		return fmt.Errorf("root %s/%s missing spec.modelRef", root.Namespace, root.Name)
	}

	stsName := rootStatefulSetName(root.Name)

	labels := map[string]string{
		labelComponent:        componentRoot,
		labelRootName:         sanitizeLabelValue(root.Name),
		labelConversationHash: labelValue(root.Labels, labelConversationHash),
	}
	if dllamaName := labelValue(root.Labels, labelDllamaName); dllamaName != "" {
		labels[labelDllamaName] = dllamaName
	}

	selector := map[string]string{}
	for k, v := range labels {
		selector[k] = v
	}

	rootResources := corev1.ResourceRequirements{}
	if haveMemoryPlan {
		rootResources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: rootMemory},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: rootMemory},
		}
	}

	rootContainer := h.rootContainer(root, modelFile, tokenizerFile, weightsFloatType, threads, workerEndpoints, rootResources)
	llmContainer := h.llmSidecarContainer(root)

	var podAnnotations map[string]string
	if haveMemoryPlan {
		podAnnotations = map[string]string{}
		sizeHuman := strings.TrimSpace(model.Status.ConversionSizeHuman)
		if sizeHuman == "" {
			sizeHuman = fmt.Sprintf("%dB", model.Status.ConversionSizeBytes)
		}
		totalNodes := workerReplicas + 1
		podAnnotations[annotationConversionSizeHuman] = sizeHuman
		podAnnotations[annotationMemoryPlan] = fmt.Sprintf("model=%s nodes=%d root=%s worker=%s", sizeHuman, totalNodes, rootMemory.String(), workerMemory.String())
	}

	sts := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName,
			Namespace: root.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: root.Name,
			Replicas:    pointer.Int32(1),
			Selector:    &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: pointer.Int64(0),
					Volumes: []corev1.Volume{
						{
							Name: "model-output",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: root.Spec.ModelRef, ReadOnly: true},
							},
						},
					},
					Containers: []corev1.Container{
						rootContainer,
						llmContainer,
					},
				},
			},
		},
	}

	if err := h.apply.WithOwner(root).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(root.Namespace).
		WithSetID(fmt.Sprintf("root-%s-statefulset", root.Name)).
		ApplyObjects(sts); err != nil {
		return err
	}

	if h.deployments != nil {
		if _, err := h.deployments.Cache().Get(root.Namespace, root.Name); err == nil {
			if err := h.deployments.Delete(root.Namespace, root.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete legacy root deployment: %w", err)
			}
		} else if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("lookup legacy root deployment: %w", err)
		}
	}

	return nil
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
				labelComponent:        componentRoot,
				labelRootName:         sanitizeLabelValue(root.Name),
				labelConversationHash: labelValue(root.Labels, labelConversationHash),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector: map[string]string{
				labelComponent: componentRoot,
				labelRootName:  sanitizeLabelValue(root.Name),
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "tcp",
					Port:       9999,
					TargetPort: intstr.FromInt(9999),
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
		WithSetID(fmt.Sprintf("root-%s-service", root.Name)).
		ApplyObjects(svc)
}

func (h *rootHandler) workerStatus(root *v1.Root) (allReady bool, readyCount int32, endpoints []string, err error) {
	if hook := h.workerStatusHook; hook != nil {
		return hook(root)
	}
	if root == nil {
		return false, 0, nil, nil
	}
	if len(root.Spec.WorkerSelector) == 0 {
		// No worker selector means standalone mode - workers are ready (empty list)
		return true, 0, nil, nil
	}

	dllama, err := h.resolveDllama(root)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, 0, nil, nil
		}
		return false, 0, nil, err
	}

	replicas := workersForReplicaPower(dllama.Spec.ReplicaPower)
	if replicas <= 0 {
		// Standalone mode - no workers needed, always ready
		return true, 0, nil, nil
	}

	workerName := workerResourceName(dllama.Name)
	worker, err := h.workers.Cache().Get(root.Namespace, workerName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, 0, nil, nil
		}
		return false, 0, nil, err
	}

	endpoints = make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		endpoints = append(endpoints, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:9999", worker.Name, i, worker.Name, worker.Namespace))
	}

	sts, err := h.statefulsets.Cache().Get(worker.Namespace, worker.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, 0, endpoints, nil
		}
		return false, 0, nil, err
	}

	readyCount = sts.Status.ReadyReplicas
	allReady = readyCount >= replicas
	return allReady, readyCount, endpoints, nil
}

func (h *rootHandler) rootContainer(root *v1.Root, modelFile, tokenizerFile, weightsFloatType string, threads int32, workers []string, resources corev1.ResourceRequirements) corev1.Container {
	// distributed-llama requires q80 sync type for inter-node communication regardless of weights type
	bufferFloatType := "q80"
	args := []string{"--port", "9999", "--model", modelFile, "--tokenizer", tokenizerFile, "--buffer-float-type", bufferFloatType, "--nthreads", fmt.Sprintf("%d", threads), "--max-seq-len", "4096"}
	if len(workers) > 0 {
		args = append(args, "--workers")
		args = append(args, workers...)
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

	if root.Spec.NATS != nil {
		env = append(env, corev1.EnvVar{Name: "NATS_URL", Value: root.Spec.NATS.URL})
		if root.Spec.NATS.CredentialsSecret != nil {
			env = append(env, corev1.EnvVar{
				Name: "NATS_CREDS",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: root.Spec.NATS.CredentialsSecret.Name},
						Key:                  "nats.creds",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

	container := corev1.Container{
		Name:            "root",
		Image:           root.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"dllama-api"},
		Args:            args,
		Env:             env,
		// NOTE: No health probes configured for root container.
		//
		// The root container (dllama-api) runs a single-threaded HTTP server on port 9999.
		// During CPU inference, the /v1/models endpoint becomes unresponsive.
		// Any HTTP or TCP probe would cause Kubernetes to kill the pod mid-inference.
		//
		// Health management is delegated to:
		// 1. Kubernetes: Restarts container if process exits
		// 2. LLM sidecar: Manages readiness via NATS subscription (pulls work when ready)
		//
		// TODO: Patch distributed-llama to add a non-blocking /healthz endpoint.
		// See: https://github.com/gorizond/koldun/issues/XXX
		Ports: []corev1.ContainerPort{{ContainerPort: 9999}},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "model-output",
				MountPath: "/model",
				ReadOnly:  true,
			},
		},
	}

	if !isResourceRequirementsEmpty(resources) {
		container.Resources = resources
	}

	return container
}

func (h *rootHandler) llmSidecarContainer(root *v1.Root) corev1.Container {
	hash := labelValue(root.Annotations, labelConversationHash)
	if hash == "" {
		hash = labelValue(root.Labels, labelConversationHash)
	}
	args := []string{"llm", "--llm-sidecar-url", "http://127.0.0.1:9999"}
	if hash != "" {
		args = append(args, "--llm-hash", hash)
	}
	if root.Spec.NATS != nil && strings.TrimSpace(root.Spec.NATS.URL) != "" {
		args = append(args, "--llm-nats-url", root.Spec.NATS.URL)
	}
	dllamaName := labelValue(root.Labels, labelDllamaName)
	queuePrefix := strings.TrimSpace(root.Annotations[annotationSessionQueuePrefix])
	if queuePrefix != "" && !strings.HasSuffix(queuePrefix, ".") {
		queuePrefix += "."
	}
	if queuePrefix != "" {
		args = append(args, "--llm-in-prefix", queuePrefix)
	}
	if queuePrefix != "" && dllamaName != "" {
		requestSubject := fmt.Sprintf("%s%s.in", queuePrefix, dllamaName)
		stateSubject := fmt.Sprintf("%s%s.state", queuePrefix, dllamaName)
		args = append(args, "--llm-request-subject", requestSubject)
		args = append(args, "--llm-state-subject", stateSubject)
	}
	if dllamaName != "" {
		args = append(args, "--llm-dllama-name", dllamaName)
	}

	env := []corev1.EnvVar{
		{Name: "HASH_KOLDUN", Value: hash},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
	}
	env = append(env, buildLLMNATSEnv(root.Spec.NATS)...)

	return corev1.Container{
		Name:            "llm",
		Image:           root.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/koldun"},
		Args:            args,
		Env:             env,
		Ports:           []corev1.ContainerPort{{ContainerPort: 8081}},
	}
}

func buildLLMNATSEnv(cfg *v1.RootNATSConfig) []corev1.EnvVar {
	if cfg == nil {
		return nil
	}
	vars := []corev1.EnvVar{{Name: "NATS_URL", Value: cfg.GetURL()}}
	if cfg.CredentialsSecret != nil {
		vars = append(vars, corev1.EnvVar{
			Name: "NATS_CREDS",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CredentialsSecret.Name},
					Key:                  "nats.creds",
					Optional:             pointer.Bool(true),
				},
			},
		})
	}
	return vars
}

func (h *rootHandler) resolveDllama(root *v1.Root) (*v1.Dllama, error) {
	if hook := h.resolveDllamaHook; hook != nil {
		return hook(root)
	}
	dllamaName := labelValue(root.Labels, labelDllamaName)
	if dllamaName == "" {
		return nil, fmt.Errorf("root %s/%s missing dllama label", root.Namespace, root.Name)
	}
	return h.dllamas.Cache().Get(root.Namespace, dllamaName)
}

func (h *rootHandler) resolveModel(dllama *v1.Dllama) (*v1.Model, error) {
	if hook := h.resolveModelHook; hook != nil {
		return hook(dllama)
	}
	if dllama == nil {
		return nil, fmt.Errorf("dllama not provided")
	}
	modelName := strings.TrimSpace(dllama.Spec.ModelRef.Name)
	if modelName == "" {
		return nil, fmt.Errorf("dllama %s/%s has empty spec.modelRef.name", dllama.Namespace, dllama.Name)
	}
	modelNamespace := referencedModelNamespace(dllama)
	return h.models.Cache().Get(modelNamespace, modelName)
}

func (h *rootHandler) onRelatedDllama(key string, obj *v1.Dllama) (*v1.Dllama, error) {
	namespace, name := "", ""
	if obj != nil {
		namespace, name = obj.Namespace, obj.Name
	} else {
		namespace, name = splitKey(key)
	}
	if namespace == "" || name == "" {
		return obj, nil
	}
	h.roots.Enqueue(namespace, fmt.Sprintf("%s-root", name))
	return obj, nil
}

func (h *rootHandler) onRelatedModel(key string, obj *v1.Model) (*v1.Model, error) {
	namespace, name := "", ""
	if obj != nil {
		namespace, name = obj.Namespace, obj.Name
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
			h.roots.Enqueue(dllama.Namespace, fmt.Sprintf("%s-root", dllama.Name))
		}
	}
	return obj, nil
}

func (h *rootHandler) ensureStatus(root *v1.Root) (*v1.Root, error) {
	updated := root.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	ready := metav1.Condition{
		Type:    conditionReady,
		Reason:  "WorkersNotReady",
		Message: "Waiting for worker pods to become ready",
		Status:  metav1.ConditionFalse,
	}

	workersReady, _, _, err := h.workerStatus(root)
	if err != nil {
		ready.Reason = "WorkersLookupFailed"
		ready.Message = fmt.Sprintf("Failed to list workers: %v", err)
	} else if workersReady {
		ready.Reason = "StatefulSetNotReady"
		ready.Message = "Root statefulset is not yet ready"
		if sts, err := h.statefulsets.Cache().Get(root.Namespace, rootStatefulSetName(root.Name)); err == nil {
			if sts.Status.ReadyReplicas >= 1 {
				ready.Status = metav1.ConditionTrue
				ready.Reason = "StatefulSetReady"
				ready.Message = "Root statefulset is ready"
			}
		} else if !apierrors.IsNotFound(err) {
			ready.Reason = "StatefulSetLookupFailed"
			ready.Message = fmt.Sprintf("Failed to fetch root statefulset: %v", err)
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

// parseLaunchOptions extracts --model and --tokenizer paths from launchOptions slice
func parseLaunchOptions(opts []string) (modelPath, tokenizerPath string) {
	for i := 0; i < len(opts)-1; i++ {
		switch opts[i] {
		case "--model":
			modelPath = opts[i+1]
		case "--tokenizer":
			tokenizerPath = opts[i+1]
		}
	}
	return
}
