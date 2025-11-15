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
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

type ingressHandler struct {
	ctx         context.Context
	apply       apply.Apply
	ingresses   generic.ControllerInterface[*v1.Ingress, *v1.IngressList]
	deployments generic.ControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList]
	services    generic.ControllerInterface[*corev1.Service, *corev1.ServiceList]
}

func registerIngressController(ctx context.Context, m *Manager) error {
	handler := &ingressHandler{
		ctx:         ctx,
		apply:       m.Apply(ctx),
		ingresses:   m.Kold.Ingress(),
		deployments: m.Apps.Deployment(),
		services:    m.Core.Service(),
	}

	handler.ingresses.OnChange(ctx, "koldun-ingress-controller", handler.onChange)
	handler.ingresses.OnRemove(ctx, "koldun-ingress-controller", handler.onRemove)

	handler.deployments.OnChange(ctx, "koldun-ingress-deployment-watch", handler.onRelatedDeployment)
	handler.services.OnChange(ctx, "koldun-ingress-service-watch", handler.onRelatedService)

	return nil
}

func (h *ingressHandler) onChange(key string, obj *v1.Ingress) (*v1.Ingress, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureResources(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *ingressHandler) onRemove(key string, obj *v1.Ingress) (*v1.Ingress, error) {
	return obj, nil
}

func (h *ingressHandler) onRelatedDeployment(key string, obj *appsv1.Deployment) (*appsv1.Deployment, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.Labels[labelComponent] != componentBackend {
		return obj, nil
	}
	name := obj.Labels[labelBackendName]
	if name == "" {
		return obj, nil
	}
	h.ingresses.Enqueue(obj.Namespace, name)
	return obj, nil
}

func (h *ingressHandler) onRelatedService(key string, obj *corev1.Service) (*corev1.Service, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.Labels[labelComponent] != componentBackend {
		return obj, nil
	}
	name := obj.Labels[labelBackendName]
	if name == "" {
		return obj, nil
	}
	h.ingresses.Enqueue(obj.Namespace, name)
	return obj, nil
}

func (h *ingressHandler) ensureResources(ing *v1.Ingress) error {
	if err := validateIngressSpec(&ing.Spec); err != nil {
		return err
	}

	backendName := fmt.Sprintf("%s-backend", ing.Name)
	serviceName := backendName
	routeName := backendName

	applier := h.apply.WithOwner(ing).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(ing.Namespace).
		WithSetID(fmt.Sprintf("ingress-%s", ing.Name))

	deployment := desiredBackendDeployment(ing, backendName)
	service := desiredBackendService(ing, serviceName)
	route := desiredKubernetesIngress(ing, serviceName, routeName)

	return applier.ApplyObjects(deployment, service, route)
}

func desiredBackendDeployment(ing *v1.Ingress, name string) *appsv1.Deployment {
	spec := ing.Spec.Backend
	svcPort := servicePort(&ing.Spec)
	args := backendArgs(&ing.Spec, svcPort)

	labels := map[string]string{
		labelComponent:   componentBackend,
		labelBackendName: ing.Name,
	}

	replicas := pointer.Int32(1)
	pullPolicy := corev1.PullIfNotPresent
	if spec.ImagePullPolicy != "" {
		pullPolicy = corev1.PullPolicy(spec.ImagePullPolicy)
	}

	container := corev1.Container{
		Name:            "backend",
		Image:           spec.Image,
		ImagePullPolicy: pullPolicy,
		Args:            args,
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: svcPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
			},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")},
			},
		},
	}

	if len(spec.ExtraArgs) > 0 {
		container.Args = append(container.Args, spec.ExtraArgs...)
	}
	if !isResourceRequirementsEmpty(spec.Resources) {
		container.Resources = spec.Resources
	}

	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ing.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
				},
			},
		},
	}

	return deployment
}

func desiredBackendService(ing *v1.Ingress, name string) *corev1.Service {
	svcSpec := ing.Spec.Service
	svcType := corev1.ServiceTypeClusterIP
	if svcSpec != nil && svcSpec.Type != "" {
		svcType = corev1.ServiceType(svcSpec.Type)
	}
	port := servicePort(&ing.Spec)

	labels := map[string]string{
		labelComponent:   componentBackend,
		labelBackendName: ing.Name,
	}

	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ing.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type: svcType,
			Selector: map[string]string{
				labelComponent:   componentBackend,
				labelBackendName: ing.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       port,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

func desiredKubernetesIngress(ing *v1.Ingress, serviceName, name string) *networkingv1.Ingress {
	route := ing.Spec.Route
	path := route.Path
	if path == "" {
		path = "/"
	}
	pathType := pathTypePtr(route.PathType)

	tls := make([]networkingv1.IngressTLS, len(route.TLS))
	for i := range route.TLS {
		tls[i] = networkingv1.IngressTLS{
			SecretName: route.TLS[i].SecretName,
			Hosts:      append([]string(nil), route.TLS[i].Hosts...),
		}
	}

	labels := map[string]string{
		labelComponent:   componentBackend,
		labelBackendName: ing.Name,
	}

	obj := &networkingv1.Ingress{
		TypeMeta: metav1.TypeMeta{
			APIVersion: networkingv1.SchemeGroupVersion.String(),
			Kind:       "Ingress",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   ing.Namespace,
			Labels:      labels,
			Annotations: route.Annotations,
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					Host: route.Host,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     path,
									PathType: pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: serviceName,
											Port: networkingv1.ServiceBackendPort{Number: servicePort(&ing.Spec)},
										},
									},
								},
							},
						},
					},
				},
			},
			TLS: tls,
		},
	}

	if route.IngressClassName != "" {
		obj.Spec.IngressClassName = pointer.String(route.IngressClassName)
	}

	return obj
}

func (h *ingressHandler) ensureStatus(ing *v1.Ingress) (*v1.Ingress, error) {
	updated := ing.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	deploymentName := fmt.Sprintf("%s-backend", ing.Name)
	serviceName := deploymentName
	routeName := deploymentName

	condition := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "BackendNotReady",
		Message: "backend deployment is not yet ready",
	}

	if dep, err := h.deployments.Cache().Get(ing.Namespace, deploymentName); err == nil {
		if dep.Status.ReadyReplicas >= 1 {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "BackendReady"
			condition.Message = "backend deployment is ready"
		}
	}

	changed := setCondition(&updated.Status.Conditions, condition)
	if updated.Status.ObservedGeneration != updated.Generation {
		updated.Status.ObservedGeneration = updated.Generation
		changed = true
	}
	updated.Status.BackendServiceName = serviceName
	updated.Status.IngressName = routeName

	if !changed {
		return ing, nil
	}

	return h.ingresses.UpdateStatus(updated)
}

func validateIngressSpec(spec *v1.IngressSpec) error {
	if strings.TrimSpace(spec.Backend.Image) == "" {
		return fmt.Errorf("backend.image is required")
	}
	if strings.TrimSpace(spec.Backend.RootImage) == "" {
		return fmt.Errorf("backend.rootImage is required")
	}
	if strings.TrimSpace(spec.Backend.WorkerImage) == "" {
		return fmt.Errorf("backend.workerImage is required")
	}
	if strings.TrimSpace(spec.Backend.NATS.URL) == "" {
		return fmt.Errorf("backend.nats.url is required")
	}
	if strings.TrimSpace(spec.Route.Host) == "" {
		return fmt.Errorf("route.host is required")
	}
	if spec.Backend.ReplicaPower < 0 {
		return fmt.Errorf("backend.replicaPower must be >= 0")
	}
	if rm := spec.Backend.RootMemory; rm != nil && rm.OverheadMaxRatio != nil {
		if *rm.OverheadMaxRatio <= 0 {
			return fmt.Errorf("backend.rootMemory.overheadMaxRatio must be > 0")
		}
	}
	if scaling := spec.Backend.SessionScaling; scaling != nil {
		if scaling.MinDllamas < 0 {
			return fmt.Errorf("backend.sessionScaling.minDllamas must be >= 0")
		}
		if scaling.MaxDllamas < 0 {
			return fmt.Errorf("backend.sessionScaling.maxDllamas must be >= 0")
		}
		if scaling.MinDllamas > 0 && scaling.MaxDllamas > 0 && scaling.MaxDllamas < scaling.MinDllamas {
			return fmt.Errorf("backend.sessionScaling.maxDllamas must be >= minDllamas")
		}
	}
	return nil
}

func backendArgs(spec *v1.IngressSpec, port int32) []string {
	args := []string{
		"--mode=backend",
		fmt.Sprintf("--backend-listen=:%d", port),
		"--backend-namespace=$(POD_NAMESPACE)",
		fmt.Sprintf("--backend-root-image=%s", spec.Backend.RootImage),
		fmt.Sprintf("--backend-worker-image=%s", spec.Backend.WorkerImage),
	}

	if spec.Backend.ReplicaPower > 0 {
		args = append(args, fmt.Sprintf("--backend-replica-power=%d", spec.Backend.ReplicaPower))
	}

	dispatcherImage := spec.Backend.DispatcherImage
	if dispatcherImage == "" {
		dispatcherImage = spec.Backend.Image
	}
	if dispatcherImage != "" {
		args = append(args, fmt.Sprintf("--backend-session-dispatcher-image=%s", dispatcherImage))
	}
	if addr := strings.TrimSpace(spec.Backend.DispatcherMetricsListen); addr != "" {
		args = append(args, fmt.Sprintf("--backend-session-dispatcher-metrics-listen=%s", addr))
	}

	if spec.Backend.NATS.URL != "" {
		args = append(args, fmt.Sprintf("--backend-nats-url=%s", spec.Backend.NATS.URL))
	}
	if spec.Backend.NATS.ConversationBucket != "" {
		args = append(args, fmt.Sprintf("--backend-conversation-bucket=%s", spec.Backend.NATS.ConversationBucket))
	}
	if spec.Backend.NATS.ModelsBucket != "" {
		args = append(args, fmt.Sprintf("--backend-models-bucket=%s", spec.Backend.NATS.ModelsBucket))
	}
	if spec.Backend.NATS.TokensBucket != "" {
		args = append(args, fmt.Sprintf("--backend-tokens-bucket=%s", spec.Backend.NATS.TokensBucket))
	}
	if spec.Backend.NATS.ModelPrefix != "" {
		args = append(args, fmt.Sprintf("--backend-model-prefix=%s", spec.Backend.NATS.ModelPrefix))
	}
	if spec.Backend.NATS.TokenPrefix != "" {
		args = append(args, fmt.Sprintf("--backend-token-prefix=%s", spec.Backend.NATS.TokenPrefix))
	}
	ttlPrefix := spec.Backend.NATS.TTLPrefix
	if ttlPrefix == "" {
		ttlPrefix = "nats_ttl_"
	}
	args = append(args, fmt.Sprintf("--backend-ttl-prefix=%s", ttlPrefix))

	if spec.Backend.ConversationTTL != nil {
		args = append(args, fmt.Sprintf("--backend-conversation-ttl=%s", spec.Backend.ConversationTTL.Duration.String()))
	}
	if spec.Backend.ResponseTimeout != nil {
		args = append(args, fmt.Sprintf("--backend-response-timeout=%s", spec.Backend.ResponseTimeout.Duration.String()))
	}
	if spec.Backend.SessionScaling != nil {
		scaling := spec.Backend.SessionScaling
		if scaling.MinDllamas > 0 {
			args = append(args, fmt.Sprintf("--backend-session-min-dllamas=%d", scaling.MinDllamas))
		}
		if scaling.MaxDllamas > 0 {
			args = append(args, fmt.Sprintf("--backend-session-max-dllamas=%d", scaling.MaxDllamas))
		}
		if scaling.ScaleUpBacklog > 0 {
			args = append(args, fmt.Sprintf("--backend-session-scale-up-backlog=%d", scaling.ScaleUpBacklog))
		}
		if scaling.ScaleDownIdleSeconds > 0 {
			args = append(args, fmt.Sprintf("--backend-session-scale-down-idle-seconds=%d", scaling.ScaleDownIdleSeconds))
		}
	}
	if spec.Backend.HashSecret != "" {
		args = append(args, fmt.Sprintf("--backend-hash-secret=%s", spec.Backend.HashSecret))
	}
	if spec.Backend.AllowAnonymous {
		args = append(args, "--backend-allow-anonymous")
	}

	return args
}

func servicePort(spec *v1.IngressSpec) int32 {
	if spec.Service != nil && spec.Service.Port != 0 {
		return spec.Service.Port
	}
	return 8082
}

func pathTypePtr(value string) *networkingv1.PathType {
	if value == "" {
		pt := networkingv1.PathTypeImplementationSpecific
		return &pt
	}
	switch strings.ToLower(value) {
	case "exact":
		pt := networkingv1.PathTypeExact
		return &pt
	case "prefix":
		pt := networkingv1.PathTypePrefix
		return &pt
	default:
		pt := networkingv1.PathTypeImplementationSpecific
		return &pt
	}
}

func isResourceRequirementsEmpty(req corev1.ResourceRequirements) bool {
	return len(req.Requests) == 0 && len(req.Limits) == 0
}
