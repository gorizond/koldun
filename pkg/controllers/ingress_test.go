package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	appscontroller "github.com/rancher/wrangler/v3/pkg/generated/controllers/apps/v1"
	corecontroller "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func newIngressSpec() *v1.IngressSpec {
	conversationTTL := metav1.Duration{Duration: 30 * time.Second}
	responseTimeout := metav1.Duration{Duration: 45 * time.Second}

	return &v1.IngressSpec{
		Backend: v1.IngressBackendSpec{
			Image:                   "ghcr.io/gorizond/backend:latest",
			RootImage:               "ghcr.io/gorizond/root:latest",
			WorkerImage:             "ghcr.io/gorizond/worker:latest",
			DispatcherMetricsListen: ":9090",
			ReplicaPower:            2,
			HashSecret:              "topsecret",
			AllowAnonymous:          true,
			NATS: v1.IngressNATSConfig{
				URL:                "nats://example:4222",
				ConversationBucket: "conversation-bucket",
				ModelsBucket:       "models-bucket",
				TokensBucket:       "tokens-bucket",
				ModelPrefix:        "models",
				TokenPrefix:        "tokens",
			},
			ConversationTTL: &conversationTTL,
			ResponseTimeout: &responseTimeout,
			SessionScaling: &v1.IngressSessionScalingSpec{
				MinDllamas:           1,
				MaxDllamas:           3,
				ScaleUpBacklog:       5,
				ScaleDownIdleSeconds: 120,
			},
		},
		Route: v1.IngressRouteSpec{
			Host: "chat.example.com",
		},
	}
}

func newIngress() *v1.Ingress {
	spec := newIngressSpec()
	return &v1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chat-backend",
			Namespace: "testing",
		},
		Spec: *spec,
	}
}

type fakeKoldControllers struct {
	ingress generic.ControllerInterface[*v1.Ingress, *v1.IngressList]
}

func (f fakeKoldControllers) Dllama() generic.ControllerInterface[*v1.Dllama, *v1.DllamaList] {
	return nil
}

func (f fakeKoldControllers) Model() generic.ControllerInterface[*v1.Model, *v1.ModelList] {
	return nil
}

func (f fakeKoldControllers) Root() generic.ControllerInterface[*v1.Root, *v1.RootList] {
	return nil
}

func (f fakeKoldControllers) Worker() generic.ControllerInterface[*v1.Worker, *v1.WorkerList] {
	return nil
}

func (f fakeKoldControllers) Ingress() generic.ControllerInterface[*v1.Ingress, *v1.IngressList] {
	return f.ingress
}

func (f fakeKoldControllers) Session() generic.ControllerInterface[*v1.Session, *v1.SessionList] {
	return nil
}

type fakeAppsControllers struct {
	deployment appscontroller.DeploymentController
}

func (f fakeAppsControllers) DaemonSet() appscontroller.DaemonSetController {
	return nil
}

func (f fakeAppsControllers) Deployment() appscontroller.DeploymentController {
	return f.deployment
}

func (f fakeAppsControllers) StatefulSet() appscontroller.StatefulSetController {
	return nil
}

type fakeCoreControllers struct {
	secret  corecontroller.SecretController
	service corecontroller.ServiceController
}

func (f fakeCoreControllers) ConfigMap() corecontroller.ConfigMapController {
	return nil
}

func (f fakeCoreControllers) Endpoints() corecontroller.EndpointsController {
	return nil
}

func (f fakeCoreControllers) Event() corecontroller.EventController {
	return nil
}

func (f fakeCoreControllers) Namespace() corecontroller.NamespaceController {
	return nil
}

func (f fakeCoreControllers) Node() corecontroller.NodeController {
	return nil
}

func (f fakeCoreControllers) PersistentVolume() corecontroller.PersistentVolumeController {
	return nil
}

func (f fakeCoreControllers) PersistentVolumeClaim() corecontroller.PersistentVolumeClaimController {
	return nil
}

func (f fakeCoreControllers) Pod() corecontroller.PodController {
	return nil
}

func (f fakeCoreControllers) Secret() corecontroller.SecretController {
	return f.secret
}

func (f fakeCoreControllers) Service() corecontroller.ServiceController {
	return f.service
}

func (f fakeCoreControllers) ServiceAccount() corecontroller.ServiceAccountController {
	return nil
}

func TestRegisterIngressControllerRegistersHandlers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingresses := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)
	deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)
	services := genericfake.NewMockControllerInterface[*corev1.Service, *corev1.ServiceList](ctrl)

	ingresses.EXPECT().
		OnChange(gomock.Any(), "koldun-ingress-controller", gomock.AssignableToTypeOf(generic.ObjectHandler[*v1.Ingress](nil)))
	ingresses.EXPECT().
		OnRemove(gomock.Any(), "koldun-ingress-controller", gomock.AssignableToTypeOf(generic.ObjectHandler[*v1.Ingress](nil)))
	deployments.EXPECT().
		OnChange(gomock.Any(), "koldun-ingress-deployment-watch", gomock.AssignableToTypeOf(generic.ObjectHandler[*appsv1.Deployment](nil)))
	services.EXPECT().
		OnChange(gomock.Any(), "koldun-ingress-service-watch", gomock.AssignableToTypeOf(generic.ObjectHandler[*corev1.Service](nil)))

	manager := &Manager{
		apply: &fakeapply.FakeApply{},
		Kold:  fakeKoldControllers{ingress: ingresses},
		Apps:  fakeAppsControllers{deployment: deployments},
		Core:  fakeCoreControllers{service: services},
	}

	err := registerIngressController(context.Background(), manager)
	require.NoError(t, err)
}

func TestIngressOnChangeScenarios(t *testing.T) {
	t.Run("nil object returns nil", func(t *testing.T) {
		handler := &ingressHandler{}

		obj, err := handler.onChange("testing/chat", nil)
		require.NoError(t, err)
		require.Nil(t, obj)
	})

	t.Run("deletion timestamp skips reconciliation", func(t *testing.T) {
		handler := &ingressHandler{}
		ing := newIngress()
		now := metav1.Now()
		ing.DeletionTimestamp = &now

		obj, err := handler.onChange("testing/chat", ing)
		require.NoError(t, err)
		require.Same(t, ing, obj)
	})

	t.Run("ensureResources failure bubbles up", func(t *testing.T) {
		handler := &ingressHandler{apply: &fakeapply.FakeApply{}}
		ing := newIngress()
		ing.Spec.Backend.Image = ""

		obj, err := handler.onChange("testing/chat", ing)
		require.Error(t, err)
		require.Same(t, ing, obj)
	})

	t.Run("successful reconciliation applies resources and updates status", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		applySpy := &fakeapply.FakeApply{}
		deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)
		deploymentsCache := genericfake.NewMockCacheInterface[*appsv1.Deployment](ctrl)
		ingresses := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

		handler := &ingressHandler{
			apply:       applySpy,
			deployments: deployments,
			ingresses:   ingresses,
		}

		deployments.EXPECT().Cache().Return(deploymentsCache)
		deploymentsCache.EXPECT().
			Get("testing", "chat-backend-backend").
			Return(&appsv1.Deployment{Status: appsv1.DeploymentStatus{ReadyReplicas: 1}}, nil)
		ingresses.EXPECT().
			UpdateStatus(gomock.AssignableToTypeOf(&v1.Ingress{})).
			DoAndReturn(func(obj *v1.Ingress) (*v1.Ingress, error) {
				cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
				require.NotNil(t, cond)
				require.Equal(t, metav1.ConditionTrue, cond.Status)
				return obj, nil
			})

		ing := newIngress()
		ing.Generation = 5

		obj, err := handler.onChange("testing/chat", ing)
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, 1, applySpy.Count)
	})
}

func TestIngressOnRelatedDeployment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingresses := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)
	handler := &ingressHandler{ingresses: ingresses}

	t.Run("nil object", func(t *testing.T) {
		obj, err := handler.onRelatedDeployment("ns/key", nil)
		require.NoError(t, err)
		require.Nil(t, obj)
	})

	t.Run("non backend component", func(t *testing.T) {
		deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}
		obj, err := handler.onRelatedDeployment("ns/key", deploy)
		require.NoError(t, err)
		require.Same(t, deploy, obj)
	})

	t.Run("backend component without name label", func(t *testing.T) {
		ingresses.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					labelComponent: componentBackend,
				},
			},
		}
		obj, err := handler.onRelatedDeployment("ns/key", deploy)
		require.NoError(t, err)
		require.Same(t, deploy, obj)
	})

	t.Run("enqueue backend ingress", func(t *testing.T) {
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "worker",
				Namespace: "testing",
				Labels: map[string]string{
					labelComponent:   componentBackend,
					labelBackendName: "chat",
				},
			},
		}
		ingresses.EXPECT().Enqueue("testing", "chat")

		obj, err := handler.onRelatedDeployment("ns/key", deploy)
		require.NoError(t, err)
		require.Same(t, deploy, obj)
	})
}

func TestIngressOnRelatedService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingresses := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)
	handler := &ingressHandler{ingresses: ingresses}

	t.Run("nil object", func(t *testing.T) {
		obj, err := handler.onRelatedService("ns/key", nil)
		require.NoError(t, err)
		require.Nil(t, obj)
	})

	t.Run("non backend component", func(t *testing.T) {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}
		obj, err := handler.onRelatedService("ns/key", svc)
		require.NoError(t, err)
		require.Same(t, svc, obj)
	})

	t.Run("enqueue backend ingress", func(t *testing.T) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backend",
				Namespace: "testing",
				Labels: map[string]string{
					labelComponent:   componentBackend,
					labelBackendName: "chat",
				},
			},
		}
		ingresses.EXPECT().Enqueue("testing", "chat")

		obj, err := handler.onRelatedService("ns/key", svc)
		require.NoError(t, err)
		require.Same(t, svc, obj)
	})

	t.Run("backend component without name is ignored", func(t *testing.T) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "testing",
				Labels: map[string]string{
					labelComponent: componentBackend,
				},
			},
		}

		obj, err := handler.onRelatedService("ns/key", svc)
		require.NoError(t, err)
		require.Same(t, svc, obj)
	})
}

func TestBackendArgs_DefaultsAndOptionalFlags(t *testing.T) {
	spec := newIngressSpec()
	args := backendArgs(spec, 9090)

	expected := []string{
		"--mode=backend",
		"--backend-listen=:9090",
		"--backend-namespace=$(POD_NAMESPACE)",
		"--backend-root-image=ghcr.io/gorizond/root:latest",
		"--backend-worker-image=ghcr.io/gorizond/worker:latest",
		"--backend-replica-power=2",
		"--backend-session-dispatcher-image=ghcr.io/gorizond/backend:latest",
		"--backend-session-dispatcher-metrics-listen=:9090",
		"--backend-nats-url=nats://example:4222",
		"--backend-conversation-bucket=conversation-bucket",
		"--backend-models-bucket=models-bucket",
		"--backend-tokens-bucket=tokens-bucket",
		"--backend-model-prefix=models",
		"--backend-token-prefix=tokens",
		"--backend-ttl-prefix=nats_ttl_",
		"--backend-conversation-ttl=30s",
		"--backend-response-timeout=45s",
		"--backend-session-min-dllamas=1",
		"--backend-session-max-dllamas=3",
		"--backend-session-scale-up-backlog=5",
		"--backend-session-scale-down-idle-seconds=120",
		"--backend-hash-secret=topsecret",
		"--backend-allow-anonymous",
	}

	require.Equal(t, expected, args)
}

func containsArg(args []string, prefix string) bool {
	for _, arg := range args {
		if strings.HasPrefix(arg, prefix) {
			return true
		}
	}
	return false
}

func TestBackendArgs_OverridesAndOmissions(t *testing.T) {
	spec := newIngressSpec()
	spec.Backend.ReplicaPower = 0
	spec.Backend.DispatcherImage = "ghcr.io/gorizond/dispatcher:v1.2.3"
	spec.Backend.NATS.TTLPrefix = "ttl_custom_"
	spec.Backend.ConversationTTL = nil
	spec.Backend.ResponseTimeout = nil
	spec.Backend.SessionScaling = &v1.IngressSessionScalingSpec{}
	spec.Backend.HashSecret = ""
	spec.Backend.AllowAnonymous = false

	args := backendArgs(spec, 8082)

	require.Contains(t, args, "--backend-session-dispatcher-image=ghcr.io/gorizond/dispatcher:v1.2.3")
	require.Contains(t, args, "--backend-ttl-prefix=ttl_custom_")
	require.False(t, containsArg(args, "--backend-replica-power"))
	require.False(t, containsArg(args, "--backend-conversation-ttl"))
	require.False(t, containsArg(args, "--backend-response-timeout"))
	require.False(t, containsArg(args, "--backend-session-min-dllamas"))
	require.False(t, containsArg(args, "--backend-session-max-dllamas"))
	require.False(t, containsArg(args, "--backend-session-scale-up-backlog"))
	require.False(t, containsArg(args, "--backend-session-scale-down-idle-seconds"))
	require.False(t, containsArg(args, "--backend-hash-secret"))
	require.False(t, containsArg(args, "--backend-allow-anonymous"))
}

func TestServicePort(t *testing.T) {
	spec := newIngressSpec()
	require.Equal(t, int32(8082), servicePort(spec))

	spec.Service = &v1.IngressServiceSpec{Port: 9090}
	require.Equal(t, int32(9090), servicePort(spec))
}

func TestDesiredBackendDeployment(t *testing.T) {
	ing := newIngress()
	ing.Spec.Backend.Image = "ghcr.io/gorizond/backend:stable"
	ing.Spec.Backend.DispatcherImage = "ghcr.io/gorizond/dispatcher:stable"
	ing.Spec.Backend.ImagePullPolicy = string(corev1.PullAlways)
	ing.Spec.Backend.ExtraArgs = []string{"--log-level=debug", "--metrics-bind=:9000"}
	ing.Spec.Backend.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}

	deployment := desiredBackendDeployment(ing, "chat-backend")
	require.Equal(t, "chat-backend", deployment.Name)
	require.Equal(t, "testing", deployment.Namespace)
	require.NotNil(t, deployment.Spec.Replicas)
	require.Equal(t, int32(1), *deployment.Spec.Replicas)
	require.Equal(t, map[string]string{
		labelComponent:   componentBackend,
		labelBackendName: ing.Name,
	}, deployment.Spec.Template.Labels)

	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
	container := deployment.Spec.Template.Spec.Containers[0]
	require.Equal(t, "backend", container.Name)
	require.Equal(t, "ghcr.io/gorizond/backend:stable", container.Image)
	require.Equal(t, corev1.PullAlways, container.ImagePullPolicy)
	require.True(t, strings.HasPrefix(container.Args[0], "--mode=backend"))
	require.True(t, containsArg(container.Args, "--backend-session-dispatcher-image=ghcr.io/gorizond/dispatcher:stable"))
	require.True(t, containsArg(container.Args, "--backend-session-dispatcher-metrics-listen=:9090"))
	require.Equal(t, ing.Spec.Backend.ExtraArgs, container.Args[len(container.Args)-len(ing.Spec.Backend.ExtraArgs):])
	require.Equal(t, ing.Spec.Backend.Resources, container.Resources)
}

func TestEnsureResourcesAppliesExpectedObjects(t *testing.T) {
	ing := newIngress()
	applySpy := &fakeapply.FakeApply{}
	handler := &ingressHandler{apply: applySpy}

	require.NoError(t, handler.ensureResources(ing))
	require.Len(t, applySpy.Objects, 1)
	objects := applySpy.Objects[0].All()
	require.Len(t, objects, 3)

	var (
		deployment *appsv1.Deployment
		service    *corev1.Service
		route      *networkingv1.Ingress
	)

	for _, obj := range objects {
		switch typed := obj.(type) {
		case *appsv1.Deployment:
			deployment = typed
		case *corev1.Service:
			service = typed
		case *networkingv1.Ingress:
			route = typed
		default:
			t.Fatalf("unexpected object type %T", obj)
		}
	}

	expectedName := fmt.Sprintf("%s-backend", ing.Name)

	require.NotNil(t, deployment)
	require.Equal(t, expectedName, deployment.Name)
	require.Equal(t, ing.Namespace, deployment.Namespace)
	require.NotNil(t, deployment.Spec.Template.Spec.Containers)
	require.Equal(t, ing.Spec.Backend.Image, deployment.Spec.Template.Spec.Containers[0].Image)

	require.NotNil(t, service)
	require.Equal(t, expectedName, service.Name)
	require.Equal(t, int32(8082), service.Spec.Ports[0].Port)

	require.NotNil(t, route)
	require.Equal(t, expectedName, route.Name)
	require.Equal(t, ing.Spec.Route.Host, route.Spec.Rules[0].Host)
}

func TestEnsureResourcesValidatesSpec(t *testing.T) {
	ing := newIngress()
	ing.Spec.Backend.Image = ""
	handler := &ingressHandler{apply: &fakeapply.FakeApply{}}

	err := handler.ensureResources(ing)
	require.EqualError(t, err, "backend.image is required")
}

func TestEnsureStatusSetsBackendReadyCondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	deploymentsCache := genericfake.NewMockCacheInterface[*appsv1.Deployment](ctrl)
	deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &ingressHandler{
		deployments: deployments,
		ingresses:   ingressController,
	}

	deployments.EXPECT().Cache().Return(deploymentsCache)
	backendName := fmt.Sprintf("%s-backend", "chat-backend")
	deploymentsCache.EXPECT().Get("testing", backendName).Return(&appsv1.Deployment{Status: appsv1.DeploymentStatus{ReadyReplicas: 1}}, nil)
	ingressController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Ingress{})).DoAndReturn(func(obj *v1.Ingress) (*v1.Ingress, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionTrue, cond.Status)
		require.Equal(t, "BackendReady", cond.Reason)
		require.Equal(t, obj.Generation, obj.Status.ObservedGeneration)
		require.Equal(t, backendName, obj.Status.BackendServiceName)
		require.Equal(t, backendName, obj.Status.IngressName)
		return obj, nil
	})

	ing := newIngress()
	ing.Generation = 3

	updated, err := handler.ensureStatus(ing)
	require.NoError(t, err)
	require.NotNil(t, updated)
}

func TestEnsureStatusNoChangeReturnsOriginal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	deploymentsCache := genericfake.NewMockCacheInterface[*appsv1.Deployment](ctrl)
	deployments := genericfake.NewMockControllerInterface[*appsv1.Deployment, *appsv1.DeploymentList](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &ingressHandler{
		deployments: deployments,
		ingresses:   ingressController,
	}

	deployments.EXPECT().Cache().Return(deploymentsCache)
	backendName := fmt.Sprintf("%s-backend", "chat-backend")
	deploymentsCache.EXPECT().Get("testing", backendName).Return(&appsv1.Deployment{Status: appsv1.DeploymentStatus{ReadyReplicas: 0}}, nil)
	ingressController.EXPECT().UpdateStatus(gomock.Any()).Times(0)

	ing := newIngress()
	ing.Status.Conditions = []metav1.Condition{{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "BackendNotReady",
		Message: "backend deployment is not yet ready",
	}}
	ing.Status.ObservedGeneration = ing.Generation
	ing.Status.BackendServiceName = backendName
	ing.Status.IngressName = backendName

	returned, err := handler.ensureStatus(ing)
	require.NoError(t, err)
	require.Equal(t, ing, returned)
}

func TestDesiredBackendService(t *testing.T) {
	ing := newIngress()
	ing.Spec.Service = &v1.IngressServiceSpec{
		Type: string(corev1.ServiceTypeNodePort),
		Port: 9090,
	}

	service := desiredBackendService(ing, "chat-backend")
	require.Equal(t, "chat-backend", service.Name)
	require.Equal(t, corev1.ServiceTypeNodePort, service.Spec.Type)
	require.Equal(t, int32(9090), service.Spec.Ports[0].Port)
	require.Equal(t, map[string]string{
		labelComponent:   componentBackend,
		labelBackendName: ing.Name,
	}, service.Spec.Selector)
}

func TestDesiredKubernetesIngress(t *testing.T) {
	ing := newIngress()
	ing.Spec.Service = &v1.IngressServiceSpec{Port: 8443}
	ing.Spec.Route = v1.IngressRouteSpec{
		Host:             "chat.example.com",
		Path:             "/v1/chat",
		PathType:         "Prefix",
		IngressClassName: "nginx",
		Annotations: map[string]string{
			"nginx.ingress.kubernetes.io/rewrite-target": "/",
		},
		TLS: []v1.IngressTLSSpec{
			{
				SecretName: "chat-tls",
				Hosts:      []string{"chat.example.com"},
			},
		},
	}

	route := desiredKubernetesIngress(ing, "chat-backend", "chat-route")
	require.Equal(t, "chat-route", route.Name)
	require.Equal(t, "testing", route.Namespace)
	require.Equal(t, "chat.example.com", route.Spec.Rules[0].Host)
	require.Equal(t, "/v1/chat", route.Spec.Rules[0].IngressRuleValue.HTTP.Paths[0].Path)
	require.Equal(t, networkingv1.PathTypePrefix, *route.Spec.Rules[0].IngressRuleValue.HTTP.Paths[0].PathType)
	require.Equal(t, int32(8443), route.Spec.Rules[0].IngressRuleValue.HTTP.Paths[0].Backend.Service.Port.Number)
	require.NotNil(t, route.Spec.IngressClassName)
	require.Equal(t, "nginx", *route.Spec.IngressClassName)
	require.Equal(t, map[string]string{
		labelComponent:   componentBackend,
		labelBackendName: ing.Name,
	}, route.Labels)
	require.Equal(t, ing.Spec.Route.Annotations, route.Annotations)
	require.Len(t, route.Spec.TLS, 1)
	require.Equal(t, "chat-tls", route.Spec.TLS[0].SecretName)
	require.Equal(t, []string{"chat.example.com"}, route.Spec.TLS[0].Hosts)

	// Ensure TLS hosts were deep-copied.
	ing.Spec.Route.TLS[0].Hosts[0] = "mutated.example.com"
	require.Equal(t, []string{"chat.example.com"}, route.Spec.TLS[0].Hosts)
}

func TestPathTypePtr(t *testing.T) {
	require.Equal(t, networkingv1.PathTypeImplementationSpecific, *pathTypePtr(""))
	require.Equal(t, networkingv1.PathTypeExact, *pathTypePtr("exact"))
	require.Equal(t, networkingv1.PathTypePrefix, *pathTypePtr("PREFIX"))
	require.Equal(t, networkingv1.PathTypeImplementationSpecific, *pathTypePtr("unknown"))
}

func TestIsResourceRequirementsEmpty(t *testing.T) {
	require.True(t, isResourceRequirementsEmpty(corev1.ResourceRequirements{}))

	req := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	require.False(t, isResourceRequirementsEmpty(req))

	limits := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"),
		},
	}
	require.False(t, isResourceRequirementsEmpty(limits))
}

func TestValidateIngressSpec(t *testing.T) {
	require.NoError(t, validateIngressSpec(newIngressSpec()))

	tests := []struct {
		name    string
		mutate  func(*v1.IngressSpec)
		wantErr string
	}{
		{
			name:    "missing backend image",
			mutate:  func(spec *v1.IngressSpec) { spec.Backend.Image = " " },
			wantErr: "backend.image is required",
		},
		{
			name:    "missing root image",
			mutate:  func(spec *v1.IngressSpec) { spec.Backend.RootImage = "" },
			wantErr: "backend.rootImage is required",
		},
		{
			name:    "missing worker image",
			mutate:  func(spec *v1.IngressSpec) { spec.Backend.WorkerImage = "" },
			wantErr: "backend.workerImage is required",
		},
		{
			name:    "missing nats url",
			mutate:  func(spec *v1.IngressSpec) { spec.Backend.NATS.URL = "  " },
			wantErr: "backend.nats.url is required",
		},
		{
			name:    "missing route host",
			mutate:  func(spec *v1.IngressSpec) { spec.Route.Host = "" },
			wantErr: "route.host is required",
		},
		{
			name:    "negative replica power",
			mutate:  func(spec *v1.IngressSpec) { spec.Backend.ReplicaPower = -1 },
			wantErr: "backend.replicaPower must be >= 0",
		},
		{
			name: "invalid root memory ratio",
			mutate: func(spec *v1.IngressSpec) {
				spec.Backend.RootMemory = &v1.IngressRootMemorySpec{
					OverheadMaxRatio: pointer.Float64(-0.1),
				}
			},
			wantErr: "backend.rootMemory.overheadMaxRatio must be > 0",
		},
		{
			name: "negative min dllamas",
			mutate: func(spec *v1.IngressSpec) {
				spec.Backend.SessionScaling = &v1.IngressSessionScalingSpec{
					MinDllamas: -1,
				}
			},
			wantErr: "backend.sessionScaling.minDllamas must be >= 0",
		},
		{
			name: "negative max dllamas",
			mutate: func(spec *v1.IngressSpec) {
				spec.Backend.SessionScaling = &v1.IngressSessionScalingSpec{
					MaxDllamas: -1,
				}
			},
			wantErr: "backend.sessionScaling.maxDllamas must be >= 0",
		},
		{
			name: "max dllamas less than min",
			mutate: func(spec *v1.IngressSpec) {
				spec.Backend.SessionScaling = &v1.IngressSessionScalingSpec{
					MinDllamas: 2,
					MaxDllamas: 1,
				}
			},
			wantErr: "backend.sessionScaling.maxDllamas must be >= minDllamas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := newIngressSpec()
			tt.mutate(spec)
			err := validateIngressSpec(spec)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
