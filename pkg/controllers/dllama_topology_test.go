package controllers

import (
	"errors"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/pointer"
)

func TestEnsureTopologyCreatesRootAndWorker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	ingressesCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressesController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	ingressesController.EXPECT().Cache().Return(ingressesCache).AnyTimes()

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"},
		Status:     v1.ModelStatus{OutputPVCName: "model-pvc"},
	}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)

	ratio := pointer.Float64(1.25)
	ingress := &v1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "public", Namespace: "default"},
		Spec: v1.IngressSpec{
			Backend: v1.IngressBackendSpec{
				NATS:       v1.IngressNATSConfig{URL: "nats://ingress:4222"},
				RootMemory: &v1.IngressRootMemorySpec{OverheadMaxRatio: ratio},
			},
		},
	}
	ingressesCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{ingress}, nil).AnyTimes()
	ingressesCache.EXPECT().List("", gomock.Any()).Return([]*v1.Ingress{}, nil).AnyTimes()

	applySpy := &fakeapply.FakeApply{}

	handler := &dllamaHandler{
		apply:     applySpy,
		models:    modelsController,
		ingresses: ingressesController,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Labels: map[string]string{
				labelConversationHash: "hash-value",
				labelSessionName:      "chat-session",
			},
			Annotations: map[string]string{
				labelConversationHash:              "hash-annotation",
				annotationSessionQueuePrefix:       "sessions.",
				annotationSessionAssignmentsBucket: "assignments",
			},
		},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Kind: "Model", Name: "demo-model"},
			ReplicaPower: 1,
			RootImage:    "root:latest",
			WorkerImage:  "worker:latest",
		},
	}

	err := handler.ensureTopology(dllama)
	require.NoError(t, err)

	require.Len(t, applySpy.Objects, 1)
	appliedObjects := applySpy.Objects[0].All()
	require.Len(t, appliedObjects, 2)

	var root *v1.Root
	var worker *v1.Worker
	for _, obj := range appliedObjects {
		switch o := obj.(type) {
		case *v1.Root:
			root = o
		case *v1.Worker:
			worker = o
		default:
			t.Fatalf("unexpected object type %T", obj)
		}
	}

	require.NotNil(t, root)
	require.NotNil(t, worker)

	require.Equal(t, "demo-root", root.Name)
	require.Equal(t, "default", root.Namespace)
	require.Equal(t, "model-pvc", root.Spec.ModelRef)
	require.NotNil(t, root.Spec.NATS)
	require.Equal(t, "nats://ingress:4222", root.Spec.NATS.URL)
	require.NotNil(t, root.Spec.Memory)
	require.NotNil(t, root.Spec.Memory.OverheadMaxRatio)
	require.InDelta(t, *ratio, *root.Spec.Memory.OverheadMaxRatio, 0.0001)
	require.Equal(t, map[string]string{
		labelDllamaName:       "demo",
		labelComponent:        componentRoot,
		labelRootName:         "demo-root",
		labelModelName:        "demo-model",
		labelConversationHash: "hash-value",
		labelSessionName:      "chat-session",
	}, root.Labels)
	require.Equal(t, "hash-annotation", root.Annotations[labelConversationHash])
	require.Equal(t, "sessions.", root.Annotations[annotationSessionQueuePrefix])

	require.Equal(t, workerResourceName("demo"), worker.Name)
	require.Equal(t, "default", worker.Namespace)
	require.Equal(t, "model-pvc", worker.Spec.ModelRef)
	require.Equal(t, "worker:latest", worker.Spec.Image)
	require.Equal(t, "demo-root", worker.Spec.RootRef)
	require.NotNil(t, worker.Spec.NATS)
	require.Equal(t, "nats://ingress:4222", worker.Spec.NATS.URL)
	require.Equal(t, map[string]string{
		labelDllamaName:       "demo",
		labelComponent:        componentWorker,
		labelWorkerName:       workerResourceName("demo"),
		labelModelName:        "demo-model",
		labelConversationHash: "hash-value",
		labelSessionName:      "chat-session",
	}, worker.Labels)
	require.Equal(t, "hash-annotation", worker.Annotations[labelConversationHash])
}

func TestEnsureTopologySkipsWhenModelMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()

	modelsCache.EXPECT().Get("default", "missing-model").Return((*v1.Model)(nil), apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "models"}, "missing-model"))

	applySpy := &fakeapply.FakeApply{}

	handler := &dllamaHandler{
		apply:  applySpy,
		models: modelsController,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{Kind: "Model", Name: "missing-model"},
		},
	}

	err := handler.ensureTopology(dllama)
	require.NoError(t, err)
	require.Len(t, applySpy.Objects, 1)
	require.Len(t, applySpy.Objects[0].All(), 0)
}

func TestEnsureTopologySkipsWhenKindNotModel(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &dllamaHandler{
		apply: applySpy,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{Kind: "Service"},
		},
	}

	err := handler.ensureTopology(dllama)
	require.NoError(t, err)
	require.Len(t, applySpy.Objects, 1)
	require.Len(t, applySpy.Objects[0].All(), 0)
}

func TestEnsureTopologySkipsWhenAPIGroupMismatch(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &dllamaHandler{
		apply: applySpy,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{
				Kind:     "Model",
				Name:     "demo-model",
				APIGroup: "other.gorizond.io",
			},
		},
	}

	err := handler.ensureTopology(dllama)
	require.NoError(t, err)
	require.Len(t, applySpy.Objects, 1)
	require.Len(t, applySpy.Objects[0].All(), 0)
}

func TestEnsureTopologySkipsWhenModelNameMissing(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &dllamaHandler{
		apply: applySpy,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{
				Kind: "Model",
				Name: "   ",
			},
		},
	}

	err := handler.ensureTopology(dllama)
	require.NoError(t, err)
	require.Len(t, applySpy.Objects, 1)
	require.Len(t, applySpy.Objects[0].All(), 0)
}

func TestEnsureTopologyPropagatesLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	expectedErr := errors.New("model lookup failed")
	modelsCache.EXPECT().Get("default", "demo-model").Return((*v1.Model)(nil), expectedErr)

	handler := &dllamaHandler{
		apply:  &fakeapply.FakeApply{},
		models: modelsController,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{Kind: "Model", Name: "demo-model"},
		},
	}

	err := handler.ensureTopology(dllama)
	require.ErrorIs(t, err, expectedErr)
}

func TestEnsureTopologySkipsWhenModelNotReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"},
		Status:     v1.ModelStatus{},
	}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)

	applySpy := &fakeapply.FakeApply{}
	handler := &dllamaHandler{
		apply:  applySpy,
		models: modelsController,
	}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{Kind: "Model", Name: "demo-model"},
		},
	}

	err := handler.ensureTopology(dllama)
	require.NoError(t, err)
	require.Len(t, applySpy.Objects, 1)
	require.Len(t, applySpy.Objects[0].All(), 0)
}

func TestDesiredRootPrefersExplicitNATSAndLabelHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)
	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("models", gomock.Any()).Return([]*v1.Ingress{}, nil)
	ingressCache.EXPECT().List("", gomock.Any()).Return([]*v1.Ingress{}, nil)

	handler := &dllamaHandler{ingresses: ingressController}

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "models",
			Labels: map[string]string{
				labelConversationHash: "hash-from-label",
				labelSessionName:      "session-a",
			},
			Annotations: map[string]string{
				annotationSessionBacklogSubject: "sessions.backlog",
				annotationSessionStateStream:    "sessions.state",
			},
		},
		Spec: v1.DllamaSpec{
			ModelRef:    v1.ModelReference{Kind: "Model", Name: "demo-model"},
			RootImage:   "root:latest",
			WorkerImage: "worker:latest",
			NATS: &v1.DllamaNATSConfig{
				URL:               "nats://explicit:4222",
				CredentialsSecret: &v1.SecretReference{Name: "nats-creds"},
			},
		},
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "models"},
		Status:     v1.ModelStatus{OutputPVCName: "model-pvc"},
	}

	root := handler.desiredRoot(dllama, model)
	require.NotNil(t, root)
	require.Equal(t, "demo-root", root.Name)
	require.Equal(t, "models", root.Namespace)
	require.Equal(t, "hash-from-label", root.Annotations[labelConversationHash])
	require.Equal(t, "hash-from-label", root.Labels[labelConversationHash])
	require.Equal(t, "session-a", root.Labels[labelSessionName])
	require.Equal(t, "sessions.backlog", root.Annotations[annotationSessionBacklogSubject])
	require.Equal(t, "sessions.state", root.Annotations[annotationSessionStateStream])
	require.NotNil(t, root.Spec.NATS)
	require.Equal(t, "nats://explicit:4222", root.Spec.NATS.URL)
	require.NotNil(t, root.Spec.NATS.CredentialsSecret)
}

func TestEnsureStatusMarksTopologyReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	rootsCache := genericfake.NewMockCacheInterface[*v1.Root](ctrl)
	rootsController := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
	workersCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workersController := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	stsController := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	rootsController.EXPECT().Cache().Return(rootsCache).AnyTimes()
	workersController.EXPECT().Cache().Return(workersCache).AnyTimes()
	stsController.EXPECT().Cache().Return(stsCache).AnyTimes()

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"},
		Status:     v1.ModelStatus{OutputPVCName: "model-pvc"},
	}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)

	root := &v1.Root{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-root", Namespace: "default"},
		Status: v1.RootStatus{
			Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}},
		},
	}
	rootsCache.EXPECT().Get("default", "demo-root").Return(root, nil)

	workerName := workerResourceName("demo")
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "default"}}
	workersCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Worker{worker}, nil)

	stsCache.EXPECT().Get("default", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Kind: "Model", Name: "demo-model"},
			ReplicaPower: 1,
		},
		Status: v1.DllamaStatus{},
	}
	dllama.Generation = 3

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionTrue, cond.Status)
		require.Equal(t, "TopologyReady", cond.Reason)
		require.Equal(t, "Root and workers are ready", cond.Message)
		require.True(t, obj.Status.ReadyRoot)
		require.Equal(t, int32(1), obj.Status.ReadyWorkers)
		require.Equal(t, int64(3), obj.Status.ObservedGeneration)
		return obj, nil
	})

	handler := &dllamaHandler{
		models:       modelsController,
		roots:        rootsController,
		workers:      workersController,
		statefulsets: stsController,
		dllamas:      dllamaController,
	}

	updated, err := handler.ensureStatus(dllama)
	require.NoError(t, err)
	require.NotNil(t, updated)
}

func TestEnsureStatusModelNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()

	modelsCache.EXPECT().Get("default", "demo-model").Return((*v1.Model)(nil), apierrors.NewNotFound(schema.GroupResource{Group: v1.GroupName, Resource: "models"}, "demo-model"))

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef: v1.ModelReference{Kind: "Model", Name: "demo-model"},
		},
		Status: v1.DllamaStatus{},
	}

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionFalse, cond.Status)
		require.Equal(t, "ModelNotFound", cond.Reason)
		require.Contains(t, cond.Message, "demo-model")
		return obj, nil
	})

	handler := &dllamaHandler{
		models:  modelsController,
		dllamas: dllamaController,
	}

	_, err := handler.ensureStatus(dllama)
	require.NoError(t, err)
}

func TestEnsureStatusInvalidModelKind(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		dllamas: dllamaController,
	}

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, "InvalidModelReference", cond.Reason)
		require.Contains(t, cond.Message, "kind")
		return obj, nil
	})

	_, err := handler.ensureStatus(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "ConfigMap"}},
	})
	require.NoError(t, err)
}

func TestEnsureStatusInvalidAPIGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		dllamas: dllamaController,
	}

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, "InvalidModelReference", cond.Reason)
		require.Contains(t, cond.Message, v1.GroupName)
		return obj, nil
	})

	_, err := handler.ensureStatus(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{ModelRef: v1.ModelReference{
			Kind:     "Model",
			Name:     "model",
			APIGroup: "other.group",
		}},
	})
	require.NoError(t, err)
}

func TestEnsureStatusMissingModelName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		dllamas: dllamaController,
	}

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, "InvalidModelReference", cond.Reason)
		require.Contains(t, cond.Message, "name")
		return obj, nil
	})

	_, err := handler.ensureStatus(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "Model", Name: " "}},
	})
	require.NoError(t, err)
}

func TestEnsureStatusModelLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		models:  modelsController,
		dllamas: dllamaController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	expectedErr := errors.New("cache failure")
	modelsCache.EXPECT().Get("default", "demo-model").Return((*v1.Model)(nil), expectedErr)

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, "ModelLookupFailed", cond.Reason)
		require.Contains(t, cond.Message, "cache failure")
		return obj, nil
	})

	_, err := handler.ensureStatus(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{ModelRef: v1.ModelReference{
			Kind: "Model",
			Name: "demo-model",
		}},
	})
	require.NoError(t, err)
}

func TestEnsureStatusModelWithoutPVC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		models:  modelsController,
		dllamas: dllamaController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	modelsCache.EXPECT().Get("default", "demo-model").Return(&v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"},
		Status:     v1.ModelStatus{},
	}, nil)

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, "ModelNotReady", cond.Reason)
		return obj, nil
	})

	_, err := handler.ensureStatus(&v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{ModelRef: v1.ModelReference{
			Kind: "Model",
			Name: "demo-model",
		}},
	})
	require.NoError(t, err)
}

func TestOnRelatedModelEnqueuesMatchingDllamas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}

	dllamasController.EXPECT().Cache().Return(dllamasCache)

	matching := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "ns"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "Model", Name: "model"}},
	}
	nonMatching := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "beta", Namespace: "ns"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "Model", Name: "other"}},
	}

	dllamasCache.EXPECT().List("", gomock.Any()).Return([]*v1.Dllama{matching, nonMatching}, nil)
	dllamasController.EXPECT().Enqueue("ns", "alpha")

	_, err := handler.onRelatedModel("", &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "ns"}})
	require.NoError(t, err)
}

func TestOnRelatedModelUsesKeyWhenObjectNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}

	dllamasController.EXPECT().Cache().Return(dllamasCache)
	matches := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "ns"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "Model", Name: "model"}},
	}
	dllamasCache.EXPECT().List("", gomock.Any()).Return([]*v1.Dllama{matches}, nil)
	dllamasController.EXPECT().Enqueue("ns", "alpha")

	_, err := handler.onRelatedModel("ns/model", nil)
	require.NoError(t, err)
}

func TestOnRelatedModelPropagatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}

	expectedErr := errors.New("cache failure")

	dllamasController.EXPECT().Cache().Return(dllamasCache)
	dllamasCache.EXPECT().List("", gomock.Any()).Return(nil, expectedErr)

	_, err := handler.onRelatedModel("", &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "ns"}})
	require.ErrorIs(t, err, expectedErr)
}

func TestOnRelatedRootEnqueuesDllama(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}
	root := &v1.Root{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Labels: map[string]string{labelDllamaName: "demo"}}}

	dllamasController.EXPECT().Enqueue("ns", "demo")

	returned, err := handler.onRelatedRoot("", root)
	require.NoError(t, err)
	require.Equal(t, root, returned)
}

func TestOnRelatedRootIgnoresWithoutLabel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}
	root := &v1.Root{ObjectMeta: metav1.ObjectMeta{Namespace: "ns"}}

	returned, err := handler.onRelatedRoot("", root)
	require.NoError(t, err)
	require.Equal(t, root, returned)
}

func TestOnRelatedWorkerEnqueuesDllama(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}
	worker := &v1.Worker{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Labels: map[string]string{labelDllamaName: "demo"}}}

	dllamasController.EXPECT().Enqueue("ns", "demo")

	returned, err := handler.onRelatedWorker("", worker)
	require.NoError(t, err)
	require.Equal(t, worker, returned)
}

func TestOnRelatedIngressTriggersAllDllamas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dllamasCache := genericfake.NewMockCacheInterface[*v1.Dllama](ctrl)
	dllamasController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{dllamas: dllamasController}

	dllamasController.EXPECT().Cache().Return(dllamasCache)
	dllamaA := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}
	dllamaB := &v1.Dllama{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"}}
	dllamasCache.EXPECT().List("", gomock.Any()).Return([]*v1.Dllama{dllamaA, dllamaB}, nil)
	dllamasController.EXPECT().Enqueue("ns", "a")
	dllamasController.EXPECT().Enqueue("default", "b")

	_, err := handler.onRelatedIngress("", &v1.Ingress{})
	require.NoError(t, err)
}

func TestGetIngressNatsURLClusterFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{}, nil)
	ingressCache.EXPECT().List("", gomock.Any()).Return([]*v1.Ingress{{
		Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{NATS: v1.IngressNATSConfig{URL: "nats://fallback:4222"}}},
	}}, nil)

	url := handler.getIngressNatsURL(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.Equal(t, "nats://fallback:4222", url)
}

func TestGetIngressNatsURLSameNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{
		{Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{NATS: v1.IngressNATSConfig{URL: "nats://local:4222"}}}},
	}, nil)

	url := handler.getIngressNatsURL(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.Equal(t, "nats://local:4222", url)
}

func TestGetIngressNatsURLError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return(nil, errors.New("list failed"))

	url := handler.getIngressNatsURL(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.Empty(t, url)
}

func TestGetIngressNatsURLSkipsEmptyEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{
		{Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{NATS: v1.IngressNATSConfig{URL: ""}}}},
		{Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{NATS: v1.IngressNATSConfig{URL: "nats://usable:4222"}}}},
	}, nil)

	url := handler.getIngressNatsURL(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.Equal(t, "nats://usable:4222", url)
}

func TestGetIngressNatsURLClusterListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{}, nil)
	ingressCache.EXPECT().List("", gomock.Any()).Return(nil, errors.New("boom"))

	url := handler.getIngressNatsURL(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.Empty(t, url)
}

func TestGetIngressNatsURLNoMatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{{}}, nil)
	ingressCache.EXPECT().List("", gomock.Any()).Return([]*v1.Ingress{{}}, nil)

	url := handler.getIngressNatsURL(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.Empty(t, url)
}

func TestGetIngressRootOverheadMaxRatioClusterFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ingressCache := genericfake.NewMockCacheInterface[*v1.Ingress](ctrl)
	ingressController := genericfake.NewMockControllerInterface[*v1.Ingress, *v1.IngressList](ctrl)

	handler := &dllamaHandler{ingresses: ingressController}

	ingressController.EXPECT().Cache().Return(ingressCache).AnyTimes()
	ingressCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Ingress{}, nil)
	val := pointer.Float64(1.1)
	ingressCache.EXPECT().List("", gomock.Any()).Return([]*v1.Ingress{{
		Spec: v1.IngressSpec{Backend: v1.IngressBackendSpec{RootMemory: &v1.IngressRootMemorySpec{OverheadMaxRatio: val}}},
	}}, nil)

	ratio := handler.getIngressRootOverheadMaxRatio(&v1.Dllama{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
	require.NotNil(t, ratio)
	require.InDelta(t, *val, *ratio, 0.0001)
}

func TestEnsureStatusTopologyNotReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	rootsCache := genericfake.NewMockCacheInterface[*v1.Root](ctrl)
	rootsController := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
	workersCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workersController := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	stsController := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		models:       modelsController,
		roots:        rootsController,
		workers:      workersController,
		statefulsets: stsController,
		dllamas:      dllamaController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	rootsController.EXPECT().Cache().Return(rootsCache).AnyTimes()
	workersController.EXPECT().Cache().Return(workersCache).AnyTimes()
	stsController.EXPECT().Cache().Return(stsCache).AnyTimes()

	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"}, Status: v1.ModelStatus{OutputPVCName: "pvc"}}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)

	root := &v1.Root{ObjectMeta: metav1.ObjectMeta{Name: "demo-root", Namespace: "default"}, Status: v1.RootStatus{Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionFalse}}}}
	rootsCache.EXPECT().Get("default", "demo-root").Return(root, nil)

	workerName := workerResourceName("demo")
	workersCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Worker{{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "default"}}}, nil)
	stsCache.EXPECT().Get("default", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 0}}, nil)

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Kind: "Model", Name: "demo-model"},
			ReplicaPower: 2,
		},
	}

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionFalse, cond.Status)
		require.Equal(t, "TopologyNotReady", cond.Reason)
		require.Equal(t, "Root and worker resources are not ready", cond.Message)
		require.False(t, obj.Status.ReadyRoot)
		require.Equal(t, int32(0), obj.Status.ReadyWorkers)
		return obj, nil
	})

	_, err := handler.ensureStatus(dllama)
	require.NoError(t, err)
}

func TestEnsureStatusReturnsOriginalWhenUnchanged(t *testing.T) {
	handler := &dllamaHandler{}
	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "Service"}},
		Status: v1.DllamaStatus{
			Conditions: []metav1.Condition{{
				Type:    conditionReady,
				Status:  metav1.ConditionFalse,
				Reason:  "InvalidModelReference",
				Message: "spec.modelRef.kind must be Model",
			}},
		},
	}

	result, err := handler.ensureStatus(dllama)
	require.NoError(t, err)
	require.Equal(t, dllama, result)
}

func TestEnsureStatusWorkerListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	rootsCache := genericfake.NewMockCacheInterface[*v1.Root](ctrl)
	rootsController := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
	workersCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workersController := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)

	handler := &dllamaHandler{
		models:  modelsController,
		roots:   rootsController,
		workers: workersController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	rootsController.EXPECT().Cache().Return(rootsCache).AnyTimes()
	workersController.EXPECT().Cache().Return(workersCache).AnyTimes()

	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"}, Status: v1.ModelStatus{OutputPVCName: "pvc"}}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)
	rootsCache.EXPECT().Get("default", "demo-root").Return(&v1.Root{Status: v1.RootStatus{Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}}}}, nil)
	workersCache.EXPECT().List("default", gomock.Any()).Return(nil, errors.New("list failure"))

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       v1.DllamaSpec{ModelRef: v1.ModelReference{Kind: "Model", Name: "demo-model"}},
	}

	result, err := handler.ensureStatus(dllama)
	require.Error(t, err)
	require.EqualError(t, err, "list failure")
	require.Equal(t, dllama, result)
}

func TestEnsureStatusReadyReplicasError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	rootsCache := genericfake.NewMockCacheInterface[*v1.Root](ctrl)
	rootsController := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
	workersCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workersController := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	stsController := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)

	handler := &dllamaHandler{
		models:       modelsController,
		roots:        rootsController,
		workers:      workersController,
		statefulsets: stsController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	rootsController.EXPECT().Cache().Return(rootsCache).AnyTimes()
	workersController.EXPECT().Cache().Return(workersCache).AnyTimes()
	stsController.EXPECT().Cache().Return(stsCache).AnyTimes()

	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"}, Status: v1.ModelStatus{OutputPVCName: "pvc"}}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)
	rootsCache.EXPECT().Get("default", "demo-root").Return(&v1.Root{Status: v1.RootStatus{Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}}}}, nil)
	workerName := workerResourceName("demo")
	workersCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Worker{{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "default"}}}, nil)
	stsCache.EXPECT().Get("default", workerName).Return(nil, errors.New("sts boom"))

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Kind: "Model", Name: "demo-model"},
			ReplicaPower: 2,
		},
	}

	result, err := handler.ensureStatus(dllama)
	require.Error(t, err)
	require.EqualError(t, err, "sts boom")
	require.Equal(t, dllama, result)
}

func TestEnsureStatusZeroReplicaPowerDefaultsToOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	modelsCache := genericfake.NewMockCacheInterface[*v1.Model](ctrl)
	modelsController := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	rootsCache := genericfake.NewMockCacheInterface[*v1.Root](ctrl)
	rootsController := genericfake.NewMockControllerInterface[*v1.Root, *v1.RootList](ctrl)
	workersCache := genericfake.NewMockCacheInterface[*v1.Worker](ctrl)
	workersController := genericfake.NewMockControllerInterface[*v1.Worker, *v1.WorkerList](ctrl)
	stsCache := genericfake.NewMockCacheInterface[*appsv1.StatefulSet](ctrl)
	stsController := genericfake.NewMockControllerInterface[*appsv1.StatefulSet, *appsv1.StatefulSetList](ctrl)
	dllamaController := genericfake.NewMockControllerInterface[*v1.Dllama, *v1.DllamaList](ctrl)

	handler := &dllamaHandler{
		models:       modelsController,
		roots:        rootsController,
		workers:      workersController,
		statefulsets: stsController,
		dllamas:      dllamaController,
	}

	modelsController.EXPECT().Cache().Return(modelsCache).AnyTimes()
	rootsController.EXPECT().Cache().Return(rootsCache).AnyTimes()
	workersController.EXPECT().Cache().Return(workersCache).AnyTimes()
	stsController.EXPECT().Cache().Return(stsCache).AnyTimes()

	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "demo-model", Namespace: "default"}, Status: v1.ModelStatus{OutputPVCName: "pvc"}}
	modelsCache.EXPECT().Get("default", "demo-model").Return(model, nil)
	rootsCache.EXPECT().Get("default", "demo-root").Return(&v1.Root{Status: v1.RootStatus{Conditions: []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue}}}}, nil)
	workerName := workerResourceName("demo")
	workersCache.EXPECT().List("default", gomock.Any()).Return([]*v1.Worker{{ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: "default"}}}, nil)
	stsCache.EXPECT().Get("default", workerName).Return(&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}, nil)

	dllama := &v1.Dllama{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: v1.DllamaSpec{
			ModelRef:     v1.ModelReference{Kind: "Model", Name: "demo-model"},
			ReplicaPower: 0,
		},
	}

	dllamaController.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Dllama{})).DoAndReturn(func(obj *v1.Dllama) (*v1.Dllama, error) {
		cond := meta.FindStatusCondition(obj.Status.Conditions, conditionReady)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionTrue, cond.Status)
		require.Equal(t, int32(1), obj.Status.ReadyWorkers)
		return obj, nil
	})

	_, err := handler.ensureStatus(dllama)
	require.NoError(t, err)
}
