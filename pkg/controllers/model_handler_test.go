package controllers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

func TestModelHandlerOnRelatedJobDeletion(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	handler := &modelHandler{models: models}

	models.EXPECT().Enqueue("models", "mistral")

	result, err := handler.onRelatedJob("models/mistral-download", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestModelHandlerOnRelatedJobDeletionSkipsInvalidKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	models.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)
	handler := &modelHandler{models: models}

	result, err := handler.onRelatedJob("malformed", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestModelHandlerOnRelatedJobDeletionSkipsUnknownSuffix(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	models.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)
	handler := &modelHandler{models: models}

	result, err := handler.onRelatedJob("models/mistral", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestModelHandlerOnRelatedJobStatusChange(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	handler := &modelHandler{models: models}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-download",
			Labels: map[string]string{
				labelComponent: componentModel,
				labelModelName: "mistral",
			},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}

	models.EXPECT().Enqueue("models", "mistral")

	result, err := handler.onRelatedJob("ignored", job)
	require.NoError(t, err)
	require.Equal(t, job, result)
}

func TestModelHandlerOnRelatedJobIgnoresNonModel(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	handler := &modelHandler{models: models}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "random",
			Labels: map[string]string{
				labelComponent: componentWorker,
			},
		},
		Status: batchv1.JobStatus{Active: 1, StartTime: &metav1.Time{}},
	}

	models.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

	result, err := handler.onRelatedJob("ignored", job)
	require.NoError(t, err)
	require.Equal(t, job, result)

	// Also verify missing model label short-circuits without enqueue
	job.Labels = map[string]string{labelComponent: componentModel}
	result, err = handler.onRelatedJob("ignored", job)
	require.NoError(t, err)
	require.Equal(t, job, result)
}

func TestModelHandlerShouldEnqueueModelForJob(t *testing.T) {

	handler := &modelHandler{}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models",
			Name:      "mistral-download",
		},
		Status: batchv1.JobStatus{
			Active: 1,
			StartTime: func() *metav1.Time {
				ts := metav1.NewTime(time.Now().Add(-130 * time.Second))
				return &ts
			}(),
		},
	}

	require.True(t, handler.shouldEnqueueModelForJob(job), "long running job should enqueue model reconciliation")

	recent := metav1.NewTime(time.Now().Add(-30 * time.Second))
	job.Status.StartTime = &recent
	require.False(t, handler.shouldEnqueueModelForJob(job), "recently started job should not enqueue yet")

	job.Status.StartTime = nil
	require.False(t, handler.shouldEnqueueModelForJob(job), "job without start time should not enqueue immediately")

	job.Status.Active = 0
	job.Status.Succeeded = 1
	require.True(t, handler.shouldEnqueueModelForJob(job), "finished job should always enqueue")
}

func TestModelHandlerEnsureMetadataUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	sentinel := errors.New("override error")
	var called bool

	handler.ensureMetadataFn = func(m *v1.Model) error {
		called = true
		require.Same(t, model, m)
		return sentinel
	}

	err := handler.ensureMetadata(model)
	require.ErrorIs(t, err, sentinel)
	require.True(t, called, "override function should be invoked")
}

func TestModelHandlerEnsureMetadataFallsBackToConfigMap(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &modelHandler{
		apply:            fakeApply,
		ensureMetadataFn: nil,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "models",
		},
	}

	err := handler.ensureMetadata(model)
	require.NoError(t, err)
	require.Len(t, fakeApply.appliedObjects, 1, "metadata ConfigMap should be applied")

	cm, ok := fakeApply.appliedObjects[0].(*corev1.ConfigMap)
	require.True(t, ok, "applied object should be a ConfigMap")
	require.Equal(t, "llama-metadata", cm.Name)
	require.Equal(t, "models", fakeApply.defaultNamespace)
}

func TestModelHandlerEnsureScriptUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	sentinel := errors.New("script override")
	var called bool

	handler.ensureScriptFn = func(m *v1.Model) error {
		called = true
		require.Same(t, model, m)
		return sentinel
	}

	err := handler.ensureScript(model)
	require.ErrorIs(t, err, sentinel)
	require.True(t, called, "override script function should run")
}

func TestModelHandlerEnsureScriptFallsBackToConfigMap(t *testing.T) {

	fakeApply := newFakeApply()
	handler := &modelHandler{
		apply:          fakeApply,
		ensureScriptFn: nil,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "models",
		},
	}

	err := handler.ensureScript(model)
	require.NoError(t, err)
	require.Len(t, fakeApply.appliedObjects, 1, "script ConfigMap should be applied")

	cm, ok := fakeApply.appliedObjects[0].(*corev1.ConfigMap)
	require.True(t, ok, "applied object should be a ConfigMap")
	require.Equal(t, "llama-download-script", cm.Name)
	require.Equal(t, "models", fakeApply.defaultNamespace)
}

func TestModelHandlerEnsureDownloadUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	sentinel := errors.New("download override")
	var called bool

	handler.ensureDownloadJobFn = func(m *v1.Model) error {
		called = true
		require.Same(t, model, m)
		return sentinel
	}

	err := handler.ensureDownload(model)
	require.ErrorIs(t, err, sentinel)
	require.True(t, called, "override download function should be invoked")
}

func TestModelHandlerEnsureDownloadFallsBackGracefully(t *testing.T) {

	handler := &modelHandler{
		ensureDownloadJobFn: nil,
	}

	model := &v1.Model{}
	err := handler.ensureDownload(model)
	require.NoError(t, err)
}

func TestModelHandlerEnsureConversionUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	sentinel := errors.New("conversion override")
	var called bool

	handler.ensureConversionFn = func(m *v1.Model) error {
		called = true
		require.Same(t, model, m)
		return sentinel
	}

	err := handler.ensureConversion(model)
	require.ErrorIs(t, err, sentinel)
	require.True(t, called, "override conversion function should be invoked")
}

func TestModelHandlerEnsureConversionFallbackQuickly(t *testing.T) {

	handler := &modelHandler{
		ensureConversionFn: nil,
	}

	model := &v1.Model{}
	err := handler.ensureConversion(model)
	require.NoError(t, err)
}

func TestModelHandlerEnsureDownloadFallbackPropagatesApplyError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	cache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)

	jobs.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		Get("models", "llama-download").
		Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "llama-download")).
		AnyTimes()

	sentinel := errors.New("apply download failure")
	handler := &modelHandler{
		apply: &failingApply{
			fakeApply: newFakeApply(),
			err:       sentinel,
		},
		jobs: jobs,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "llama",
			Namespace:  "models",
			Generation: 3,
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/llama.bin",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "source",
				BucketForConvert: "convert",
			},
		},
	}

	err := handler.ensureDownload(model)
	require.ErrorIs(t, err, sentinel)
}

func TestModelHandlerEnsureConversionFallbackPropagatesApplyError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	cache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)

	jobs.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		Get("models", "llama-convert").
		Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "llama-convert")).
		AnyTimes()

	sentinel := errors.New("apply conversion failure")
	handler := &modelHandler{
		apply: &failingApply{
			fakeApply: newFakeApply(),
			err:       sentinel,
		},
		jobs: jobs,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "llama",
			Namespace:  "models",
			Generation: 5,
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/llama.bin",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "source",
				BucketForConvert: "convert",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 5,
		},
	}

	err := handler.ensureConversion(model)
	require.ErrorIs(t, err, sentinel)
}

func TestModelHandlerEnsureBucketsUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	sentinel := errors.New("buckets override")
	var called bool

	handler.ensureBucketsFn = func(m *v1.Model) error {
		called = true
		require.Same(t, model, m)
		return sentinel
	}

	err := handler.ensureBucketsForModel(model)
	require.ErrorIs(t, err, sentinel)
	require.True(t, called, "ensureBuckets override should be invoked")
}

func TestModelHandlerEnsureBucketsFallbackPropagatesSecretError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	secrets := genericfake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	cache := genericfake.NewMockCacheInterface[*corev1.Secret](ctrl)

	secrets.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().Get("models", "storage").Return(nil, errors.New("secret lookup failed"))

	handler := &modelHandler{
		ctx:             context.Background(),
		ensureBuckets:   true,
		secrets:         secrets,
		ensureBucketsFn: nil,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForSource:  "source",
				BucketForConvert: "convert",
				SecretRef:        &v1.SecretReference{Name: "storage"},
			},
		},
	}

	err := handler.ensureBucketsForModel(model)
	require.ErrorContains(t, err, "fetch object storage secret")
}

func TestModelHandlerEnsureSizingUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	sentinel := errors.New("sizing override")

	handler.ensureSizingFn = func(m *v1.Model) error {
		require.Same(t, model, m)
		return sentinel
	}

	err := handler.ensureSizing(model)
	require.ErrorIs(t, err, sentinel)
}

func TestModelHandlerEnsureSizingFallbackPropagatesApplyError(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	cache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)

	jobs.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().
		Get("models", "llama-size").
		Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "llama-size")).
		AnyTimes()

	sentinel := errors.New("apply sizing failure")
	handler := &modelHandler{
		ctx:            context.Background(),
		jobs:           jobs,
		apply:          &failingApply{fakeApply: newFakeApply(), err: sentinel},
		ensureSizingFn: nil,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "llama",
			Namespace:  "models",
			Generation: 4,
		},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:       "Succeeded",
			ObservedGeneration:    4,
			OutputPVCName:         "llama-output",
			ConversionSizeState:   "Pending",
			ConversionSizeJobName: "",
		},
	}

	err := handler.ensureSizing(model)
	require.ErrorIs(t, err, sentinel)
}

func TestModelHandlerEnsureStatusUpdateUsesOverride(t *testing.T) {

	handler := &modelHandler{}
	model := &v1.Model{}
	updated := model.DeepCopy()
	updated.Status.ObservedGeneration = 42
	sentinel := errors.New("status override")

	handler.ensureStatusFn = func(m *v1.Model) (*v1.Model, error) {
		require.Same(t, model, m)
		return updated, sentinel
	}

	result, err := handler.ensureStatusUpdate(model)
	require.Same(t, updated, result)
	require.ErrorIs(t, err, sentinel)
}

func TestModelHandlerEnsureStatusUpdateFallsBackToEnsureStatus(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	models.EXPECT().
		UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).
		DoAndReturn(func(m *v1.Model) (*v1.Model, error) {
			return m, nil
		})

	handler := &modelHandler{
		models:         models,
		ensureStatusFn: nil,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "llama",
			Namespace:  "models",
			Generation: 2,
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 0,
		},
	}

	result, err := handler.ensureStatusUpdate(model)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestModelHandlerOnChangeRunsEnsureSequence(t *testing.T) {

	handler := &modelHandler{}
	obj := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "models"}}

	var calls []string
	set := func(name string) func(*v1.Model) error {
		return func(m *v1.Model) error {
			calls = append(calls, name)
			require.Same(t, obj, m)
			return nil
		}
	}

	handler.ensureMetadataFn = set("metadata")
	handler.ensureScriptFn = set("script")
	handler.ensureBucketsFn = set("buckets")
	handler.ensureDownloadJobFn = set("download")
	handler.ensureConversionFn = set("conversion")
	handler.ensureSizingFn = set("sizing")
	handler.ensureStatusFn = func(m *v1.Model) (*v1.Model, error) {
		calls = append(calls, "status")
		require.Same(t, obj, m)
		return m, nil
	}

	result, err := handler.onChange("models/llama", obj)
	require.NoError(t, err)
	require.Same(t, obj, result)
	require.Equal(t, []string{"metadata", "script", "buckets", "download", "conversion", "sizing", "status"}, calls)
}

func TestModelHandlerOnChangeHandlesNilAndDeletion(t *testing.T) {

	handler := &modelHandler{}
	result, err := handler.onChange("", nil)
	require.NoError(t, err)
	require.Nil(t, result)

	obj := &v1.Model{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{Time: time.Now()}}}
	handler.ensureMetadataFn = func(*v1.Model) error {
		t.Fatalf("ensureMetadata should not run for deleting models")
		return nil
	}
	result, err = handler.onChange("models/llama", obj)
	require.NoError(t, err)
	require.Same(t, obj, result)
}

func TestModelHandlerOnChangeHandlesEnsureScriptError(t *testing.T) {

	sentinel := errors.New("script ensure failed")
	handler := &modelHandler{}
	obj := &v1.Model{}

	handler.ensureMetadataFn = func(*v1.Model) error { return nil }
	handler.ensureScriptFn = func(*v1.Model) error { return sentinel }
	handler.ensureBucketsFn = func(*v1.Model) error {
		t.Fatalf("ensureBuckets should not run when script ensure fails")
		return nil
	}
	handler.ensureDownloadJobFn = func(*v1.Model) error {
		t.Fatalf("ensureDownload should not run when script ensure fails")
		return nil
	}
	handler.ensureConversionFn = func(*v1.Model) error {
		t.Fatalf("ensureConversion should not run when script ensure fails")
		return nil
	}
	handler.ensureSizingFn = func(*v1.Model) error {
		t.Fatalf("ensureSizing should not run when script ensure fails")
		return nil
	}

	result, err := handler.onChange("models/llama", obj)
	require.ErrorIs(t, err, sentinel)
	require.Same(t, obj, result)
}

func TestModelHandlerOnChangePropagatesErrors(t *testing.T) {

	sentinel := errors.New("ensure failure")
	handler := &modelHandler{}
	obj := &v1.Model{}

	handler.ensureMetadataFn = func(*v1.Model) error { return sentinel }
	result, err := handler.onChange("models/llama", obj)
	require.ErrorIs(t, err, sentinel)
	require.Same(t, obj, result)
}

func TestModelHandlerOnChangeLogsFailuresPerStage(t *testing.T) {

	testCases := []struct {
		name  string
		setup func(h *modelHandler)
	}{
		{
			name: "buckets",
			setup: func(h *modelHandler) {
				h.ensureBucketsFn = func(*v1.Model) error { return assert.AnError }
			},
		},
		{
			name: "download",
			setup: func(h *modelHandler) {
				h.ensureDownloadJobFn = func(*v1.Model) error { return assert.AnError }
			},
		},
		{
			name: "conversion",
			setup: func(h *modelHandler) {
				h.ensureConversionFn = func(*v1.Model) error { return assert.AnError }
			},
		},
		{
			name: "sizing",
			setup: func(h *modelHandler) {
				h.ensureSizingFn = func(*v1.Model) error { return assert.AnError }
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &modelHandler{}
			obj := &v1.Model{}

			// Default the earlier ensure functions so the test reaches the target stage.
			h.ensureMetadataFn = func(*v1.Model) error { return nil }
			h.ensureScriptFn = func(*v1.Model) error { return nil }
			h.ensureBucketsFn = func(*v1.Model) error { return nil }
			h.ensureDownloadJobFn = func(*v1.Model) error { return nil }
			h.ensureConversionFn = func(*v1.Model) error { return nil }
			h.ensureSizingFn = func(*v1.Model) error { return nil }
			h.ensureStatusFn = func(*v1.Model) (*v1.Model, error) {
				t.Fatalf("ensureStatus should not be called for %s", tc.name)
				return nil, nil
			}

			tc := tc // capture
			tc.setup(h)

			result, err := h.onChange("models/llama", obj)
			require.ErrorIs(t, err, assert.AnError)
			require.Same(t, obj, result)
		})
	}
}

func TestModelHandlerOnRemoveDeletesVolumes(t *testing.T) {

	pvcs := newTrackingPVCController()
	pvs := newTrackingPVController()
	handler := &modelHandler{pvcs: pvcs, pvs: pvs}

	obj := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}}
	result, err := handler.onRemove("models/mistral", obj)
	require.NoError(t, err)
	require.Same(t, obj, result)
	require.ElementsMatch(t, []string{"models/mistral-s3-pvc", "models/mistral-s3-output-pvc"}, pvcs.deleted)
	require.ElementsMatch(t, []string{"mistral-s3-pv", "mistral-s3-output-pv"}, pvs.deleted)
}

func TestModelHandlerOnRemoveHandlesNilObject(t *testing.T) {

	handler := &modelHandler{}
	result, err := handler.onRemove("models/mistral", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestModelHandlerOnRemoveIgnoresDeleteErrors(t *testing.T) {

	pvcs := newTrackingPVCController()
	pvcs.deleteErr = assert.AnError
	pvs := newTrackingPVController()
	pvs.deleteErr = assert.AnError

	handler := &modelHandler{pvcs: pvcs, pvs: pvs}
	obj := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "falcon", Namespace: "models"}}

	result, err := handler.onRemove("models/falcon", obj)
	require.NoError(t, err)
	require.Same(t, obj, result)
	require.Len(t, pvcs.deleted, 2)
	require.Len(t, pvs.deleted, 2)
}

type trackingPVCController struct {
	*controllerStub[*corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaimList]
	deleted   []string
	deleteErr error
}

func newTrackingPVCController() *trackingPVCController {
	return &trackingPVCController{
		controllerStub: newControllerStub[*corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaimList](schema.GroupVersionKind{}),
	}
}

func (t *trackingPVCController) Delete(namespace, name string, opts *metav1.DeleteOptions) error {
	t.deleted = append(t.deleted, fmt.Sprintf("%s/%s", namespace, name))
	return t.deleteErr
}

type trackingPVController struct {
	*nonNamespacedControllerStub[*corev1.PersistentVolume, *corev1.PersistentVolumeList]
	deleted   []string
	deleteErr error
}

func newTrackingPVController() *trackingPVController {
	return &trackingPVController{
		nonNamespacedControllerStub: newNonNamespacedControllerStub[*corev1.PersistentVolume, *corev1.PersistentVolumeList](schema.GroupVersionKind{}),
	}
}

func (t *trackingPVController) Delete(name string, opts *metav1.DeleteOptions) error {
	t.deleted = append(t.deleted, name)
	return t.deleteErr
}

type nonNamespacedControllerStub[T generic.RuntimeMetaObject, TL runtime.Object] struct {
	*controllerStub[T, TL]
}

func newNonNamespacedControllerStub[T generic.RuntimeMetaObject, TL runtime.Object](gvk schema.GroupVersionKind) *nonNamespacedControllerStub[T, TL] {
	return &nonNamespacedControllerStub[T, TL]{controllerStub: newControllerStub[T, TL](gvk)}
}

func (n *nonNamespacedControllerStub[T, TL]) Enqueue(name string) {
	n.controllerStub.Enqueue("", name)
}

func (n *nonNamespacedControllerStub[T, TL]) EnqueueAfter(name string, duration time.Duration) {
	n.controllerStub.EnqueueAfter("", name, duration)
}

func (n *nonNamespacedControllerStub[T, TL]) Cache() generic.NonNamespacedCacheInterface[T] {
	return nil
}

func (n *nonNamespacedControllerStub[T, TL]) Delete(name string, opts *metav1.DeleteOptions) error {
	return n.controllerStub.Delete("", name, opts)
}

func (n *nonNamespacedControllerStub[T, TL]) Get(name string, opts metav1.GetOptions) (T, error) {
	return n.controllerStub.Get("", name, opts)
}

func (n *nonNamespacedControllerStub[T, TL]) List(opts metav1.ListOptions) (TL, error) {
	return n.controllerStub.List("", opts)
}

func (n *nonNamespacedControllerStub[T, TL]) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return n.controllerStub.Watch("", opts)
}

func (n *nonNamespacedControllerStub[T, TL]) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (T, error) {
	return n.controllerStub.Patch("", name, pt, data, subresources...)
}

func (n *nonNamespacedControllerStub[T, TL]) WithImpersonation(cfg rest.ImpersonationConfig) (generic.NonNamespacedClientInterface[T, TL], error) {
	_, err := n.controllerStub.WithImpersonation(cfg)
	return n, err
}
