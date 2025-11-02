package controllers

import (
	"strings"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestJobNameForModel(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		want      string
	}{
		{
			name:      "short name",
			modelName: "test-model",
			want:      "test-model-download",
		},
		{
			name:      "long name gets truncated",
			modelName: "this-is-a-very-long-model-name-that-exceeds-kubernetes-limits",
			want:      "this-is-a-very-long-model-name-that-exceeds-kubernetes-limits-d",
		},
		{
			name:      "empty name",
			modelName: "",
			want:      "-download",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.modelName,
				},
			}
			got := jobNameForModel(model)
			if got != tt.want {
				t.Errorf("jobNameForModel() = %v, want %v", got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("jobNameForModel() length %d > 63", len(got))
			}
		})
	}
}

func TestConversionJobName(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		want      string
	}{
		{
			name:      "short name",
			modelName: "test-model",
			want:      "test-model-convert",
		},
		{
			name:      "long name gets truncated",
			modelName: "this-is-a-very-long-model-name-that-exceeds-kubernetes-limits",
			want:      "this-is-a-very-long-model-name-that-exceeds-kubernetes-limits-c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.modelName,
				},
			}
			got := conversionJobName(model)
			if got != tt.want {
				t.Errorf("conversionJobName() = %v, want %v", got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("conversionJobName() length %d > 63", len(got))
			}
		})
	}
}

func TestSizeJobName(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		want      string
	}{
		{
			name:      "short name",
			modelName: "test-model",
			want:      "test-model-size",
		},
		{
			name:      "long name gets truncated",
			modelName: "this-is-a-very-long-model-name-that-exceeds-kubernetes-limits",
			want:      "this-is-a-very-long-model-name-that-exceeds-kubernetes-limits-s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.modelName,
				},
			}
			got := sizeJobName(model)
			if got != tt.want {
				t.Errorf("sizeJobName() = %v, want %v", got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("sizeJobName() length %d > 63", len(got))
			}
		})
	}
}

func TestModelNameFromJob(t *testing.T) {
	tests := []struct {
		name    string
		jobName string
		want    string
	}{
		{
			name:    "download job",
			jobName: "test-model-download",
			want:    "test-model",
		},
		{
			name:    "convert job",
			jobName: "test-model-convert",
			want:    "test-model",
		},
		{
			name:    "size job - no match",
			jobName: "test-model-size",
			want:    "",
		},
		{
			name:    "no suffix",
			jobName: "test-model",
			want:    "",
		},
		{
			name:    "empty string",
			jobName: "",
			want:    "",
		},
		{
			name:    "partial suffix match",
			jobName: "test-download-model",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := modelNameFromJob(tt.jobName)
			if got != tt.want {
				t.Errorf("modelNameFromJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnsureConversionJobCreatesJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache)
	jobsCache.EXPECT().Get("models", "mistral-convert").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-convert"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "src",
				BucketForConvert: "out",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 2,
		},
	}
	model.Generation = 2

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if len(applySpy.Objects) == 0 {
		t.Fatalf("expected job to be applied")
	}
}

func TestEnsureConversionJobSkipsWithoutBuckets(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "", BucketForConvert: ""},
			Conversion:    &v1.ModelConversionSpec{},
		},
	}

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
}

func TestEnsureConversionJobDeletesOutdatedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-convert",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "1"},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-convert").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-convert", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).DoAndReturn(func(namespace, name string, opts *metav1.DeleteOptions) error {
		if opts == nil || opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationBackground {
			t.Fatalf("unexpected delete options: %#v", opts)
		}
		return nil
	})

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: "dst"},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 2,
		},
	}
	model.Generation = 2

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 0 {
		t.Fatalf("expected no objects applied, got %d", len(applySpy.Objects))
	}
}

func TestEnsureConversionJobSkipsWhenTrackedInStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-convert").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-convert"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: "dst"},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 5,
			ConversionJobName:  "mistral-convert",
			ConversionState:    "Running",
		},
	}
	model.Generation = 5

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 0 {
		t.Fatalf("expected no objects applied, got %d", len(applySpy.Objects))
	}
}

func TestEnsureConversionJobWaitsForDownload(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: "dst"},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Pending",
			ObservedGeneration: 1,
		},
	}
	model.Generation = 2

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
}

func TestEnsureConversionJobWaitsWhenJobBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	now := metav1.NewTime(time.Now())
	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "mistral-convert",
			Namespace:         "models",
			DeletionTimestamp: &now,
			Annotations:       map[string]string{annotationModelGeneration: "2"},
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-convert").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: "dst"},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 2,
		},
	}
	model.Generation = 2

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureConversionJobReadsGenerationFromLabels(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-convert",
			Namespace: "models",
			Labels:    map[string]string{annotationModelGeneration: "3"},
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-convert").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: "dst"},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 3,
		},
	}
	model.Generation = 3

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls when generation matches from labels, got %d", applySpy.Count)
	}
}

func TestEnsureConversionJobSkipsWhenJobFinishedWithCorrectGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-convert",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "4"},
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-convert").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: "dst"},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 4,
		},
	}
	model.Generation = 4

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls when job finished with correct generation, got %d", applySpy.Count)
	}
}

func TestEnsureConversionJobSkipsWhenMissingBucketForConvert(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{BucketForSource: "src", BucketForConvert: ""},
			Conversion:    &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 1,
		},
	}
	model.Generation = 1

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
}

func TestEnsureConversionJobRespectsPVOverrides(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache)
	jobsCache.EXPECT().Get("models", "mistral-convert").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-convert"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			LocalPath: "s3://override-bucket/models",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "source-bucket",
				BucketForConvert: "convert-bucket",
			},
			Conversion: &v1.ModelConversionSpec{
				ConverterVersion: "v0.20.0",
				ToolsImage:       "custom-tools:1.0",
				WeightsFloatType: "q6",
			},
			PV: &v1.ModelPVSpec{
				Capacity:            "20Gi",
				StorageClassName:    "custom-class",
				AccessModes:         []string{"ReadWriteMany", "ReadWriteOnce", "ReadOnlyMany"},
				ReclaimPolicy:       "Delete",
				CSIDriver:           "example.csi/driver",
				CSIMounter:          "geesefs",
				CSIOptions:          "cache=true",
				VolumeAttributes:    map[string]string{"foo": "bar", "bucket": "ignored"},
				CSISecretName:       "custom-secret",
				CSISecretNamespace:  "custom-namespace",
				PVCStorageClassName: "custom-pvc-class",
				PVCCapacity:         "15Gi",
				PVCAccessModes:      []string{"ReadWriteOnce"},
			},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 3,
		},
	}
	model.Generation = 3

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}
	if applySpy.Count != 5 {
		t.Fatalf("expected 5 apply calls (PV, PVC, output PV/PVC, Job), got %d", applySpy.Count)
	}

	var (
		inputPV, outputPV   *corev1.PersistentVolume
		inputPVC, outputPVC *corev1.PersistentVolumeClaim
		conversionJob       *batchv1.Job
	)
	for _, set := range applySpy.Objects {
		for _, obj := range set.All() {
			switch o := obj.(type) {
			case *corev1.PersistentVolume:
				if strings.Contains(o.Name, "-output-") {
					outputPV = o
				} else {
					inputPV = o
				}
			case *corev1.PersistentVolumeClaim:
				if strings.Contains(o.Name, "-output-") {
					outputPVC = o
				} else {
					inputPVC = o
				}
			case *batchv1.Job:
				conversionJob = o
			}
		}
	}

	if inputPV == nil || outputPV == nil || inputPVC == nil || outputPVC == nil || conversionJob == nil {
		t.Fatalf("expected all resources to be applied (input/output PV/PVC and job)")
	}

	if inputPV.Spec.StorageClassName != "custom-class" {
		t.Fatalf("input PV StorageClassName = %s, want custom-class", inputPV.Spec.StorageClassName)
	}
	pvModes := map[corev1.PersistentVolumeAccessMode]bool{}
	for _, m := range inputPV.Spec.AccessModes {
		pvModes[m] = true
	}
	if !(pvModes[corev1.ReadWriteMany] && pvModes[corev1.ReadWriteOnce] && pvModes[corev1.ReadOnlyMany]) {
		t.Fatalf("input PV access modes = %v, expected all RWX/RWO/ROX", inputPV.Spec.AccessModes)
	}
	csi := inputPV.Spec.PersistentVolumeSource.CSI
	if csi == nil {
		t.Fatalf("input PV CSI source is nil")
	}
	if csi.Driver != "example.csi/driver" {
		t.Fatalf("input PV CSI driver = %s, want example.csi/driver", csi.Driver)
	}
	if csi.ControllerPublishSecretRef == nil || csi.ControllerPublishSecretRef.Name != "custom-secret" || csi.ControllerPublishSecretRef.Namespace != "custom-namespace" {
		t.Fatalf("input PV controller secret = %#v, want custom-secret/custom-namespace", csi.ControllerPublishSecretRef)
	}
	if csi.NodePublishSecretRef == nil || csi.NodePublishSecretRef.Name != "custom-secret" || csi.NodePublishSecretRef.Namespace != "custom-namespace" {
		t.Fatalf("input PV node publish secret = %#v, want custom-secret/custom-namespace", csi.NodePublishSecretRef)
	}
	if got := csi.VolumeAttributes["bucket"]; got != "ignored" {
		t.Fatalf("input PV volume attribute bucket = %s, want ignored", got)
	}
	if got := csi.VolumeAttributes["foo"]; got != "bar" {
		t.Fatalf("input PV volume attribute foo = %s, want bar", got)
	}
	if got := csi.VolumeAttributes["prefix"]; got != "models" {
		t.Fatalf("input PV volume attribute prefix = %s, want models", got)
	}
	if got := csi.VolumeAttributes["mounter"]; got != "geesefs" {
		t.Fatalf("input PV volume attribute mounter = %s, want geesefs", got)
	}
	if got := csi.VolumeAttributes["options"]; got != "cache=true" {
		t.Fatalf("input PV volume attribute options = %s, want cache=true", got)
	}
	if csi.VolumeHandle != "override-bucket/models" {
		t.Fatalf("input PV volume handle = %s, want override-bucket/models", csi.VolumeHandle)
	}

	if inputPVC.Spec.StorageClassName == nil || *inputPVC.Spec.StorageClassName != "custom-pvc-class" {
		t.Fatalf("input PVC storage class = %v, want custom-pvc-class", inputPVC.Spec.StorageClassName)
	}
	if qty := inputPVC.Spec.Resources.Requests[corev1.ResourceStorage]; qty.String() != "15Gi" {
		t.Fatalf("input PVC storage request = %s, want 15Gi", qty.String())
	}
	if len(inputPVC.Spec.AccessModes) != 1 || inputPVC.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatalf("input PVC access modes = %v, want [ReadWriteOnce]", inputPVC.Spec.AccessModes)
	}

	if outputPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["bucket"] != "convert-bucket" {
		t.Fatalf("output PV bucket = %s, want convert-bucket", outputPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["bucket"])
	}
	if prefix, ok := outputPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["prefix"]; !ok || prefix == "" {
		t.Fatalf("output PV must include non-empty prefix attribute, got %q", prefix)
	}
	if _, ok := outputPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["foo"]; !ok {
		t.Fatalf("output PV must retain custom volume attribute foo")
	}

	if qty := outputPVC.Spec.Resources.Requests[corev1.ResourceStorage]; qty.String() != "15Gi" {
		t.Fatalf("output PVC storage request = %s, want 15Gi", qty.String())
	}
	if len(outputPVC.Spec.AccessModes) != 1 || outputPVC.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatalf("output PVC access modes = %v, want [ReadWriteOnce]", outputPVC.Spec.AccessModes)
	}

	if len(conversionJob.Spec.Template.Spec.Volumes) < 3 {
		t.Fatalf("expected at least three volumes (workspace, s3, s3-output), got %d", len(conversionJob.Spec.Template.Spec.Volumes))
	}
	if len(conversionJob.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected single init container, got %d", len(conversionJob.Spec.Template.Spec.InitContainers))
	}
	if conversionJob.Spec.Template.Spec.InitContainers[0].Image != "custom-tools:1.0" {
		t.Fatalf("init container image = %s, want custom-tools:1.0", conversionJob.Spec.Template.Spec.InitContainers[0].Image)
	}
	mainContainer := conversionJob.Spec.Template.Spec.Containers[0]
	foundOutputMount := false
	for _, mount := range mainContainer.VolumeMounts {
		if mount.Name == "s3-output" && mount.MountPath == "/mnt/s3-output" {
			foundOutputMount = true
			break
		}
	}
	if !foundOutputMount {
		t.Fatalf("expected main container to mount s3-output PVC")
	}
	foundOutputEnv := false
	for _, env := range mainContainer.Env {
		if env.Name == "CONVERSION_OUTPUT_PATH" && env.Value == "/mnt/s3-output" {
			foundOutputEnv = true
		}
	}
	if !foundOutputEnv {
		t.Fatalf("expected CONVERSION_OUTPUT_PATH env var to be set")
	}
}

func TestEnsureConversionJobFallsBackPVAccessModes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache)
	jobsCache.EXPECT().Get("models", "mistral-convert").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-convert"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "src",
				BucketForConvert: "dst",
			},
			Conversion: &v1.ModelConversionSpec{},
			PV: &v1.ModelPVSpec{
				AccessModes:    []string{"unsupported"},
				PVCAccessModes: []string{"invalid"},
			},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 1,
		},
	}
	model.Generation = 1

	if err := handler.ensureConversionJob(model); err != nil {
		t.Fatalf("ensureConversionJob returned error: %v", err)
	}

	var (
		inputPV  *corev1.PersistentVolume
		inputPVC *corev1.PersistentVolumeClaim
	)
	for _, set := range applySpy.Objects {
		for _, obj := range set.All() {
			switch o := obj.(type) {
			case *corev1.PersistentVolume:
				if !strings.Contains(o.Name, "-output-") {
					inputPV = o
				}
			case *corev1.PersistentVolumeClaim:
				if !strings.Contains(o.Name, "-output-") {
					inputPVC = o
				}
			}
		}
	}

	if inputPV == nil || inputPVC == nil {
		t.Fatalf("expected input PV and PVC to be applied")
	}
	if len(inputPV.Spec.AccessModes) != 1 || inputPV.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Fatalf("fallback PV access modes = %v, want [ReadWriteMany]", inputPV.Spec.AccessModes)
	}
	if len(inputPVC.Spec.AccessModes) != 1 || inputPVC.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Fatalf("fallback PVC access modes = %v, want [ReadWriteMany]", inputPVC.Spec.AccessModes)
	}
}

func TestEnsureDownloadJobCreatesJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download")).Times(2)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 1,
		},
	}
	model.Generation = 2

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 1 {
		t.Fatalf("expected 1 apply set, got %d", len(applySpy.Objects))
	}
	objects := applySpy.Objects[0].All()
	if len(objects) != 1 {
		t.Fatalf("expected 1 object applied, got %d", len(objects))
	}
	job, ok := objects[0].(*batchv1.Job)
	if !ok {
		t.Fatalf("expected *batchv1.Job, got %T", objects[0])
	}
	if job.Name != "mistral-download" {
		t.Fatalf("unexpected job name %s", job.Name)
	}
	if job.Annotations[annotationModelGeneration] != "2" {
		t.Fatalf("expected annotationModelGeneration=2, got %s", job.Annotations[annotationModelGeneration])
	}
}

func TestEnsureDownloadJobDeletesLegacyJobWithoutGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply:  applySpy,
		jobs:   jobs,
		models: models,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-download",
			Namespace:   "models",
			Annotations: map[string]string{},
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-download", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).DoAndReturn(func(namespace, name string, opts *metav1.DeleteOptions) error {
		if opts == nil || opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationBackground {
			t.Fatalf("unexpected delete options: %#v", opts)
		}
		return nil
	})
	models.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		if updated.Annotations == nil {
			t.Fatalf("expected annotations to be initialized")
		}
		raw := updated.Annotations[annotationJobDeletedAt]
		if raw == "" {
			t.Fatalf("expected %s annotation to be set", annotationJobDeletedAt)
		}
		if _, err := time.Parse(time.RFC3339, raw); err != nil {
			t.Fatalf("annotation value %q is not RFC3339: %v", raw, err)
		}
		return updated, nil
	})

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
	}
	model.Generation = 3

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 0 {
		t.Fatalf("expected no apply objects, got %d", len(applySpy.Objects))
	}
}

func TestEnsureDownloadJobSkipsWhenRecentlyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download")).Times(2)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral",
			Namespace:   "models",
			Annotations: map[string]string{annotationJobDeletedAt: time.Now().Format(time.RFC3339)},
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
	}
	model.Generation = 7

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 0 {
		t.Fatalf("expected no apply objects, got %d", len(applySpy.Objects))
	}
}

func TestEnsureDownloadJobSkipsWithoutStorage(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &modelHandler{
		apply: applySpy,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec:       v1.ModelSpec{SourceURL: ""},
	}

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobSkipsWhenAlreadySucceeded(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &modelHandler{
		apply: applySpy,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 4,
		},
	}
	model.Generation = 4

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobWaitsWhenJobBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	now := metav1.NewTime(time.Now())
	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "mistral-download",
			Namespace:         "models",
			DeletionTimestamp: &now,
			Annotations:       map[string]string{annotationModelGeneration: "1"},
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 1,
		},
	}
	model.Generation = 2

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobSkipsWhenGenerationMatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-download",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "5"},
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 5,
		},
	}
	model.Generation = 5

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobSkipsWhenJobRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-download",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "3"},
		},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 4,
		},
	}
	model.Generation = 4

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobDeletesFinishedOutdatedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply:  applySpy,
		jobs:   jobs,
		models: models,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-download",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "1"},
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-download", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).Return(nil)
	models.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		if updated.Annotations[annotationJobDeletedAt] == "" {
			t.Fatalf("expected %s annotation to be set", annotationJobDeletedAt)
		}
		return updated, nil
	})

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 3,
		},
	}
	model.Generation = 3

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobSkipsTrackedNonFailedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 3,
			DownloadJobName:    "mistral-download",
			DownloadState:      "Running",
		},
	}
	model.Generation = 3

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobSkipsWhenJobRecentlyCreatedCondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 3,
			DownloadJobName:    "mistral-download",
			Conditions: []metav1.Condition{
				{
					Type:               conditionDownloaded,
					Status:             metav1.ConditionFalse,
					Reason:             "JobCreated",
					LastTransitionTime: metav1.NewTime(time.Now()),
				},
			},
		},
	}
	model.Generation = 3

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobSkipsWhenJobRecentlyCreatedJobPendingReason(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 5,
			DownloadJobName:    "mistral-download",
			Conditions: []metav1.Condition{
				{
					Type:               conditionDownloaded,
					Status:             metav1.ConditionFalse,
					Reason:             "JobPending",
					LastTransitionTime: metav1.NewTime(time.Now().Add(-30 * time.Second)),
				},
			},
		},
	}
	model.Generation = 5

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobWaitsForUnfinishedJobWithoutGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-download",
			Namespace:   "models",
			Annotations: map[string]string{},
		},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 2,
		},
	}
	model.Generation = 3

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureDownloadJobCreatesAfterDeletedAtCooldownExpired(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download")).Times(2)

	oldTime := time.Now().Add(-90 * time.Second)
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral",
			Namespace:   "models",
			Annotations: map[string]string{annotationJobDeletedAt: oldTime.Format(time.RFC3339)},
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 1,
		},
	}
	model.Generation = 2

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 1 {
		t.Fatalf("expected 1 apply set after cooldown, got %d", len(applySpy.Objects))
	}
}

func TestEnsureDownloadJobIgnoresInvalidDeletedAtAnnotation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-download").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-download")).Times(2)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral",
			Namespace:   "models",
			Annotations: map[string]string{annotationJobDeletedAt: "invalid-time-format"},
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 3,
		},
	}
	model.Generation = 4

	if err := handler.ensureDownloadJob(model); err != nil {
		t.Fatalf("ensureDownloadJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 1 {
		t.Fatalf("expected job to be created when deletedAt is invalid, got %d", len(applySpy.Objects))
	}
}

func TestEnsureSizingJobCreatesJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return((*batchv1.Job)(nil), apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "mistral-size"))

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:       "Succeeded",
			ObservedGeneration:    2,
			OutputPVCName:         "mistral-output",
			ConversionSizeState:   "",
			ConversionSizeJobName: "",
		},
	}
	model.Generation = 2

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 1 {
		t.Fatalf("expected 1 apply set, got %d", len(applySpy.Objects))
	}
	objects := applySpy.Objects[0].All()
	if len(objects) != 1 {
		t.Fatalf("expected 1 object applied, got %d", len(objects))
	}
	job, ok := objects[0].(*batchv1.Job)
	if !ok {
		t.Fatalf("expected *batchv1.Job, got %T", objects[0])
	}
	if job.Name != "mistral-size" {
		t.Fatalf("unexpected job name %s", job.Name)
	}
	if job.Annotations[annotationModelGeneration] != "2" {
		t.Fatalf("expected annotationModelGeneration=2, got %s", job.Annotations[annotationModelGeneration])
	}
}

func TestEnsureSizingJobDeletesOutdatedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-size",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "1"},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-size", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).DoAndReturn(func(namespace, name string, opts *metav1.DeleteOptions) error {
		if opts == nil || opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationBackground {
			t.Fatalf("unexpected delete options: %#v", opts)
		}
		return nil
	})

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:    "Succeeded",
			ObservedGeneration: 2,
			OutputPVCName:      "mistral-output",
		},
	}
	model.Generation = 2

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 0 {
		t.Fatalf("expected no objects applied, got %d", len(applySpy.Objects))
	}
}

func TestEnsureSizingJobDeletesOnForceTokenChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-size",
			Namespace: "models",
			Annotations: map[string]string{
				annotationModelGeneration: "2",
				annotationForceSizeRerun:  "token-old",
			},
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-size", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).Return(nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "mistral",
			Namespace:       "models",
			Annotations:     map[string]string{annotationForceSizeRerun: "token-new"},
			ResourceVersion: "42",
		},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:    "Succeeded",
			ObservedGeneration: 2,
			OutputPVCName:      "mistral-output",
		},
	}
	model.Generation = 2

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if len(applySpy.Objects) != 0 {
		t.Fatalf("expected no objects applied, got %d", len(applySpy.Objects))
	}
}

func TestEnsureSizingJobSkipsWhenAlreadySucceeded(t *testing.T) {
	handler := &modelHandler{}
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:          "Succeeded",
			ObservedGeneration:       3,
			OutputPVCName:            "mistral-output",
			ConversionSizeState:      "Succeeded",
			ConversionSizeGeneration: 3,
			ConversionSizeJobName:    "mistral-size",
		},
	}
	model.Generation = 3

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
}

func TestEnsureSizingJobWaitsWhenJobBeingDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	now := metav1.NewTime(time.Now())
	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "mistral-size",
			Namespace:         "models",
			DeletionTimestamp: &now,
			Annotations:       map[string]string{annotationModelGeneration: "2"},
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:    "Succeeded",
			ObservedGeneration: 2,
			OutputPVCName:      "mistral-output",
		},
	}
	model.Generation = 2

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls, got %d", applySpy.Count)
	}
}

func TestEnsureSizingJobReadsGenerationFromLabels(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mistral-size",
			Namespace: "models",
			Labels:    map[string]string{annotationModelGeneration: "3"},
		},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:    "Succeeded",
			ObservedGeneration: 3,
			OutputPVCName:      "mistral-output",
		},
	}
	model.Generation = 3

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls when generation matches from labels, got %d", applySpy.Count)
	}
}

func TestEnsureSizingJobDeletesWhenSucceededAndNoForceToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-size",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "4"},
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-size", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).Return(nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:          "Succeeded",
			ObservedGeneration:       4,
			OutputPVCName:            "mistral-output",
			ConversionSizeState:      "Succeeded",
			ConversionSizeGeneration: 4,
		},
	}
	model.Generation = 4

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
}

func TestEnsureSizingJobDeletesWhenFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-size",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "5"},
		},
		Status: batchv1.JobStatus{
			Failed: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)
	jobs.EXPECT().Delete("models", "mistral-size", gomock.AssignableToTypeOf(&metav1.DeleteOptions{})).Return(nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:     "Succeeded",
			ObservedGeneration:  5,
			OutputPVCName:       "mistral-output",
			ConversionSizeState: "Failed",
		},
	}
	model.Generation = 5

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
}

func TestEnsureSizingJobKeepsWhenPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-size",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "6"},
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:     "Succeeded",
			ObservedGeneration:  6,
			OutputPVCName:       "mistral-output",
			ConversionSizeState: "Pending",
		},
	}
	model.Generation = 6

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no delete when state is pending, got %d calls", applySpy.Count)
	}
}

func TestEnsureSizingJobSkipsWhenUnfinished(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	applySpy := &fakeapply.FakeApply{}

	handler := &modelHandler{
		apply: applySpy,
		jobs:  jobs,
	}

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mistral-size",
			Namespace:   "models",
			Annotations: map[string]string{annotationModelGeneration: "7"},
		},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "mistral-size").Return(existing, nil)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ConversionState:     "Succeeded",
			ObservedGeneration:  7,
			OutputPVCName:       "mistral-output",
			ConversionSizeState: "Running",
		},
	}
	model.Generation = 7

	if err := handler.ensureSizingJob(model); err != nil {
		t.Fatalf("ensureSizingJob returned error: %v", err)
	}
	if applySpy.Count != 0 {
		t.Fatalf("expected no apply calls when job is running, got %d", applySpy.Count)
	}
}

func TestPersistModelAnnotationCallsUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	handler := &modelHandler{models: models}

	model := &v1.Model{ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"}}

	models.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		if updated.Annotations == nil || updated.Annotations["key"] != "value" {
			t.Fatalf("annotation not persisted: %#v", updated.Annotations)
		}
		return updated, nil
	})

	handler.persistModelAnnotation(model, "key", "value")
}

func TestIsJobFinished(t *testing.T) {
	tests := []struct {
		name string
		job  *batchv1.Job
		want bool
	}{
		{
			name: "job complete condition",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:   batchv1.JobComplete,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "job failed condition",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:   batchv1.JobFailed,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "job succeeded counter",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Succeeded: 1,
				},
			},
			want: true,
		},
		{
			name: "job failed counter",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Failed: 1,
				},
			},
			want: true,
		},
		{
			name: "job active",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Active: 1,
				},
			},
			want: false,
		},
		{
			name: "job pending",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{},
			},
			want: false,
		},
		{
			name: "job marked for deletion",
			job: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{},
				},
				Status: batchv1.JobStatus{
					Active: 1,
				},
			},
			want: true,
		},
		{
			name: "job condition false status",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:   batchv1.JobComplete,
							Status: corev1.ConditionFalse,
						},
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isJobFinished(tt.job)
			if got != tt.want {
				t.Errorf("isJobFinished() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSummarizeJob(t *testing.T) {
	tests := []struct {
		name       string
		job        *batchv1.Job
		condType   string
		wantState  string
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name: "job succeeded with condition",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:   batchv1.JobComplete,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			condType:   conditionDownloaded,
			wantState:  "Succeeded",
			wantStatus: metav1.ConditionTrue,
			wantReason: "JobSucceeded",
		},
		{
			name: "job failed with condition",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{
							Type:    batchv1.JobFailed,
							Status:  corev1.ConditionTrue,
							Message: "Pod failed",
						},
					},
				},
			},
			condType:   conditionDownloaded,
			wantState:  "Failed",
			wantStatus: metav1.ConditionFalse,
			wantReason: "JobFailed",
		},
		{
			name: "job succeeded via counter",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Succeeded: 1,
				},
			},
			condType:   conditionConverted,
			wantState:  "Succeeded",
			wantStatus: metav1.ConditionTrue,
			wantReason: "JobSucceeded",
		},
		{
			name: "job failed via counter",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Failed: 1,
				},
			},
			condType:   conditionSized,
			wantState:  "Failed",
			wantStatus: metav1.ConditionFalse,
			wantReason: "JobFailed",
		},
		{
			name: "job running",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{
					Active: 1,
				},
			},
			condType:   conditionDownloaded,
			wantState:  "Running",
			wantStatus: metav1.ConditionFalse,
			wantReason: "JobRunning",
		},
		{
			name: "job pending",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{},
			},
			condType:   conditionDownloaded,
			wantState:  "Pending",
			wantStatus: metav1.ConditionFalse,
			wantReason: "JobPending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotState, gotCond := summarizeJob(tt.job, tt.condType)
			if gotState != tt.wantState {
				t.Errorf("summarizeJob() state = %v, want %v", gotState, tt.wantState)
			}
			if gotCond.Status != tt.wantStatus {
				t.Errorf("summarizeJob() condition status = %v, want %v", gotCond.Status, tt.wantStatus)
			}
			if gotCond.Reason != tt.wantReason {
				t.Errorf("summarizeJob() condition reason = %v, want %v", gotCond.Reason, tt.wantReason)
			}
			if gotCond.Type != tt.condType {
				t.Errorf("summarizeJob() condition type = %v, want %v", gotCond.Type, tt.condType)
			}
		})
	}
}
