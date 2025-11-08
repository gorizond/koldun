package controllers

import (
	"errors"
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestParseSizeMeasurement(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    *sizeMeasurement
		wantErr bool
	}{
		{
			name:    "valid measurement",
			payload: `{"bytes":1024,"human":"1 KiB"}`,
			want: &sizeMeasurement{
				Bytes: 1024,
				Human: "1 KiB",
			},
			wantErr: false,
		},
		{
			name:    "valid measurement with large bytes",
			payload: `{"bytes":1073741824,"human":"1 GiB"}`,
			want: &sizeMeasurement{
				Bytes: 1073741824,
				Human: "1 GiB",
			},
			wantErr: false,
		},
		{
			name:    "empty human field",
			payload: `{"bytes":512}`,
			want: &sizeMeasurement{
				Bytes: 512,
				Human: "",
			},
			wantErr: false,
		},
		{
			name:    "invalid json",
			payload: `{invalid}`,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty string",
			payload: ``,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "null json",
			payload: `null`,
			want:    &sizeMeasurement{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSizeMeasurement(tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSizeMeasurement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if got.Bytes != tt.want.Bytes {
				t.Errorf("parseSizeMeasurement().Bytes = %v, want %v", got.Bytes, tt.want.Bytes)
			}
			if got.Human != tt.want.Human {
				t.Errorf("parseSizeMeasurement().Human = %v, want %v", got.Human, tt.want.Human)
			}
		})
	}
}

func TestHasCondition(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   "Ready",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Available",
			Status: metav1.ConditionFalse,
		},
	}

	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		want       bool
	}{
		{
			name:       "condition exists",
			conditions: conditions,
			condType:   "Ready",
			want:       true,
		},
		{
			name:       "condition exists - second item",
			conditions: conditions,
			condType:   "Available",
			want:       true,
		},
		{
			name:       "condition does not exist",
			conditions: conditions,
			condType:   "Unknown",
			want:       false,
		},
		{
			name:       "empty conditions",
			conditions: []metav1.Condition{},
			condType:   "Ready",
			want:       false,
		},
		{
			name:       "nil conditions",
			conditions: nil,
			condType:   "Ready",
			want:       false,
		},
		{
			name:       "empty condType",
			conditions: conditions,
			condType:   "",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCondition(tt.conditions, tt.condType)
			if got != tt.want {
				t.Errorf("hasCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectSizeMeasurement(t *testing.T) {

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	podsCache := genericfake.NewMockCacheInterface[*corev1.Pod](ctrl)
	pods := genericfake.NewMockControllerInterface[*corev1.Pod, *corev1.PodList](ctrl)

	handler := &modelHandler{
		pods: pods,
	}

	tests := []struct {
		name      string
		namespace string
		jobName   string
		setupMock func()
		want      *sizeMeasurement
		wantErr   bool
	}{
		{
			name:      "pod with terminated message",
			namespace: "default",
			jobName:   "test-job",
			setupMock: func() {
				pods.EXPECT().Cache().Return(podsCache)
				podsCache.EXPECT().List("default", gomock.Any()).Return([]*corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-job-pod",
							Namespace: "default",
							Labels:    map[string]string{"job-name": "test-job"},
						},
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{
									State: corev1.ContainerState{
										Terminated: &corev1.ContainerStateTerminated{
											Message: `{"bytes":1024,"human":"1 KiB"}`,
										},
									},
								},
							},
						},
					},
				}, nil)
			},
			want: &sizeMeasurement{
				Bytes: 1024,
				Human: "1 KiB",
			},
			wantErr: false,
		},
		{
			name:      "no terminated message",
			namespace: "default",
			jobName:   "test-job",
			setupMock: func() {
				pods.EXPECT().Cache().Return(podsCache)
				podsCache.EXPECT().List("default", gomock.Any()).Return([]*corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-job-pod",
							Namespace: "default",
							Labels:    map[string]string{"job-name": "test-job"},
						},
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{
									State: corev1.ContainerState{
										Running: &corev1.ContainerStateRunning{},
									},
								},
							},
						},
					},
				}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "list pods error",
			namespace: "default",
			jobName:   "test-job",
			setupMock: func() {
				pods.EXPECT().Cache().Return(podsCache)
				podsCache.EXPECT().List("default", gomock.Any()).Return(nil, errors.New("list failed"))
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:      "no pods",
			namespace: "default",
			jobName:   "test-job",
			setupMock: func() {
				pods.EXPECT().Cache().Return(podsCache)
				podsCache.EXPECT().List("default", gomock.Any()).Return([]*corev1.Pod{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "empty terminated message",
			namespace: "default",
			jobName:   "test-job",
			setupMock: func() {
				pods.EXPECT().Cache().Return(podsCache)
				podsCache.EXPECT().List("default", gomock.Any()).Return([]*corev1.Pod{
					{
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{
									State: corev1.ContainerState{
										Terminated: &corev1.ContainerStateTerminated{
											Message: "  ",
										},
									},
								},
							},
						},
					},
				}, nil)
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()
			got, err := handler.collectSizeMeasurement(tt.namespace, tt.jobName)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectSizeMeasurement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.want == nil && got != nil {
				t.Errorf("collectSizeMeasurement() = %v, want nil", got)
				return
			}
			if tt.want != nil && got == nil {
				t.Errorf("collectSizeMeasurement() = nil, want %v", tt.want)
				return
			}
			if tt.want != nil && got != nil {
				if got.Bytes != tt.want.Bytes || got.Human != tt.want.Human {
					t.Errorf("collectSizeMeasurement() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestReuseExistingSizeMeasurement(t *testing.T) {
	tests := []struct {
		name string
		obj  *v1.Model
		upd  *v1.Model
		cond *metav1.Condition
		want bool
	}{
		{
			name: "valid reuse - has bytes and human",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: v1.ModelStatus{
					ConversionSizeBytes:      1024,
					ConversionSizeHuman:      "1 KiB",
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: true,
		},
		{
			name: "valid reuse - only bytes no human",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: v1.ModelStatus{
					ConversionSizeBytes:      512,
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: true,
		},
		{
			name: "generation mismatch",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 2,
				},
				Status: v1.ModelStatus{
					ConversionSizeBytes:      1024,
					ConversionSizeHuman:      "1 KiB",
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "no size data",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: v1.ModelStatus{
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "nil obj",
			obj:  nil,
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "nil updated",
			obj:  &v1.Model{},
			upd:  nil,
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "nil condition",
			obj:  &v1.Model{},
			upd:  &v1.Model{},
			cond: nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reuseExistingSizeMeasurement(tt.obj, tt.upd, tt.cond)
			if got != tt.want {
				t.Errorf("reuseExistingSizeMeasurement() = %v, want %v", got, tt.want)
				return
			}

			// Verify condition and status are set correctly when reuse is successful
			if got && tt.cond != nil {
				if tt.cond.Status != metav1.ConditionTrue {
					t.Errorf("condition Status = %v, want True", tt.cond.Status)
				}
				if tt.cond.Reason != "SizingSucceeded" {
					t.Errorf("condition Reason = %v, want SizingSucceeded", tt.cond.Reason)
				}
				if tt.upd != nil && tt.obj != nil {
					if tt.upd.Status.ConversionSizeBytes != tt.obj.Status.ConversionSizeBytes {
						t.Errorf("updated bytes not copied correctly")
					}
					if tt.upd.Status.ConversionSizeGeneration != tt.obj.Status.ConversionSizeGeneration {
						t.Errorf("updated generation not copied correctly")
					}
				}
			}
		})
	}
}

func TestEnsureStatusMissingConfiguration(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		models: models,
	}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "",
		},
	}
	model.Generation = 2

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		if updated.Status.ObservedGeneration != updated.Generation {
			t.Fatalf("ObservedGeneration not updated: got %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
		}
		downloadCond := meta.FindStatusCondition(updated.Status.Conditions, conditionDownloaded)
		if downloadCond == nil {
			t.Fatalf("expected Downloaded condition to be set")
		}
		if downloadCond.Reason != "ConfigurationMissing" {
			t.Fatalf("unexpected download condition reason: %s", downloadCond.Reason)
		}
		if downloadCond.Status != metav1.ConditionFalse {
			t.Fatalf("download condition status = %s, want False", downloadCond.Status)
		}
		if downloadCond.Message == "" {
			t.Fatalf("expected download condition message to be populated")
		}
		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil {
			t.Fatalf("expected Ready condition to be set")
		}
		if readyCond.Reason != "ConfigurationMissing" {
			t.Fatalf("unexpected ready condition reason: %s", readyCond.Reason)
		}
		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	result, err := handler.ensureStatus(model)
	if err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
	if result == nil {
		t.Fatalf("ensureStatus returned nil result")
	}
}

func TestEnsureStatusReuseSucceededDownloadWhenJobMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		models: models,
		jobs:   jobs,
	}

	jobs.EXPECT().Cache().Return(jobsCache)
	jobsCache.EXPECT().Get("models", "alpha-download").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-download"))
	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		downloadCond := meta.FindStatusCondition(updated.Status.Conditions, conditionDownloaded)
		if downloadCond == nil {
			t.Fatalf("expected Downloaded condition to be set")
		}
		if downloadCond.Status != metav1.ConditionTrue {
			t.Fatalf("download condition status = %s, want True", downloadCond.Status)
		}
		if downloadCond.Reason != "JobSucceeded" {
			t.Fatalf("unexpected download condition reason: %s", downloadCond.Reason)
		}
		if updated.Status.DownloadState != "Succeeded" {
			t.Fatalf("download state = %s, want Succeeded", updated.Status.DownloadState)
		}
		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil {
			t.Fatalf("expected Ready condition to be set")
		}
		if readyCond.Status != metav1.ConditionTrue || readyCond.Reason != "ArtifactsReady" {
			t.Fatalf("unexpected ready condition: status=%s reason=%s", readyCond.Status, readyCond.Reason)
		}
		if updated.Status.DownloadJobName != "" {
			t.Fatalf("expected DownloadJobName to be empty, got %q", updated.Status.DownloadJobName)
		}
		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Succeeded",
			ObservedGeneration: 5,
		},
	}
	model.Generation = 5

	result, err := handler.ensureStatus(model)
	if err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
	if result == nil {
		t.Fatalf("ensureStatus returned nil result")
	}
}

func TestEnsureStatusConversionAndSizingSuccessClearsForceAnnotation(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	podsCache := genericfake.NewMockCacheInterface[*corev1.Pod](ctrl)
	pods := genericfake.NewMockControllerInterface[*corev1.Pod, *corev1.PodList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		models: models,
		jobs:   jobs,
		pods:   pods,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	gomock.InOrder(
		jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "alpha-download",
				Namespace: "models",
			},
			Status: batchv1.JobStatus{Succeeded: 1},
		}, nil),
		jobsCache.EXPECT().Get("models", "alpha-convert").Return(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "alpha-convert",
				Namespace: "models",
			},
			Status: batchv1.JobStatus{Succeeded: 1},
		}, nil),
		jobsCache.EXPECT().Get("models", "alpha-size").Return(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "alpha-size",
				Namespace: "models",
			},
			Status: batchv1.JobStatus{Succeeded: 1},
		}, nil),
	)

	pods.EXPECT().Cache().Return(podsCache)
	podsCache.EXPECT().List("models", gomock.Any()).Return([]*corev1.Pod{
		{
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								Message: `{"bytes":2048,"human":"2 KiB"}`,
							},
						},
					},
				},
			},
		},
	}, nil)

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		if updated.Status.DownloadJobName != "alpha-download" {
			t.Fatalf("DownloadJobName = %q, want alpha-download", updated.Status.DownloadJobName)
		}
		if updated.Status.ConversionJobName != "alpha-convert" {
			t.Fatalf("ConversionJobName = %q, want alpha-convert", updated.Status.ConversionJobName)
		}
		if updated.Status.ConversionSizeJobName != "alpha-size" {
			t.Fatalf("ConversionSizeJobName = %q, want alpha-size", updated.Status.ConversionSizeJobName)
		}
		if updated.Status.OutputPVCName != "alpha-s3-output-pvc" {
			t.Fatalf("OutputPVCName = %q, want alpha-s3-output-pvc", updated.Status.OutputPVCName)
		}
		if updated.Status.ConversionSizeBytes != 2048 || updated.Status.ConversionSizeHuman != "2 KiB" {
			t.Fatalf("unexpected size measurement: %d %s", updated.Status.ConversionSizeBytes, updated.Status.ConversionSizeHuman)
		}
		if token := updated.Status.ConversionSizeForceToken; token != "annotation-rv-13" {
			t.Fatalf("ConversionSizeForceToken = %q, want annotation-rv-13", token)
		}
		downloadCond := meta.FindStatusCondition(updated.Status.Conditions, conditionDownloaded)
		if downloadCond == nil || downloadCond.Status != metav1.ConditionTrue {
			t.Fatalf("download condition not ready")
		}
		conversionCond := meta.FindStatusCondition(updated.Status.Conditions, conditionConverted)
		if conversionCond == nil || conversionCond.Status != metav1.ConditionTrue {
			t.Fatalf("conversion condition not ready")
		}
		sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
		if sizeCond == nil || sizeCond.Status != metav1.ConditionTrue {
			t.Fatalf("size condition not ready")
		}
		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil || readyCond.Status != metav1.ConditionTrue || readyCond.Reason != "ConversionReady" {
			t.Fatalf("unexpected ready condition: %#v", readyCond)
		}
		return updated, nil
	})
	models.EXPECT().Update(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		if _, ok := updated.Annotations[annotationForceSizeRerun]; ok {
			t.Fatalf("force-size annotation was not cleared")
		}
		return updated, nil
	})

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "alpha",
			Namespace:       "models",
			ResourceVersion: "13",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "models-bucket",
				BucketForConvert: "convert-bucket",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			DownloadState:            "Running",
			ObservedGeneration:       7,
			ConversionState:          "Running",
			ConversionSizeState:      "Pending",
			ConversionSizeForceToken: "",
		},
	}
	model.Generation = 7
	model.Annotations = map[string]string{
		annotationForceSizeRerun: "",
	}

	result, err := handler.ensureStatus(model)
	if err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
	if result == nil {
		t.Fatalf("ensureStatus returned nil result")
	}
	if _, ok := result.Annotations[annotationForceSizeRerun]; ok {
		t.Fatalf("expected force-size annotation to be removed in final result")
	}
	if result.Status.ConversionSizeForceToken != "annotation-rv-13" {
		t.Fatalf("final ConversionSizeForceToken = %q, want annotation-rv-13", result.Status.ConversionSizeForceToken)
	}
}

func TestEnsureStatusDownloadJobPendingSetsCondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		jobs:   jobs,
		models: models,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha-download",
			Namespace: "models",
		},
		Status: batchv1.JobStatus{},
	}, nil)

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		downloadCond := meta.FindStatusCondition(updated.Status.Conditions, conditionDownloaded)
		if downloadCond == nil {
			t.Fatalf("expected Downloaded condition to be set")
		}
		if downloadCond.Reason != "JobCreated" {
			t.Fatalf("download condition reason = %s, want JobCreated", downloadCond.Reason)
		}
		if downloadCond.Status != metav1.ConditionFalse {
			t.Fatalf("download condition status = %s, want False", downloadCond.Status)
		}

		conversionCond := meta.FindStatusCondition(updated.Status.Conditions, conditionConverted)
		if conversionCond == nil {
			t.Fatalf("expected Converted condition to be set")
		}
		if conversionCond.Reason != "WaitingForDownload" {
			t.Fatalf("conversion condition reason = %s, want WaitingForDownload", conversionCond.Reason)
		}

		sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
		if sizeCond == nil {
			t.Fatalf("expected Sized condition to be set")
		}
		if sizeCond.Reason != "WaitingForConversion" {
			t.Fatalf("size condition reason = %s, want WaitingForConversion", sizeCond.Reason)
		}

		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil {
			t.Fatalf("expected Ready condition to be set")
		}
		if readyCond.Reason != "JobCreated" {
			t.Fatalf("ready condition reason = %s, want JobCreated", readyCond.Reason)
		}
		if readyCond.Message == "" {
			t.Fatalf("expected ready condition message to be populated")
		}

		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 3,
		},
	}
	model.Generation = 3

	if _, err := handler.ensureStatus(model); err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
}

func TestEnsureStatusDownloadFailedPersistsTerminalState(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		jobs:   jobs,
		models: models,
	}

	jobs.EXPECT().Cache().Return(jobsCache)
	jobsCache.EXPECT().Get("models", "alpha-download").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-download"))

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		downloadCond := meta.FindStatusCondition(updated.Status.Conditions, conditionDownloaded)
		if downloadCond == nil {
			t.Fatalf("expected Downloaded condition to be set")
		}
		if downloadCond.Reason != "JobFailed" {
			t.Fatalf("download condition reason = %s, want JobFailed", downloadCond.Reason)
		}
		if downloadCond.Status != metav1.ConditionFalse {
			t.Fatalf("download condition status = %s, want False", downloadCond.Status)
		}
		if downloadCond.Message == "" {
			t.Fatalf("expected download condition message to be populated")
		}

		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil {
			t.Fatalf("expected Ready condition to be set")
		}
		if readyCond.Reason != "JobFailed" {
			t.Fatalf("ready condition reason = %s, want JobFailed", readyCond.Reason)
		}
		if readyCond.Status != metav1.ConditionFalse {
			t.Fatalf("ready condition status = %s, want False", readyCond.Status)
		}

		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource: "models-bucket",
			},
		},
		Status: v1.ModelStatus{
			DownloadState:      "Failed",
			ObservedGeneration: 4,
		},
	}
	model.Generation = 4

	if _, err := handler.ensureStatus(model); err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
}

func TestEnsureStatusConversionJobMissingReusesSucceededState(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		jobs:   jobs,
		models: models,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	gomock.InOrder(
		jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "alpha-download",
				Namespace: "models",
			},
			Status: batchv1.JobStatus{Succeeded: 1},
		}, nil),
		jobsCache.EXPECT().Get("models", "alpha-convert").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-convert")),
		jobsCache.EXPECT().Get("models", "alpha-size").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-size")),
	)

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		downloadCond := meta.FindStatusCondition(updated.Status.Conditions, conditionDownloaded)
		if downloadCond == nil || downloadCond.Status != metav1.ConditionTrue {
			t.Fatalf("expected download condition to be succeeded")
		}
		conversionCond := meta.FindStatusCondition(updated.Status.Conditions, conditionConverted)
		if conversionCond == nil || conversionCond.Status != metav1.ConditionTrue || conversionCond.Reason != "JobSucceeded" {
			t.Fatalf("unexpected conversion condition: %#v", conversionCond)
		}
		sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
		if sizeCond == nil || sizeCond.Status != metav1.ConditionTrue || sizeCond.Reason != "SizingSucceeded" {
			t.Fatalf("unexpected size condition: %#v", sizeCond)
		}
		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil || readyCond.Status != metav1.ConditionTrue || readyCond.Reason != "ConversionReady" {
			t.Fatalf("unexpected ready condition: %#v", readyCond)
		}
		if updated.Status.OutputPVCName != "alpha-s3-output-pvc" {
			t.Fatalf("OutputPVCName = %q, want alpha-s3-output-pvc", updated.Status.OutputPVCName)
		}
		if updated.Status.ConversionSizeBytes != 4096 || updated.Status.ConversionSizeHuman != "4 KiB" {
			t.Fatalf("unexpected size measurement %d %s", updated.Status.ConversionSizeBytes, updated.Status.ConversionSizeHuman)
		}
		if updated.Status.ConversionSizeForceToken != "token-keep" {
			t.Fatalf("ConversionSizeForceToken = %q, want token-keep", updated.Status.ConversionSizeForceToken)
		}
		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "models-bucket",
				BucketForConvert: "convert-bucket",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ObservedGeneration:       5,
			ConversionState:          "Succeeded",
			ConversionSizeState:      "Succeeded",
			ConversionSizeGeneration: 5,
			ConversionSizeBytes:      4096,
			ConversionSizeHuman:      "4 KiB",
			ConversionSizeJobName:    "alpha-size",
			ConversionSizeForceToken: "token-keep",
		},
	}
	model.Generation = 5

	if _, err := handler.ensureStatus(model); err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
}

func TestEnsureStatusConversionJobMissingReusesFailedState(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		jobs:   jobs,
		models: models,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	gomock.InOrder(
		jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "alpha-download",
				Namespace: "models",
			},
			Status: batchv1.JobStatus{Succeeded: 1},
		}, nil),
		jobsCache.EXPECT().Get("models", "alpha-convert").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-convert")),
	)

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		conversionCond := meta.FindStatusCondition(updated.Status.Conditions, conditionConverted)
		if conversionCond == nil || conversionCond.Status != metav1.ConditionFalse || conversionCond.Reason != "JobFailed" {
			t.Fatalf("unexpected conversion condition: %#v", conversionCond)
		}
		sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
		if sizeCond == nil || sizeCond.Reason != "ConversionNotSucceeded" || sizeCond.Status != metav1.ConditionFalse {
			t.Fatalf("unexpected size condition: %#v", sizeCond)
		}
		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil || readyCond.Reason != "JobFailed" || readyCond.Status != metav1.ConditionFalse {
			t.Fatalf("unexpected ready condition: %#v", readyCond)
		}
		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "models-bucket",
				BucketForConvert: "convert-bucket",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 6,
			ConversionState:    "Failed",
		},
	}
	model.Generation = 6

	if _, err := handler.ensureStatus(model); err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
}

func TestEnsureStatusConversionJobMissingAwaitingJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		jobs:   jobs,
		models: models,
	}

	jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
	gomock.InOrder(
		jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "alpha-download",
				Namespace: "models",
			},
			Status: batchv1.JobStatus{Succeeded: 1},
		}, nil),
		jobsCache.EXPECT().Get("models", "alpha-convert").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-convert")),
	)

	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
		conversionCond := meta.FindStatusCondition(updated.Status.Conditions, conditionConverted)
		if conversionCond == nil || conversionCond.Reason != "JobPending" || conversionCond.Status != metav1.ConditionFalse {
			t.Fatalf("unexpected conversion condition: %#v", conversionCond)
		}
		sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
		if sizeCond == nil || sizeCond.Reason != "WaitingForConversion" || sizeCond.Status != metav1.ConditionFalse {
			t.Fatalf("unexpected size condition: %#v", sizeCond)
		}
		readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		if readyCond == nil || readyCond.Reason != "JobPending" || readyCond.Status != metav1.ConditionFalse {
			t.Fatalf("unexpected ready condition: %#v", readyCond)
		}
		return updated, nil
	})
	models.EXPECT().Update(gomock.Any()).Times(0)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model.gguf",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				BucketForSource:  "models-bucket",
				BucketForConvert: "convert-bucket",
			},
			Conversion: &v1.ModelConversionSpec{},
		},
		Status: v1.ModelStatus{
			ObservedGeneration: 9,
			ConversionState:    "",
		},
	}
	model.Generation = 9

	if _, err := handler.ensureStatus(model); err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
}

func TestEnsureStatusSizingHistoricalStateHandling(t *testing.T) {

	cases := []struct {
		name            string
		sizeState       string
		generationMatch bool
		expectedReason  string
		expectedStatus  metav1.ConditionStatus
	}{
		{
			name:            "reuseSucceededPreviousGeneration",
			sizeState:       "Succeeded",
			generationMatch: false,
			expectedReason:  "SizingPending",
			expectedStatus:  metav1.ConditionFalse,
		},
		{
			name:            "failedCurrentGeneration",
			sizeState:       "Failed",
			generationMatch: true,
			expectedReason:  "SizingFailed",
			expectedStatus:  metav1.ConditionFalse,
		},
		{
			name:            "failedPreviousGeneration",
			sizeState:       "Failed",
			generationMatch: false,
			expectedReason:  "SizingPending",
			expectedStatus:  metav1.ConditionFalse,
		},
		{
			name:            "unknownStateDefaultsToPending",
			sizeState:       "Unknown",
			generationMatch: true,
			expectedReason:  "SizingPending",
			expectedStatus:  metav1.ConditionFalse,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
			jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
			models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

			handler := &modelHandler{
				jobs:   jobs,
				models: models,
			}

			jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
			gomock.InOrder(
				jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "alpha-download",
						Namespace: "models",
					},
					Status: batchv1.JobStatus{Succeeded: 1},
				}, nil),
				jobsCache.EXPECT().Get("models", "alpha-convert").Return(&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "alpha-convert",
						Namespace: "models",
					},
					Status: batchv1.JobStatus{Succeeded: 1},
				}, nil),
				jobsCache.EXPECT().Get("models", "alpha-size").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-size")),
			)

			models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
				sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
				if sizeCond == nil {
					t.Fatalf("expected size condition to be set")
				}
				if sizeCond.Reason != tt.expectedReason {
					t.Fatalf("size condition reason = %s, want %s", sizeCond.Reason, tt.expectedReason)
				}
				if sizeCond.Status != tt.expectedStatus {
					t.Fatalf("size condition status = %s, want %s", sizeCond.Status, tt.expectedStatus)
				}
				if tt.sizeState == "Failed" && tt.generationMatch {
					if updated.Status.ConversionSizeForceToken != "force-me" {
						t.Fatalf("ConversionSizeForceToken = %q, want force-me", updated.Status.ConversionSizeForceToken)
					}
				}
				if tt.sizeState == "Failed" && !tt.generationMatch {
					if updated.Status.ConversionSizeForceToken != "" {
						t.Fatalf("ConversionSizeForceToken = %q, want empty for previous generation", updated.Status.ConversionSizeForceToken)
					}
				}
				return updated, nil
			})
			models.EXPECT().Update(gomock.Any()).Times(0)

			gen := int64(12)
			prevGen := gen
			if !tt.generationMatch {
				prevGen = gen - 1
			}

			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "alpha",
					Namespace: "models",
				},
				Spec: v1.ModelSpec{
					SourceURL: "https://example.com/model.gguf",
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForSource:  "models-bucket",
						BucketForConvert: "convert-bucket",
					},
					Conversion: &v1.ModelConversionSpec{},
				},
				Status: v1.ModelStatus{
					ObservedGeneration:       gen,
					ConversionState:          "Succeeded",
					ConversionSizeState:      tt.sizeState,
					ConversionSizeGeneration: prevGen,
					ConversionSizeBytes:      3072,
					ConversionSizeHuman:      "3 KiB",
					ConversionSizeForceToken: "force-me",
				},
			}
			model.Generation = gen

			if _, err := handler.ensureStatus(model); err != nil {
				t.Fatalf("ensureStatus returned error: %v", err)
			}
		})
	}
}

func TestEnsureStatusSizingMeasurementFallbacks(t *testing.T) {

	type expectation struct {
		reason string
		status metav1.ConditionStatus
		state  string
	}

	cases := []struct {
		name      string
		podsErr   error
		message   string
		reuseData bool
		want      expectation
	}{
		{
			name:      "measurementErrorReusesExistingData",
			podsErr:   errors.New("pod list failed"),
			reuseData: true,
			want: expectation{
				reason: "SizingSucceeded",
				status: metav1.ConditionTrue,
				state:  "Succeeded",
			},
		},
		{
			name:      "measurementErrorWithoutHistoryFails",
			podsErr:   errors.New("pod list failed"),
			reuseData: false,
			want: expectation{
				reason: "ResultCollectionFailed",
				status: metav1.ConditionFalse,
				state:  "Failed",
			},
		},
		{
			name:      "measurementMissingMessageReusesExistingData",
			message:   "",
			reuseData: true,
			want: expectation{
				reason: "SizingSucceeded",
				status: metav1.ConditionTrue,
				state:  "Succeeded",
			},
		},
		{
			name:      "measurementMissingMessagePending",
			message:   "",
			reuseData: false,
			want: expectation{
				reason: "ResultPending",
				status: metav1.ConditionFalse,
				state:  "Pending",
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
			jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
			podsCache := genericfake.NewMockCacheInterface[*corev1.Pod](ctrl)
			pods := genericfake.NewMockControllerInterface[*corev1.Pod, *corev1.PodList](ctrl)
			models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

			handler := &modelHandler{
				jobs:   jobs,
				pods:   pods,
				models: models,
			}

			jobs.EXPECT().Cache().Return(jobsCache).AnyTimes()
			gomock.InOrder(
				jobsCache.EXPECT().Get("models", "alpha-download").Return(&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "alpha-download",
						Namespace: "models",
					},
					Status: batchv1.JobStatus{Succeeded: 1},
				}, nil),
				jobsCache.EXPECT().Get("models", "alpha-convert").Return(&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "alpha-convert",
						Namespace: "models",
					},
					Status: batchv1.JobStatus{Succeeded: 1},
				}, nil),
				jobsCache.EXPECT().Get("models", "alpha-size").Return(&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "alpha-size",
						Namespace: "models",
					},
					Status: batchv1.JobStatus{Succeeded: 1},
				}, nil),
			)

			pods.EXPECT().Cache().Return(podsCache)
			if tt.podsErr != nil {
				podsCache.EXPECT().List("models", gomock.Any()).Return(nil, tt.podsErr)
			} else {
				podsCache.EXPECT().List("models", gomock.Any()).Return([]*corev1.Pod{
					{
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{
									State: corev1.ContainerState{
										Terminated: &corev1.ContainerStateTerminated{
											Message: tt.message,
										},
									},
								},
							},
						},
					},
				}, nil)
			}

			models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).DoAndReturn(func(updated *v1.Model) (*v1.Model, error) {
				sizeCond := meta.FindStatusCondition(updated.Status.Conditions, conditionSized)
				if sizeCond == nil {
					t.Fatalf("expected size condition to be present")
				}
				if sizeCond.Reason != tt.want.reason {
					t.Fatalf("size condition reason = %s, want %s", sizeCond.Reason, tt.want.reason)
				}
				if sizeCond.Status != tt.want.status {
					t.Fatalf("size condition status = %s, want %s", sizeCond.Status, tt.want.status)
				}
				if updated.Status.ConversionSizeState != tt.want.state {
					t.Fatalf("ConversionSizeState = %s, want %s", updated.Status.ConversionSizeState, tt.want.state)
				}
				return updated, nil
			})
			models.EXPECT().Update(gomock.Any()).Times(0)

			gen := int64(15)
			sizeGen := gen
			sizeBytes := int64(8192)
			sizeHuman := "8 KiB"
			if !tt.reuseData {
				sizeGen = gen - 1
				sizeBytes = 0
				sizeHuman = ""
			}

			model := &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "alpha",
					Namespace: "models",
				},
				Spec: v1.ModelSpec{
					SourceURL: "https://example.com/model.gguf",
					ObjectStorage: &v1.ModelObjectStorageSpec{
						BucketForSource:  "models-bucket",
						BucketForConvert: "convert-bucket",
					},
					Conversion: &v1.ModelConversionSpec{},
				},
				Status: v1.ModelStatus{
					ObservedGeneration:       gen,
					ConversionState:          "Running",
					ConversionSizeState:      "Pending",
					ConversionSizeGeneration: sizeGen,
					ConversionSizeBytes:      sizeBytes,
					ConversionSizeHuman:      sizeHuman,
				},
			}
			model.Generation = gen

			if _, err := handler.ensureStatus(model); err != nil {
				t.Fatalf("ensureStatus returned error: %v", err)
			}
		})
	}
}

func TestEnsureStatusNoChangesReturnsOriginalObject(t *testing.T) {
	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
		Spec: v1.ModelSpec{},
		Status: v1.ModelStatus{
			ObservedGeneration:  1,
			DownloadState:       "Pending",
			ConversionState:     "NotRequested",
			ConversionSizeState: "NotRequested",
			Conditions: []metav1.Condition{
				{
					Type:    conditionDownloaded,
					Status:  metav1.ConditionFalse,
					Reason:  "ConfigurationMissing",
					Message: "Model requires sourceUrl and objectStorage.bucketForSource",
				},
				{
					Type:    conditionConverted,
					Status:  metav1.ConditionFalse,
					Reason:  "ConversionNotRequested",
					Message: "Model spec.conversion is not configured",
				},
				{
					Type:    conditionSized,
					Status:  metav1.ConditionFalse,
					Reason:  "SizingNotRequested",
					Message: "Model conversion sizing is not configured",
				},
				{
					Type:    conditionReady,
					Status:  metav1.ConditionFalse,
					Reason:  "ConfigurationMissing",
					Message: "Model requires sourceUrl and objectStorage.bucketForSource",
				},
			},
		},
	}
	model.Generation = 1

	handler := &modelHandler{}

	result, err := handler.ensureStatus(model)
	if err != nil {
		t.Fatalf("ensureStatus returned error: %v", err)
	}
	if result != model {
		t.Fatalf("expected ensureStatus to return original object when nothing changed")
	}
}

func TestEnsureStatusPropagatesUpdateStatusError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	jobsCache := genericfake.NewMockCacheInterface[*batchv1.Job](ctrl)
	jobs := genericfake.NewMockControllerInterface[*batchv1.Job, *batchv1.JobList](ctrl)
	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)

	handler := &modelHandler{
		jobs:   jobs,
		models: models,
	}

	jobs.EXPECT().Cache().Return(jobsCache)
	jobsCache.EXPECT().Get("models", "alpha-download").Return(nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "alpha-download"))

	updateErr := errors.New("update-status failed")
	models.EXPECT().UpdateStatus(gomock.AssignableToTypeOf(&v1.Model{})).Return(nil, updateErr)

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alpha",
			Namespace: "models",
		},
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
	model.Generation = 2

	if _, err := handler.ensureStatus(model); err != updateErr {
		t.Fatalf("expected ensureStatus to return %v, got %v", updateErr, err)
	}
}
