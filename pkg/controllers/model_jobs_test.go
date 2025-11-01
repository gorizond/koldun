package controllers

import (
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
