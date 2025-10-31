package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
