package controllers

import (
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestShouldEnqueueModelForJob(t *testing.T) {
	handler := &modelHandler{}
	now := metav1.NewTime(time.Now())
	old := metav1.NewTime(time.Now().Add(-3 * time.Minute))

	tests := []struct {
		name string
		job  *batchv1.Job
		want bool
	}{
		{
			name: "succeeded job",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}},
			want: true,
		},
		{
			name: "failed job",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Failed: 1}},
			want: true,
		},
		{
			name: "active job over threshold",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Active: 1, StartTime: &old}},
			want: true,
		},
		{
			name: "active job without start time",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Active: 1}},
			want: false,
		},
		{
			name: "active job below threshold",
			job:  &batchv1.Job{Status: batchv1.JobStatus{Active: 1, StartTime: &now}},
			want: false,
		},
		{
			name: "pending job",
			job:  &batchv1.Job{Status: batchv1.JobStatus{}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handler.shouldEnqueueModelForJob(tt.job); got != tt.want {
				t.Fatalf("shouldEnqueueModelForJob() = %v, want %v", got, tt.want)
			}
		})
	}
}
