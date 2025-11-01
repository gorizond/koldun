package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	genericfake "github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestModelHandlerOnRelatedJobDeletion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	models := genericfake.NewMockControllerInterface[*v1.Model, *v1.ModelList](ctrl)
	handler := &modelHandler{models: models}

	models.EXPECT().Enqueue("models", "mistral")

	result, err := handler.onRelatedJob("models/mistral-download", nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestModelHandlerOnRelatedJobStatusChange(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
