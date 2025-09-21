package controllers

import (
	"context"
	"fmt"
	"strings"

	v1 "github.com/gorizond/kold/pkg/apis/kold.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

const (
    // Use python alpine to allow installing huggingface_hub, and rclone via apk
    defaultDownloadImage = "python:3.11-alpine"
)

type modelHandler struct {
	ctx    context.Context
	apply  apply.Apply
	models generic.ControllerInterface[*v1.Model, *v1.ModelList]
	jobs   generic.ControllerInterface[*batchv1.Job, *batchv1.JobList]
}

func registerModelController(ctx context.Context, m *Manager) error {
	handler := &modelHandler{
		ctx:    ctx,
		apply:  m.Apply(ctx),
		models: m.Kold.Model(),
		jobs:   m.Batch.Job(),
	}

	handler.models.OnChange(ctx, "kold-model-controller", handler.onChange)
	handler.models.OnRemove(ctx, "kold-model-controller", handler.onRemove)
	handler.jobs.OnChange(ctx, "kold-model-job-watch", handler.onRelatedJob)
	handler.jobs.OnRemove(ctx, "kold-model-job-remove", handler.onRelatedJob)
	return nil
}

func (h *modelHandler) onChange(key string, obj *v1.Model) (*v1.Model, error) {
	if obj == nil {
		return nil, nil
	}
	if obj.DeletionTimestamp != nil {
		return obj, nil
	}

	if err := h.ensureMetadataConfigMap(obj); err != nil {
		return obj, err
	}
	if err := h.ensureDownloadJob(obj); err != nil {
		return obj, err
	}

	return h.ensureStatus(obj)
}

func (h *modelHandler) onRemove(key string, obj *v1.Model) (*v1.Model, error) {
	return obj, nil
}

func (h *modelHandler) onRelatedJob(key string, job *batchv1.Job) (*batchv1.Job, error) {
	if job == nil {
		namespace, jobName := splitKey(key)
		if namespace == "" || jobName == "" {
			return nil, nil
		}
		modelName := strings.TrimSuffix(jobName, "-download")
		if modelName == "" || modelName == jobName {
			return nil, nil
		}
		h.models.Enqueue(namespace, modelName)
		return nil, nil
	}
	if job.Labels[labelComponent] != componentModel {
		return job, nil
	}
	modelName := job.Labels[labelModelName]
	if modelName == "" {
		return job, nil
	}
	h.models.Enqueue(job.Namespace, modelName)
	return job, nil
}

func (h *modelHandler) ensureMetadataConfigMap(obj *v1.Model) error {
	metadataName := fmt.Sprintf("%s-metadata", obj.Name)
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      metadataName,
			Namespace: obj.Namespace,
			Labels: map[string]string{
				labelComponent: componentModel,
				labelModelName: obj.Name,
			},
		},
        Data: map[string]string{
            "sourceUrl": obj.Spec.SourceURL,
            "localPath": obj.Spec.LocalPath,
        },
	}

	if obj.Spec.CacheSpec != nil {
		cm.Data["cacheEndpoint"] = obj.Spec.CacheSpec.Endpoint
		cm.Data["cacheBucket"] = obj.Spec.CacheSpec.Bucket
		if obj.Spec.CacheSpec.SecretRef != nil {
			cm.Data["cacheSecret"] = obj.Spec.CacheSpec.SecretRef.Name
		}
	}

	return h.apply.WithOwner(obj).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(cm)
}

func (h *modelHandler) ensureDownloadJob(obj *v1.Model) error {
    if obj.Spec.CacheSpec == nil || obj.Spec.CacheSpec.Bucket == "" || obj.Spec.SourceURL == "" {
		return nil
	}

	spec := effectiveDownloadSpec(obj.Spec.Download)
	jobName := jobNameForModel(obj)
	labels := map[string]string{
		labelComponent: componentModel,
		labelModelName: obj.Name,
	}
	if dllama := labelValue(obj.Labels, labelDllamaName); dllama != "" {
		labels[labelDllamaName] = dllama
	}

    objectKey := modelObjectKey(obj)
	if objectKey == "" {
        objectKey = fmt.Sprintf("models/%s", obj.Name)
	}

	backoffLimit := int32(1)
	ttl := int32(3600)

    // Build init container: download all artifacts into cache path
    initContainers := []corev1.Container{}
    initContainers = append(initContainers, h.buildDownloadContainer(obj, spec, obj.Spec.SourceURL, objectKey))

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: batchv1.SchemeGroupVersion.String(),
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: obj.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:  corev1.RestartPolicyNever,
					InitContainers: initContainers,
                    Containers: []corev1.Container{
						{
							Name:            "model-download-finish",
                            Image:           spec.Image,
							Command:         []string{"/bin/sh", "-c"},
							Args:            []string{"echo download complete"},
							ImagePullPolicy: corev1.PullIfNotPresent,
						},
					},
				},
			},
		},
	}

	// Ensure default service account can list secrets if needed.
	return h.apply.WithOwner(obj).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(obj.Namespace).
		ApplyObjects(job)
}

func (h *modelHandler) buildDownloadContainer(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey string) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "MODEL_NAME", Value: model.Name},
		{Name: "CACHE_BUCKET", Value: model.Spec.CacheSpec.Bucket},
		{Name: "CACHE_OBJECT_KEY", Value: objectKey},
	}
	if model.Spec.CacheSpec.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: "CACHE_ENDPOINT", Value: model.Spec.CacheSpec.Endpoint})
	}
	env = append(env, corev1.EnvVar{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"})

    if spec.HuggingFaceTokenSecretRef != nil {
		if spec.HuggingFaceTokenSecretRef.Namespace != "" && spec.HuggingFaceTokenSecretRef.Namespace != model.Namespace {
			// cross-namespace secret mounts are not permitted; skip with warning via env comment
		} else {
			env = append(env, corev1.EnvVar{
				Name: "HF_TOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: spec.HuggingFaceTokenSecretRef.Name},
						Key:                  "token",
						Optional:             pointer.Bool(true),
					},
				},
			})
		}
	}

    // Pass SOURCE_URL to container
    env = append(env, corev1.EnvVar{Name: "SOURCE_URL", Value: sourceURL})

	if model.Spec.CacheSpec.SecretRef != nil {
		if model.Spec.CacheSpec.SecretRef.Namespace == "" || model.Spec.CacheSpec.SecretRef.Namespace == model.Namespace {
			envFrom := corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: model.Spec.CacheSpec.SecretRef.Name},
					Optional:             pointer.Bool(true),
				},
			}
            return corev1.Container{
                Name:            "model-downloader",
                Image:           spec.Image,
                Command:         h.downloadCommand(spec),
                Args:            h.downloadArgs(model, spec, sourceURL, objectKey),
                Env:             env,
                EnvFrom:         []corev1.EnvFromSource{envFrom},
                ImagePullPolicy: corev1.PullIfNotPresent,
            }
		}
	}

	return corev1.Container{
		Name:            "model-downloader",
		Image:           spec.Image,
		Command:         h.downloadCommand(spec),
		Args:            h.downloadArgs(model, spec, sourceURL, objectKey),
		Env:             env,
		ImagePullPolicy: corev1.PullIfNotPresent,
	}
}

func (h *modelHandler) downloadCommand(spec *v1.ModelDownloadSpec) []string {
    if len(spec.Command) > 0 {
        return spec.Command
    }
    // run a shell to install deps and execute a small Python + rclone workflow
    return []string{"/bin/sh", "-c"}
}

func (h *modelHandler) downloadArgs(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey string) []string {
    if len(spec.Args) > 0 {
        return spec.Args
    }

    bucket := model.Spec.CacheSpec.Bucket
    endpoint := model.Spec.CacheSpec.Endpoint

    // Construct a script that:
    // 1) Installs rclone & build deps, 2) installs huggingface_hub,
    // 3) snapshots HF repo to /tmp/model, 4) rclone sync to s3 remote prefix
    s3Flags := ""
    if endpoint != "" {
        s3Flags = fmt.Sprintf("--s3-endpoint %s --s3-force-path-style", endpoint)
    }

    script := fmt.Sprintf(`set -euo pipefail
apk add --no-cache rclone git ca-certificates
python -m pip install --no-cache-dir --upgrade pip
python -m pip install --no-cache-dir huggingface_hub

mkdir -p /tmp/model
python - <<'PY'
import os
from huggingface_hub import snapshot_download
repo_url = os.environ.get('SOURCE_URL')
target_dir = '/tmp/model'
allow_patterns=None
ignore_patterns=None
# Support both repo-id (org/name) and full URL by stripping prefix
repo_id = repo_url
prefix = 'https://huggingface.co/'
if repo_id.startswith(prefix):
    repo_id = repo_id[len(prefix):]
repo_id = repo_id.strip('/')
token = os.environ.get('HF_TOKEN')
snapshot_download(repo_id=repo_id, local_dir=target_dir, local_dir_use_symlinks=False, token=token)
PY

# Sync whole folder to S3
export AWS_S3_FORCE_PATH_STYLE=true
rclone sync /tmp/model :s3,env_auth=true,acl=private:%s/%s %s --s3-chunk-size 32M --s3-upload-concurrency 4 --transfers 4 --buffer-size 0
`, bucket, objectKey, s3Flags)

    return []string{script}
}

func (h *modelHandler) ensureStatus(obj *v1.Model) (*v1.Model, error) {
	updated := obj.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	downloadCond := metav1.Condition{
		Type:    conditionDownloaded,
		Status:  metav1.ConditionFalse,
		Reason:  "JobNotCreated",
		Message: "Download job has not been created",
	}

	readyCond := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "DownloadPending",
		Message: "Model download is pending",
	}

	state := "Pending"
	jobName := jobNameForModel(obj)
	updated.Status.DownloadJobName = ""

	if obj.Spec.CacheSpec == nil || obj.Spec.CacheSpec.Bucket == "" || obj.Spec.SourceURL == "" {
		downloadCond.Reason = "ConfigurationMissing"
		downloadCond.Message = "Model requires sourceUrl, cacheSpec.bucket"
	} else {
		job, err := h.jobs.Cache().Get(obj.Namespace, jobName)
		if err == nil && job != nil {
			updated.Status.DownloadJobName = job.Name
			state, downloadCond = summarizeJob(job)
			if downloadCond.Status == metav1.ConditionTrue {
				readyCond.Status = metav1.ConditionTrue
				readyCond.Reason = "ArtifactsReady"
				readyCond.Message = "Model artifacts available in cache"
			} else {
				readyCond.Reason = downloadCond.Reason
				readyCond.Message = downloadCond.Message
			}
		} else {
			downloadCond.Reason = "JobPending"
			downloadCond.Message = "Waiting for download job to appear"
		}
	}

	updated.Status.DownloadState = state

	changed := false
	if setCondition(&updated.Status.Conditions, downloadCond) {
		changed = true
	}
	if setCondition(&updated.Status.Conditions, readyCond) {
		changed = true
	}
	if updated.Status.ObservedGeneration != updated.Generation {
		updated.Status.ObservedGeneration = updated.Generation
		changed = true
	}
	if obj.Status.DownloadJobName != updated.Status.DownloadJobName || obj.Status.DownloadState != updated.Status.DownloadState {
		changed = true
	}

	if !changed {
		return obj, nil
	}

	return h.models.UpdateStatus(updated)
}

func effectiveDownloadSpec(spec *v1.ModelDownloadSpec) *v1.ModelDownloadSpec {
	if spec == nil {
		return &v1.ModelDownloadSpec{
			Image: defaultDownloadImage,
		}
	}
	out := spec.DeepCopy()
	if out.Image == "" {
		out.Image = defaultDownloadImage
	}
	return out
}

func jobNameForModel(model *v1.Model) string {
	base := fmt.Sprintf("%s-download", model.Name)
	return truncateName(base, 63)
}

func summarizeJob(job *batchv1.Job) (string, metav1.Condition) {
	cond := metav1.Condition{
		Type:    conditionDownloaded,
		Status:  metav1.ConditionFalse,
		Reason:  "JobPending",
		Message: "Download job is pending",
	}
	state := "Pending"

	for _, jc := range job.Status.Conditions {
		if jc.Type == batchv1.JobFailed && jc.Status == corev1.ConditionTrue {
			cond.Status = metav1.ConditionFalse
			cond.Reason = "JobFailed"
			cond.Message = jc.Message
			state = "Failed"
			return state, cond
		}
		if jc.Type == batchv1.JobComplete && jc.Status == corev1.ConditionTrue {
			cond.Status = metav1.ConditionTrue
			cond.Reason = "JobSucceeded"
			cond.Message = "Model download completed"
			state = "Succeeded"
			return state, cond
		}
	}

	if job.Status.Failed > 0 {
		cond.Reason = "JobFailed"
		cond.Message = "Model download job failed"
		state = "Failed"
		return state, cond
	}
	if job.Status.Succeeded > 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "JobSucceeded"
		cond.Message = "Model download completed"
		state = "Succeeded"
		return state, cond
	}
	if job.Status.Active > 0 {
		cond.Reason = "JobRunning"
		cond.Message = "Model download job is running"
		state = "Running"
		return state, cond
	}

	return state, cond
}

func modelObjectKey(model *v1.Model) string {
	path := strings.TrimSpace(model.Spec.LocalPath)
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "s3://") {
		trimmed := strings.TrimPrefix(path, "s3://")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) == 2 {
			return strings.TrimLeft(parts[1], "/")
		}
		return ""
	}
	return strings.TrimLeft(path, "/")
}

func splitKey(key string) (string, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
