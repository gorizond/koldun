package controllers

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1 "github.com/gorizond/kold/pkg/apis/kold.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

const (
	// Use python alpine to allow installing huggingface_hub and boto3
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
	if err := h.ensureScriptConfigMap(obj); err != nil {
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

func (h *modelHandler) ensureScriptConfigMap(obj *v1.Model) error {
	scriptName := fmt.Sprintf("%s-download-script", obj.Name)
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      scriptName,
			Namespace: obj.Namespace,
			Labels: map[string]string{
				labelComponent: componentModel,
				labelModelName: obj.Name,
			},
		},
		Data: map[string]string{
			"download.py": `import os
import io
import mimetypes
import requests
from huggingface_hub import HfApi, hf_hub_url
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

repo_url = os.environ.get('SOURCE_URL')
token = os.environ.get('HF_TOKEN')
bucket = os.environ.get('CACHE_BUCKET')
prefix = os.environ.get('CACHE_OBJECT_KEY', '').strip('/')
endpoint = os.environ.get('CACHE_ENDPOINT') or None

# Resolve repo_id from full URL if needed
repo_id = repo_url or ''
hf_prefix = 'https://huggingface.co/'
if repo_id.startswith(hf_prefix):
    repo_id = repo_id[len(hf_prefix):]
repo_id = repo_id.strip('/')

api = HfApi()
files = api.list_repo_files(repo_id=repo_id, repo_type='model')

session = boto3.session.Session()
client = session.client('s3', endpoint_url=endpoint, config=Config(s3={'addressing_style': 'path'}))
transfer_config = TransferConfig(
    multipart_threshold=8*1024*1024,
    multipart_chunksize=8*1024*1024,
    max_concurrency=1,
    use_threads=False,
)

headers = {}
if token:
    headers['Authorization'] = f'Bearer {token}'

for path in files:
    url = hf_hub_url(repo_id=repo_id, filename=path)
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        r.raw.decode_content = True
        key = f"{prefix}/{path}" if prefix else path
        content_type = r.headers.get('Content-Type') or mimetypes.guess_type(path)[0]
        extra = {'ContentType': content_type} if content_type else None
        if extra:
            client.upload_fileobj(r.raw, bucket, key, ExtraArgs=extra, Config=transfer_config)
        else:
            client.upload_fileobj(r.raw, bucket, key, Config=transfer_config)
`,
		},
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

	// If the Job already exists, do not update it to avoid restarting pods.
	// Let it run through backoffLimit attempts and be cleaned up by TTL.
	if existing, err := h.jobs.Cache().Get(obj.Namespace, jobName); err == nil && existing != nil {
		return nil
	}
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

	backoffLimit := int32(10)
	// Keep finished jobs/pods longer to inspect failures; default 1minute
	ttl := int32(60)
	if v, ok := obj.Annotations["kold.gorizond.io/ttl-seconds"]; ok {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			ttl = int32(secs)
		}
	}

	// Build init container: download all artifacts into cache path
	initContainers := []corev1.Container{}
	// mount script configmap into the init container
	downloadContainer := h.buildDownloadContainer(obj, spec, obj.Spec.SourceURL, objectKey)
	downloadContainer.VolumeMounts = append(downloadContainer.VolumeMounts, corev1.VolumeMount{
		Name:      "script",
		MountPath: "/opt/script",
		ReadOnly:  true,
	})
	initContainers = append(initContainers, downloadContainer)

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
					Volumes: []corev1.Volume{
						{
							Name: "script",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: fmt.Sprintf("%s-download-script", obj.Name),
									},
									Optional: pointer.Bool(true),
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    "model-download-finish",
							Image:   spec.Image,
							Command: []string{"/bin/sh", "-c"},
							Args:    []string{"echo download complete"},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
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
		Name:    "model-downloader",
		Image:   spec.Image,
		Command: h.downloadCommand(spec),
		Args:    h.downloadArgs(model, spec, sourceURL, objectKey),
		Env:     env,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
		ImagePullPolicy: corev1.PullIfNotPresent,
	}
}

func (h *modelHandler) downloadCommand(spec *v1.ModelDownloadSpec) []string {
	if len(spec.Command) > 0 {
		return spec.Command
	}
	// run a shell to install deps and execute a small Python + boto3 upload workflow
	return []string{"/bin/sh", "-c"}
}

func (h *modelHandler) downloadArgs(model *v1.Model, spec *v1.ModelDownloadSpec, sourceURL, objectKey string) []string {
	if len(spec.Args) > 0 {
		return spec.Args
	}

	bucket := model.Spec.CacheSpec.Bucket
	endpoint := model.Spec.CacheSpec.Endpoint

	// Construct a script that:
	// 1) Installs ca-certificates, 2) installs huggingface_hub and boto3,
	// 3) snapshots HF repo to /tmp/model, 4) uploads files directly to S3 via boto3
	_ = bucket
	_ = endpoint

	cmd := "set -euo pipefail\n" +
		"python -m pip install --no-cache-dir huggingface_hub boto3 botocore requests\n" +
		"python /opt/script/download.py\n"

	return []string{cmd}
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
