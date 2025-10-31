package controllers

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"k8s.io/klog/v2"
)

// Object Storage functions
//
// This file contains functions for interacting with S3/MinIO object storage:
// - Bucket creation and management
// - S3 path parsing and key generation
// - Conversion path resolution

// ensureObjectStorageBuckets creates the necessary object storage buckets if configured.
// It reads credentials from the referenced Secret and ensures buckets exist for both
// source downloads and converted models.
func (h *modelHandler) ensureObjectStorageBuckets(obj *v1.Model) error {
	if !h.ensureBuckets {
		return nil
	}

	storage := obj.Spec.ObjectStorage
	if storage == nil {
		return nil
	}

	endpoint := strings.TrimSpace(storage.Endpoint)
	buckets := uniqueNonEmpty(storage.BucketForSource, storage.BucketForConvert)
	if endpoint == "" || len(buckets) == 0 {
		return nil
	}

	if storage.SecretRef == nil {
		klog.V(2).Infof("Model %s/%s: objectStorage.secretRef not set, skipping bucket ensure", obj.Namespace, obj.Name)
		return nil
	}

	secretNamespace := storage.SecretRef.Namespace
	if strings.TrimSpace(secretNamespace) == "" {
		secretNamespace = obj.Namespace
	}

	secret, err := h.secrets.Cache().Get(secretNamespace, storage.SecretRef.Name)
	if err != nil {
		return fmt.Errorf("fetch object storage secret %s/%s: %w", secretNamespace, storage.SecretRef.Name, err)
	}

	accessKey := valueFromSecret(secret, "AWS_ACCESS_KEY_ID", "aws_access_key_id")
	secretKey := valueFromSecret(secret, "AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")
	sessionToken := valueFromSecret(secret, "AWS_SESSION_TOKEN", "aws_session_token")
	region := valueFromSecret(secret, "AWS_REGION", "AWS_DEFAULT_REGION", "aws_region", "aws_default_region")

	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("secret %s/%s missing AWS credentials", secretNamespace, storage.SecretRef.Name)
	}

	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("parse object storage endpoint %q: %w", endpoint, err)
	}
	host := parsedEndpoint.Host
	if host == "" {
		host = parsedEndpoint.Path
	}
	if host == "" {
		return fmt.Errorf("object storage endpoint %q missing host", endpoint)
	}

	secure := true
	switch strings.ToLower(parsedEndpoint.Scheme) {
	case "http":
		secure = false
	case "https", "":
		secure = true
	default:
		secure = parsedEndpoint.Scheme != "http"
	}

	client, err := minio.New(host, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, sessionToken),
		Secure:       secure,
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return fmt.Errorf("initialise object storage client: %w", err)
	}

	for _, bucket := range buckets {
		ctx, cancel := context.WithTimeout(h.ctx, bucketEnsureTimeout)
		exists, err := client.BucketExists(ctx, bucket)
		cancel()
		if err != nil {
			return fmt.Errorf("check bucket %s existence: %w", bucket, err)
		}
		if exists {
			continue
		}

		opts := minio.MakeBucketOptions{}
		if region != "" {
			opts.Region = region
		}
		ctx, cancel = context.WithTimeout(h.ctx, bucketEnsureTimeout)
		err = client.MakeBucket(ctx, bucket, opts)
		cancel()
		if err != nil {
			resp := minio.ToErrorResponse(err)
			if resp.Code == "BucketAlreadyOwnedByYou" || resp.Code == "BucketAlreadyExists" {
				klog.V(2).Infof("Model %s/%s: bucket %s already present", obj.Namespace, obj.Name, bucket)
				continue
			}
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		klog.Infof("Model %s/%s: created object storage bucket %s", obj.Namespace, obj.Name, bucket)
	}

	return nil
}

// modelObjectKey extracts the object key from the model's LocalPath.
// If LocalPath is an s3:// URI, it extracts the key portion.
// Otherwise, it treats the path as a relative key.
func modelObjectKey(model *v1.Model) string {
	pathValue := strings.TrimSpace(model.Spec.LocalPath)
	if pathValue == "" {
		return ""
	}
	if _, key, ok := parseS3Path(pathValue); ok {
		return strings.TrimLeft(key, "/")
	}
	return strings.TrimLeft(pathValue, "/")
}

// conversionPaths resolves paths and locations for model conversion.
// It returns:
// - workDir: the working directory inside the conversion container
// - bucket: the S3 bucket for the converted model
// - key: the S3 object key for the converted model
// - uri: the full s3:// URI for the converted model
//
// The function handles custom output paths, default paths, and S3 URI parsing.
func conversionPaths(model *v1.Model, spec *v1.ModelConversionSpec, defaultInputKey string) (workDir string, bucket string, key string, uri string) {
	workDir = "/workspace/hf"
	storage := model.Spec.ObjectStorage
	if storage != nil {
		bucket = strings.TrimSpace(storage.BucketForConvert)
		if bucket == "" {
			bucket = storage.BucketForSource
		}
	}
	baseKey := strings.TrimLeft(path.Join(defaultInputKey, "converted", spec.WeightsFloatType), "/")
	if spec.WeightsFloatType == "" {
		baseKey = strings.TrimLeft(path.Join(defaultInputKey, "converted", defaultWeightsType), "/")
	}

	if strings.TrimSpace(spec.OutputPath) != "" {
		if b, k, ok := parseS3Path(spec.OutputPath); ok {
			if b != "" {
				bucket = b
			}
			if k != "" {
				baseKey = strings.TrimLeft(k, "/")
			} else {
				baseKey = ""
			}
		} else {
			clean := spec.OutputPath
			if !strings.HasPrefix(clean, "/") {
				clean = filepath.Join("/workspace", clean)
			}
			workDir = filepath.Clean(clean)
		}
	}

	key = strings.TrimLeft(baseKey, "/")
	if key != "" {
		key = strings.TrimLeft(path.Join(key, model.Name), "/")
	} else {
		key = strings.TrimLeft(path.Join(defaultInputKey, "converted", model.Name), "/")
	}

	trimmedKey := strings.TrimLeft(key, "/")
	if bucket != "" && trimmedKey != "" {
		uri = fmt.Sprintf("s3://%s/%s", bucket, trimmedKey)
	} else if bucket != "" {
		uri = fmt.Sprintf("s3://%s", bucket)
	}
	return workDir, bucket, trimmedKey, uri
}

// splitKey splits an object key into its first component and the remainder.
// For example, "models/llama/weights" returns ("models", "llama/weights").
func splitKey(key string) (string, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// parseS3Path parses an S3 URI (s3://bucket/key) into its components.
// Returns bucket, key, and ok=true if the value is a valid S3 URI.
// The key is returned without leading slashes.
func parseS3Path(value string) (bucket string, key string, ok bool) {
	if !strings.HasPrefix(value, "s3://") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(value, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return "", "", false
	}
	bucket = parts[0]
	if len(parts) > 1 {
		key = strings.TrimLeft(parts[1], "/")
	}
	return bucket, key, true
}
