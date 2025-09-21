# Kold Operator

Kold (kubernetes operator for serverless distributed-llama) Operator manages distributed-llama topologies on Kubernetes using the [Rancher Wrangler](https://github.com/rancher/wrangler) framework. The operator introduces a set of custom resources for orchestrating model distribution, root coordination, and worker execution tuned for the [distributed-llama](https://github.com/b4rtaz/distributed-llama) runtime.

## Custom Resources

- **Dllama** (`kold.gorizond.io/v1`) — top-level orchestration resource that defines which model to run and the power-of-two fan-out for workers. The controller expands a `Dllama` into its component resources and aggregates status.
- **Model** — tracks acquisition and caching of model artifacts. Currently represented by metadata only; a future revision should integrate an actual download/cache job or external storage backend.
- **Root** — describes the distributed-llama root coordinator. The controller materialises the runtime as a `Deployment` and `Service` with Wrangler's Apply helpers.
- **Worker** — models an individual distributed-llama worker slot. Each Worker manages a single-replica `Deployment` with slot specific configuration.

## Controllers

All controllers are wired through Wrangler's generic factories and `apply` engine:

- `pkg/controllers/dllama.go` expands Dllama resources into Model/Root/Worker children, applies ownership, and updates the aggregate status once underlying components report ready.
- `pkg/controllers/model.go` orchestrates metadata `ConfigMap` creation and a streamed download `Job` that pulls HuggingFace artifacts directly into the configured S3/MinIO cache, driving Model status updates.
- `pkg/controllers/root.go` renders the coordinator `Deployment` and associated `Service`, watching Kubernetes workloads to reflect readiness and expose a stable endpoint.
- `pkg/controllers/worker.go` renders worker `Deployments` and tracks pod readiness per slot.

Each reconciliation uses `WithSetID` to ensure obsolete workers are pruned when the replica power changes. Owner references ensure garbage collection when top level resources are deleted.

## Building & Running

```shell
# Build locally
go build ./cmd/operator

# Run against the current kubeconfig
go run ./cmd/operator -kubeconfig ~/.kube/config
```

When running in-cluster the operator defaults to `InClusterConfig` and listens for termination signals via Wrangler's signal helper.

## Next Steps & TODOs

1. **Model caching pipeline** — integrate an actual artifact downloader (e.g. `Job` + PVC or S3-compatible cache) and populate `ModelStatus` with size/checksum data.
2. **Serverless triggers** — implement demand-based activation similar to Knative (the current implementation keeps Deployments running). Explore net-kourier for minimal HTTP routing once activation is in place.
3. **Credentials management** — wire `CacheSpec.SecretRef` into projected volumes/env to support S3-compatible backends securely.
4. **Scaling policies** — extend `DllamaSpec` with autoscaling hints, resource requests, and GPU scheduling specifics for distributed-llama workloads.
5. **Comprehensive status** — propagate pod phase details, endpoints, and model download progress via Conditions for better observability.
6. **CRD generation** — generate CRDs/manifests (`wrangler generate` or `controller-gen`) so the CR suite can be installed via YAML/Helm.

## Repository Layout

- `cmd/operator/main.go` — entrypoint wiring config, controller registration, and lifecycle.
- `pkg/apis/kold.gorizond.io/v1/types.go` — API definitions for the custom resources with manual DeepCopy implementations.
- `pkg/controllers/` — Wrangler controllers and shared helpers.

The project currently targets Go 1.21+ and Wrangler v2.1.4.

## Model download from Hugging Face into S3

The operator downloads entire model repositories from Hugging Face using `huggingface_hub` and then syncs them to your S3/MinIO bucket with `rclone`. You provide only a `sourceUrl` and the `localPath` (S3 prefix).

Example `Model` resource:

```yaml
apiVersion: kold.gorizond.io/v1
kind: Model
metadata:
  name: hf-convert-script
  namespace: default
spec:
  sourceUrl: https://huggingface.co/mistralai/Mistral-7B-v0.3
  localPath: s3://my-bucket-model/mistralai/Mistral-7B-v0.3
  cacheSpec:
    endpoint: https://minio.example.com
    bucket: my-bucket-model
    secretRef:
      name: minio-creds
  download:
    image: python:3.11-alpine
```

Expected Secret for S3/MinIO credentials (env auth, key names are standard for rclone/AWS SDK):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: minio-creds
  namespace: default
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "..."
  AWS_SECRET_ACCESS_KEY: "..."
```

Notes:
- For private Hugging Face repos, set `download.huggingFaceTokenSecretRef` with a key `token`. The downloader will pass `HF_TOKEN` to `huggingface_hub`.
- For MinIO endpoints, the operator configures path-style addressing and `--s3-endpoint` accordingly.
