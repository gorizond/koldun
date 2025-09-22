# Kold Operator

Kold (kubernetes operator for serverless distributed-llama) Operator manages distributed-llama topologies on Kubernetes using the [Rancher Wrangler](https://github.com/rancher/wrangler) framework. The operator introduces a set of custom resources for orchestrating model distribution, root coordination, and worker execution tuned for the [distributed-llama](https://github.com/b4rtaz/distributed-llama) runtime.

## Custom Resources

- **Dllama** (`kold.gorizond.io/v1`) — top-level orchestration resource that defines which model to run and the power-of-two fan-out for workers. The controller expands a `Dllama` into its component resources and aggregates status.
- **Model** — tracks acquisition and caching of model artifacts. The controller creates a metadata `ConfigMap`, a `ConfigMap` with a Python downloader script, and a `Job` that installs `huggingface_hub`, `boto3`, `botocore`, `requests` then runs the script to stream artifacts from Hugging Face directly into your S3/MinIO bucket.
- **Root** — describes the distributed-llama root coordinator. The controller materialises the runtime as a `Deployment` and `Service` with Wrangler's Apply helpers.
- **Worker** — models an individual distributed-llama worker slot. Each Worker manages a single-replica `Deployment` with slot specific configuration.

## Controllers

All controllers are wired through Wrangler's generic factories and `apply` engine:

- `pkg/controllers/dllama.go` expands Dllama resources into Model/Root/Worker children, applies ownership, and updates the aggregate status once underlying components report ready.
- `pkg/controllers/model.go` orchestrates metadata `ConfigMap` creation, a streamed download `Job`, and an optional post-processing conversion `Job` (e.g. GGUF export + tokenizer pack) that reads artifacts directly from the S3/MinIO cache via a goofys FUSE sidecar.
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

## Model download from Hugging Face directly to S3

The operator downloads entire model repositories from Hugging Face using `huggingface_hub` and uploads files directly to your S3/MinIO bucket using `boto3`. You provide only a `sourceUrl` and the `localPath` (S3 prefix). Based on the article “Move Your Hugging Face LLM to S3 Like a Pro” [`dev.to/codexmaker/...`](https://dev.to/codexmaker/move-your-hugging-face-llm-to-s3-like-a-pro-without-wasting-local-space-15kp).

Example `Model` resource:

```yaml
apiVersion: kold.gorizond.io/v1
kind: Model
metadata:
  name: hf-convert-script
  namespace: default
spec:
  sourceUrl: https://huggingface.co/Qwen/Qwen3-1.7B
  localPath: s3://my-bucket-model/Qwen/Qwen3-1.7B
  cacheSpec:
    endpoint: http://192.168.205.2:32090
    bucket: my-bucket-model
    secretRef:
      name: minio-creds
  download:
    memory: "2Gi"
    chunkMaxMiB: 256
    concurrency: 6
    image: python:3.11
    huggingFaceTokenSecretRef:
      name: my-hf
  conversion:
    weightsFloatType: q40
    outputPath: /converted
    image: python:3.11
    goofysImage: ghcr.io/kahing/goofys:latest
    toolsImage: alpine:3.18
    memory: "8Gi"
```

Expected Secret for S3/MinIO credentials (AWS SDK standard key names):

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
- For private Hugging Face repos, set `download.huggingFaceTokenSecretRef` with key `token`. The downloader passes `HF_TOKEN` to `huggingface_hub`.
- For MinIO endpoints, set `cacheSpec.endpoint` (the controller uses path-style addressing via boto3).
- When `spec.conversion` is defined, the controller runs a follow-up Job that (a) fetches the converter scripts from GitHub, (b) mounts the cache bucket read-only via a `goofys` sidecar and reads the model directly over FUSE, (c) executes `convert-hf.py`/`convert-tokenizer-hf.py`, and (d) uploads the GGUF + tokenizer bundles back to the target S3 prefix using boto3.

### S3 read-only via goofys

- The conversion Job includes a `goofys` sidecar that mounts the bucket at `/mnt/s3`. The main container reads model files from `"/mnt/s3/<localPath-prefix>"`.
- Credentials are taken from `cacheSpec.secretRef` using standard AWS keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optionally `AWS_REGION`). Path-style is enforced with `AWS_S3_FORCE_PATH_STYLE=true`.
- For custom endpoints (e.g. MinIO), set `spec.cacheSpec.endpoint`.
- Node must provide `/dev/fuse`; the pod mounts it via `hostPath`. The sidecar requests `SYS_ADMIN` capability and allowPrivilegeEscalation to run FUSE.
