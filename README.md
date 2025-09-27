# Koldun Operator

Koldun (kubernetes operator for serverless distributed-llama) manages distributed-llama topologies on Kubernetes using the [Rancher Wrangler](https://github.com/rancher/wrangler) framework. The operator introduces a set of custom resources for orchestrating model distribution, root coordination, and worker execution tuned for the [distributed-llama](https://github.com/b4rtaz/distributed-llama) runtime.

## Custom Resources

- **Dllama** (`koldun.gorizond.io/v1`) — top-level orchestration resource that defines which model to run and the power-of-two fan-out for workers. The controller expands a `Dllama` into its component resources and aggregates status. Before spawning any `Worker` resources it checks that the referenced `Model` reports a non-empty `status.outputPVCName`, then materialises workers using the image from `spec.workerImage`.
- **Model** — tracks acquisition and caching of model artifacts. The controller creates a metadata `ConfigMap`, a `ConfigMap` with a Python downloader script, and a `Job` that installs `huggingface_hub`, `boto3`, `botocore`, `requests` then runs the script to stream artifacts from Hugging Face directly into your S3/MinIO bucket. When conversion succeeds, an additional sizing job mounts the converted PVC, calculates its usage, and publishes the results to `status.conversionSizeBytes`/`status.conversionSizeHuman`.
- **Root** — describes the distributed-llama root coordinator. The controller materialises the runtime as a `Deployment` and `Service` with Wrangler's Apply helpers.
- **Worker** — models an individual distributed-llama worker slot. Each Worker manages a single-replica `Deployment` with slot specific configuration.

## Controllers

All controllers are wired through Wrangler's generic factories and `apply` engine:

- `pkg/controllers/dllama.go` expands Dllama resources into Model/Root/Worker children, applies ownership, and updates the aggregate status once underlying components report ready.
- `pkg/controllers/model.go` orchestrates metadata `ConfigMap` creation, a streamed download `Job`, and an optional post-processing conversion `Job` (e.g. GGUF export + tokenizer pack) that reads artifacts directly from the S3/MinIO cache via an S3 CSI PV/PVC mount.
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
3. **Credentials management** — wire `objectStorage.secretRef` into projected volumes/env to support S3-compatible backends securely.
4. **Scaling policies** — extend `DllamaSpec` with autoscaling hints, resource requests, and GPU scheduling specifics for distributed-llama workloads.
5. **Comprehensive status** — propagate pod phase details, endpoints, and model download progress via Conditions for better observability.
6. **CRD generation** — generate CRDs/manifests (`wrangler generate` or `controller-gen`) so the CR suite can be installed via YAML/Helm.

## Repository Layout

- `cmd/operator/main.go` — entrypoint wiring config, controller registration, and lifecycle.
- `pkg/apis/koldun.gorizond.io/v1/types.go` — API definitions for the custom resources with manual DeepCopy implementations.
- `pkg/controllers/` — Wrangler controllers and shared helpers.

The project currently targets Go 1.21+ and Wrangler v2.1.4.

## Model download from Hugging Face directly to S3

The operator downloads entire model repositories from Hugging Face using `huggingface_hub` and uploads files directly to your S3/MinIO bucket using `boto3`. You provide only a `sourceUrl` and the `localPath` (S3 prefix). Based on the article “Move Your Hugging Face LLM to S3 Like a Pro” [`dev.to/codexmaker/...`](https://dev.to/codexmaker/move-your-hugging-face-llm-to-s3-like-a-pro-without-wasting-local-space-15kp).

Example `Model` resource:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Model
metadata:
  name: hf-convert-script
  namespace: default
spec:
  sourceUrl: https://huggingface.co/Qwen/Qwen3-1.7B
  localPath: s3://my-bucket-model/Qwen/Qwen3-1.7B
  objectStorage:
    endpoint: http://192.168.205.2:32090
    secretRef:
      name: minio-creds
    bucketForSource: my-bucket-model
    bucketForConvert: my-bucket-convert
  pipProxy: http://192.168.205.2:4001
  download:
    memory: "2Gi"
    chunkMaxMiB: 256
    concurrency: 6
    image: python:3.11
    huggingFaceTokenSecretRef:
      name: my-hf
  conversion:
    weightsFloatType: q80
    outputPath: s3://my-bucket-convert
    converterVersion: v0.16.2
    image: python:3.11
    toolsImage: alpine:3.18
    memory: "8Gi"
```

Example `Dllama` referencing the converted model:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Dllama
metadata:
  name: hf-convert-script-topology
  namespace: default
spec:
  modelRef:
    kind: Model
    name: hf-convert-script
  replicaPower: 2
  rootImage: ghcr.io/gorizond/koldun:v0.0.1
  workerImage: ghcr.io/gorizond/koldun:v0.0.1
```

With `replicaPower: 2` the controller waits until `Model/hf-convert-script` reports `status.outputPVCName` and then spawns the root plus `replicaPower*2-1 = 3` Workers all mounting the converted artifacts PVC.

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
- For MinIO endpoints, set `objectStorage.endpoint` (the controller uses path-style addressing via boto3).
- `objectStorage.bucketForConvert` is mounted directly into the conversion job and becomes the working directory for converter scripts.
- Override `conversion.converterVersion` to pin a specific distributed-llama converter release (default `v0.16.2`).
- When `spec.conversion` is defined, the controller runs a follow-up Job that (a) fetches the converter scripts from GitHub, (b) reads the model via an S3 CSI mount at `/mnt/s3`, (c) executes `convert-hf.py`/`convert-tokenizer-hf.py`, and (d) uploads the GGUF + tokenizer bundles back to the target S3 prefix using boto3.

### S3 mount via CSI (github.com/yandex-cloud/k8s-csi-s3)

- The controller creates a static PV and PVC and mounts the PVC at `/mnt/s3` for the conversion container.
- Defaults (overridable via `spec.pv`):
  - PV: `storageClassName: csi-s3`, `accessModes: [ReadWriteMany]`, `persistentVolumeReclaimPolicy: Retain`, `csi.driver: ru.yandex.s3.csi`
  - PV `csi.volumeHandle`: derived from `spec.localPath` (`s3://bucket/prefix` → `bucket/prefix`)
  - CSI secret refs: `name: csi-s3-secret`, `namespace: kube-system` (override with `spec.pv.csiSecretName`/`spec.pv.csiSecretNamespace`)
  - PVC: `storageClassName: ""` (static binding), `accessModes: [ReadWriteMany]`, `requests.storage: <pv.capacity>`

Example `spec.pv`:

```yaml
spec:
  localPath: s3://manualbucket/path
  pv:
    storageClassName: csi-s3
    capacity: 10Gi
    accessModes: [ReadWriteMany]
    reclaimPolicy: Retain
    csiDriver: ru.yandex.s3.csi
    csiMounter: geesefs
    csiOptions: --memory-limit 1000 --dir-mode 0777 --file-mode 0666
    csiSecretName: csi-s3-secret
    csiSecretNamespace: kube-system
    pvcStorageClassName: ""
    pvcCapacity: 10Gi
    pvcAccessModes: [ReadWriteMany]
```

Python pip proxy (optional): set `spec.pipProxy` to an HTTP proxy (e.g., Dragonfly dfdaemon). The operator writes `~/.pip/pip.conf` inside Python containers.

## In Memoriam

I dedicate this repository to my grandfather, Negashev Vyacheslav Ivanovich