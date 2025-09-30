# Koldun Operator

Koldun (kubernetes operator for serverless distributed-llama) manages distributed-llama topologies on Kubernetes using the [Rancher Wrangler](https://github.com/rancher/wrangler) framework. The operator introduces a set of custom resources for orchestrating model distribution, root coordination, and worker execution tuned for the [distributed-llama](https://github.com/b4rtaz/distributed-llama) runtime.

## Custom Resources

- **Dllama** (`koldun.gorizond.io/v1`) — top-level orchestration resource that defines which model to run and the power-of-two fan-out for workers. The controller expands a `Dllama` into its component resources and aggregates status. Before spawning any `Worker` resources it checks that the referenced `Model` reports a non-empty `status.outputPVCName`, then materialises workers using the image from `spec.workerImage`. **New**: Automatically copies NATS URL from available `Ingress` resources to avoid configuration duplication.
- **Model** — tracks acquisition and caching of model artifacts. The controller creates a metadata `ConfigMap`, a `ConfigMap` with a Python downloader script, and a `Job` that installs `huggingface_hub`, `boto3`, `botocore`, `requests` then runs the script to stream artifacts from Hugging Face directly into your S3/MinIO bucket. When conversion succeeds, an additional sizing job mounts the converted PVC, calculates its usage, and publishes the results to `status.conversionSizeBytes`/`status.conversionSizeHuman`.
- **Root** — describes the distributed-llama root coordinator. The controller materialises the runtime as a `Deployment` and `Service` with Wrangler's Apply helpers.
- **Worker** — models an individual distributed-llama worker slot. Each Worker manages a single-replica `StatefulSet` with slot specific configuration.
- **Ingress** — HTTP entrypoint backed by a managed backend deployment. Contains NATS configuration that can be automatically inherited by `Dllama` resources.

## Features

### Automatic NATS URL Copying

The operator automatically copies NATS URLs from `Ingress` resources to `Dllama` resources during creation and updates:

- **Local-first**: Searches for `Ingress` resources in the same namespace as the `Dllama`
- **Cluster-wide fallback**: If no suitable `Ingress` found locally, searches across all namespaces
- **Automatic updates**: Monitors `Ingress` changes and updates dependent `Dllama` resources
- **Zero-config**: No additional configuration required - works out of the box

This eliminates the need to duplicate NATS connection details across multiple `Dllama` resources while maintaining centralized configuration through `Ingress` resources.

**Example**: Create an `Ingress` with `spec.backend.nats.url: "nats://nats.example.com:4222"` and all `Dllama` resources will automatically inherit this URL, overriding their own `spec.nats.url` values.

## Controllers

All controllers are wired through Wrangler's generic factories and `apply` engine:

- `pkg/controllers/dllama.go` expands Dllama resources into Model/Root/Worker children, applies ownership, and updates the aggregate status once underlying components report ready. Also implements automatic NATS URL copying from `Ingress` resources with comprehensive logging for troubleshooting.
- `pkg/controllers/model.go` orchestrates metadata `ConfigMap` creation, a streamed download `Job`, and an optional post-processing conversion `Job` (e.g. GGUF export + tokenizer pack) that reads artifacts directly from the S3/MinIO cache via an S3 CSI PV/PVC mount.
- `pkg/controllers/root.go` renders the coordinator `Deployment` and associated `Service`, watching Kubernetes workloads to reflect readiness and expose a stable endpoint.
- `pkg/controllers/worker.go` renders worker `Deployments` and tracks pod readiness per slot.

Each reconciliation uses `WithSetID` to ensure obsolete workers are pruned when the replica power changes. Owner references ensure garbage collection when top level resources are deleted.

## Building & Running

```shell
# Build locally
go build ./cmd/operator

# Run the operator against the current kubeconfig
go run ./cmd/operator -mode=operator -kubeconfig ~/.kube/config
```

When running in-cluster the operator defaults to `InClusterConfig` and listens for termination signals via Wrangler's signal helper.

### Additional Backends

The same binary now exposes two auxiliary services that can be enabled with the `--mode` flag:

| Mode | Purpose |
| --- | --- |
| `operator` | Default reconciliation loop (existing behaviour) |
| `backend` | HTTP edge that authenticates tokens, derives `hash_koldun`, manages JetStream TTL records, and bridges chat requests to NATS |
| `llm` | Worker that listens on `in.<hash_koldun>` subjects, proxies requests to the local dllama-api sidecar and streams responses back on `out.<hash_koldun>` |

#### Token CRD

API clients authenticate with an API token stored in the new `Token` custom resource:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Token
metadata:
  name: my-token
  namespace: default
spec:
  hash: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
  metadata:
    owner: alice@example.com
```

The operator mirrors every `Token` into the JetStream registry bucket (`koldun_tokens` by default). The backend hashes the provided `KOLDUN_API_TOKEN` header (SHA-256 hex) and validates it against that registry; disabled tokens are rejected with HTTP 401.

#### Backend Edge (`--mode=backend`)

```
go run ./cmd/operator \
  -mode=backend \
  -backend-namespace default \
  -backend-nats-url nats://koldun:k0ldun@nats.default:4222 \
  -backend-in-prefix in. \
  -backend-out-prefix out. \
  -backend-ttl-prefix nats_ttl_ \
  -backend-conversation-bucket koldun_ttl \
  -backend-models-bucket koldun_models \
  -backend-tokens-bucket koldun_tokens \
  -backend-conversation-ttl 10m
```

Request flow:

1. `POST /v1/chat/completions`
   - Validates `KOLDUN_API_TOKEN` against the token entries published by the operator.
   - Extracts `chat_id`/`chat_start_time` (headers `X-Chat-ID`, `X-Chat-Start` or `request.metadata`).
   - Computes `hash_koldun = make_id(token, chat_id, chat_start_time)` per the supplied Python reference (now re-implemented in Go).
   - Ensures a JetStream KeyValue entry `nats_ttl_<hash>` exists with JSON payload describing the requested model, namespace, replica power, and a generated Dllama name (`dllama-<timestamp>-<hash>`). The entry TTL is refreshed on every request.
   - Looks up the requested model in the registry bucket (default `koldun_models`) to verify readiness (`outputPVCName` + size metadata) and capture its desired `replicaPower`.
   - Subscribes to `out.<hash>` and publishes the OpenAI payload (plus metadata) to `in.<hash>`. Streaming responses are proxied to the HTTP client as `text/event-stream`, forwarding raw chunks and emitting `[DONE]` when finished.

2. `GET /v1/models`
   - Returns the ready models currently published in the registry bucket so OpenAI-compatible clients can discover available deployments.

Key flags:

- `--backend-root-image` / `--backend-worker-image` — images baked into the generated `Dllama` topology (must match the sidecar expectations).
- `--backend-hash-secret` — optional secret for hashing (enables HMAC-SHA256).
- `--backend-conversation-ttl` — JetStream KV TTL (default 10m) that governs conversation lifetime.
- `--backend-response-timeout` — time to wait for NATS responses before returning HTTP 504.

Notes:

- The operator publishes ready models (`backend-models-bucket`) and token hashes (`backend-tokens-bucket`) into JetStream, so the backend no longer needs Kubernetes API access—only NATS connectivity.
- The Helm chart exposes `backend.rootImage` / `backend.workerImage`; set these to the images that should power generated Dllama roots and workers.
- All components accept credentialed NATS URLs (e.g. `nats://koldun:k0ldun@host:4222`). When embedding secrets is undesirable, project credentials via env vars and append the `--*-nats-url=$(NATS_URL)` flags through `extraArgs`.

#### Ingress Custom Resource

The operator can manage the backend deployment, Service, and Kubernetes Ingress through the `Ingress` custom resource (`apiVersion: koldun.gorizond.io/v1`). Creating an `Ingress` object removes the need to run `--mode=backend` manually—the controller reconciles all supporting objects for you.

Example:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Ingress
metadata:
  name: public-backend
  namespace: default
spec:
  backend:
    image: ghcr.io/gorizond/koldun:latest
    rootImage: ghcr.io/gorizond/koldun:latest
    workerImage: ghcr.io/gorizond/koldun:latest
    nats:
      url: nats://koldun:k0ldun@nats.default:4222
      kvBucket: koldun_ttl
      modelsBucket: koldun_models
      tokensBucket: koldun_tokens
      modelPrefix: model/
      tokenPrefix: token/
    conversationTTL: 10m
    responseTimeout: 2m
  service:
    port: 8082
  route:
    host: koldun.localtest.me
    path: /
    ingressClassName: traefik
```

The controller renders a Deployment with `--mode=backend`, a ClusterIP Service exposing port 8082, and a standard `networking.k8s.io/v1` Ingress that routes `koldun.example.com` to the Service. Additional configuration is available:

- `spec.backend.extraArgs` lets you append CLI flags for advanced scenarios.
- `spec.backend.hashSecret` sets an HMAC secret shared with the backend.
- `spec.backend.nats.url` supports embedded credentials (`nats://koldun:k0ldun@host:4222`). Use `spec.backend.extraArgs` plus projected env vars if you prefer to avoid literals.
- `spec.backend.nats.modelsBucket` / `spec.backend.nats.tokensBucket` let you point at alternative registry buckets if you want per-tenant isolation.
- `spec.service.type` may be set to `LoadBalancer` or `NodePort` when required.
- TLS hosts and annotations map directly to the `Ingress` manifest.

The Helm chart includes CRDs for `Ingress` and `Token`. You can declare multiple ingress instances via `values.yaml` under the `ingresses` array—`helm install` renders one manifest per entry using `templates/ingress-cr.yaml`.

#### LLM Worker (`--mode=llm`)

```
go run ./cmd/operator \
  -mode=llm \
  -llm-hash "$HASH_KOLDUN" \
  -llm-nats-url nats://koldun:k0ldun@nats.default:4222 \
  -llm-sidecar-url http://127.0.0.1:8080 \
  -llm-in-prefix in. \
  -llm-out-prefix out.
```

- Reads `hash_koldun` from `--llm-hash` (defaults to the `HASH_KOLDUN` env variable).
- Subscribes to `in.<hash>` subjects, forwards requests to the colocated dllama-api sidecar (`/v1/chat/completions`), and publishes streaming chunks back to `out.<hash>` (terminating with `[DONE]`).
- Exposes `/healthz` and `/readyz` for liveness checks when `--llm-health-only` is false.

### Conversation Lifecycle

Every active chat produces three coordinated channels/key entries:

- `in.<hash_koldun>` — backend publishes new user prompts; the LLM worker subscribes.
- `out.<hash_koldun>` — LLM worker streams responses; the backend subscribes and relays to the client.
- `nats_ttl_<hash_koldun>` — JetStream KV record containing `{dllama, model, namespace, replicaPower}` with bucket TTL (default 10 minutes). The operator reconciler ensures a matching `Dllama` resource exists while the key is present and prunes stale `Dllama` objects once the key expires.

#### Operator Conversation Reconciler (`--mode=operator`)

```
go run ./cmd/operator \
  -mode=operator \
  -kubeconfig ~/.kube/config \
  -operator-nats-url nats://koldun:k0ldun@nats.default:4222 \
  -operator-kv-bucket koldun_ttl \
  -operator-ttl-prefix nats_ttl_ \
  -operator-poll-interval 10s
```

- Watches all keys in the configured JetStream bucket and creates/updates `Dllama` resources based on the stored record (one per conversation, regardless of which API token initiated it).
- Labels managed Dllamas with `koldun.gorizond.io/hash=<hash_koldun>` and removes them automatically once the TTL entry disappears.
- Any number of API tokens can be active simultaneously; the reconciler operates cluster-wide across every conversation hash.

## Next Steps & TODOs

1. **JetStream watch integration** — switch the operator reconciler from polling to native KeyValue watches to reduce latency and traffic.
2. **Model caching pipeline** — integrate an actual artifact downloader (e.g. `Job` + PVC or S3-compatible cache) and populate `ModelStatus` with size/checksum data.
3. **Serverless triggers** — implement demand-based activation similar to Knative (the current implementation keeps Deployments running). Explore net-kourier for minimal HTTP routing once activation is in place.
4. **Credentials management** — wire `objectStorage.secretRef` into projected volumes/env to support S3-compatible backends securely.
5. **Scaling policies** — extend `DllamaSpec` with autoscaling hints, resource requests, and GPU scheduling specifics for distributed-llama workloads.
6. **Comprehensive status** — propagate pod phase details, endpoints, and model download progress via Conditions for better observability.
7. **CRD generation** — generate CRDs/manifests (`wrangler generate` or `controller-gen`) so the CR suite can be installed via YAML/Helm.

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
  conversion:
    converterVersion: v0.16.2
    image: python:3.11
    memory: 8Gi
    outputPath: s3://my-bucket-convert
    toolsImage: alpine:3.18
    weightsFloatType: f32 #q80
  download:
    chunkMaxMiB: 256
    concurrency: 6
    huggingFaceTokenSecretRef:
      name: my-hf
    image: python:3.11
    memory: 2Gi
  localPath: s3://my-bucket-model/Qwen/Qwen3-1.7B
  objectStorage:
    bucketForConvert: my-bucket-convert
    bucketForSource: my-bucket-model
    endpoint: http://192.168.205.2:32090
    secretRef:
      name: minio-creds
  pipProxy: http://192.168.205.2:4001
  sourceUrl: https://huggingface.co/Qwen/Qwen3-1.7B
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
