# Koldun Operator 🧙‍♂️

## Overview
Koldun orchestrates distributed-llama inference topologies on Kubernetes. The single `cmd/operator` binary exposes controller, ingress backend, dispatcher, and LLM worker modes that coordinate model download, topology creation, NATS-based messaging, and conversation lifecycle management. Wrangler provides the reconciliation engine, and JetStream acts as the persistent queue for chat sessions, assignments, and registry metadata.

## Highlights
- Manages **Session → Dllama → Worker** hierarchies, wiring Deployments, StatefulSets, and Jobs for distributed-llama clusters.
- Streams Hugging Face models into S3/MinIO, converts artifacts (GGUF), and publishes sizing metadata for scheduling.
- Ingests chat traffic through an OpenAI-compatible backend that hashes API tokens, supervises dispatcher pools, and mirrors readiness back to clients.
- Automatically synchronises NATS connection URLs from `Ingress` resources to dependent `Dllama` objects to avoid configuration drift.

## Project Layout
- `cmd/operator/` — entrypoint wiring CLI flags to the desired mode.
- `pkg/apis/koldun.gorizond.io/v1/` — CRD type definitions and DeepCopy implementations.
- `pkg/controllers/` — Wrangler reconcilers plus helpers in `common.go`; tests (e.g. `memory_test.go`) sit beside implementations.
- `pkg/servers/{ingress,dispatcher,llm,operator}/` — HTTP servers and workers for runtime modes.
- `pkg/conversation`, `pkg/registry`, `pkg/tokens` — shared JetStream contracts, KV helpers, and token handling.
- `charts/` Helm chart, `k8s/` raw manifests, root `Dockerfile` and `skaffold.yaml` for image builds.

## Custom Resources
| Kind | Purpose | Notes |
| --- | --- | --- |
| `Session` | Supervises dispatcher Deployments and pools of generated `Dllama` resources per conversation hash. | Scales via `spec.sessionScaling` thresholds and stores queue prefixes/assignment buckets. |
| `Dllama` | Expands a distributed-llama topology (root + workers) for a specific model. | Ensures referenced `Model` exposes `status.outputPVCName`; inherits NATS URL from `Ingress` automatically. |
| `Model` | Downloads, converts, and sizes model artifacts. | Creates downloader/convert Jobs, S3 CSI PV/PVC, and publishes size metadata. |
| `Root` | Renders the distributed-llama root coordinator Deployment/Service. | Watches pods to update readiness. |
| `Worker` | Manages single-slot worker StatefulSets. | Tracks per-slot readiness for dispatcher accounting. |
| `Ingress` | Declares the public ingress/backend bundle. | Generates backend Deployment, Service, Kubernetes Ingress, and publishes NATS/registry configuration. |

## Binary Modes (`--mode`)
| Mode | What It Runs |
| --- | --- |
| `operator` (default) | Registers controllers, reconciles CRDs, publishes model/token registries, and synchronises JetStream conversation TTLs into `Session`/`Dllama` resources. |
| `ingress` (alias `backend`) | OpenAI-compatible HTTP edge; authenticates API tokens, maintains conversation KV records, and bridges requests to NATS. |
| `dispatcher` | Consumes backlog subjects, writes assignment KV entries, and fans work out to ready Dllama workers while tracking heartbeats. |
| `llm` | Sidecar-facing worker that streams completions between the dllama-api process and NATS `out.<hash>` subjects. |

## Conversation Flow
1. Backend validates an API token (mirrored from Secrets), computes `hash_koldun`, and stores a JSON manifest in the JetStream KV bucket (`backend-conversation-bucket`).
2. Operator watches the bucket and ensures matching `Session`/`Dllama` resources exist, labelling them with `koldun.gorizond.io/hash`.
3. Dispatcher reads backlog messages (`sessions.<hash>.requests`), assigns them to workers, and records progress in the assignments bucket.
4. LLM workers call the dllama-api sidecar, stream completion chunks to `out.<hash>`, and ping state subjects so the dispatcher can recycle slots.

## Getting Started
### Prerequisites
- Go 1.24+, Docker/Skaffold (for container builds), and access to a Kubernetes cluster (Kubernetes 1.30+) with JetStream-enabled NATS.
- Helm 3 if you plan to install via the included chart.

### Build & Run Locally
```bash
go fmt ./... && gofmt -w .
go build ./cmd/operator
# Run controllers against your kubeconfig
go run ./cmd/operator --mode=operator --kubeconfig ~/.kube/config
```

### Backend & Worker Smoke Tests
```bash
# Start the OpenAI-compatible ingress backend
KOLDUN_API_TOKEN=... \
go run ./cmd/operator --mode=ingress \
  --backend-namespace default \
  --backend-nats-url nats://user:pass@nats.default:4222

# Launch a standalone worker connected to an existing dispatcher
HASH_KOLDUN=... \
go run ./cmd/operator --mode=llm \
  --llm-request-subject "sessions.${HASH_KOLDUN}.dllama.0.in" \
  --llm-state-subject "sessions.${HASH_KOLDUN}.dllama.0.state"
```

### Deploy to Kubernetes
- **Helm**: Edit `charts/koldun/values.yaml` (images, NATS, session scaling) then `helm install koldun charts/koldun`.
- **Raw manifests**: Apply the CRDs, controllers, and sample resources from the `k8s/` directory.
- Use `skaffold build` to publish the container image `ghcr.io/gorizond/koldun` before updating chart values.

## Example Custom Resources
### Model
Example `Model` that streams a Hugging Face repository into S3/MinIO, runs GGUF conversion, and exposes sizing metadata:

```yaml
apiVersion: koldun.gorizond.io/v1
kind: Model
metadata:
  name: mistral-convert
  namespace: default
spec:
  sourceUrl: https://huggingface.co/mistralai/Mistral-7B-v0.3
  localPath: s3://models/mistral-7b-v0-3
  objectStorage:
    endpoint: http://minio.default:32090
    bucketForSource: models
    bucketForConvert: models-converted
    secretRef:
      name: minio-creds
  download:
    image: python:3.10
    memory: 2Gi
    chunkMaxMiB: 256
    concurrency: 6
    huggingFaceTokenSecretRef:
      name: hf-token
  conversion:
    converterVersion: v0.16.2
    image: python:3.10
    memory: 8Gi
    convertWeights: q40
    outputPath: s3://models-converted/mistral-7b-v0-3
    toolsImage: alpine:3.18
  pipProxy: http://dragonfly.default:4001
```

- Downloader and converter Jobs mount the S3 buckets via CSI and publish size data to `status.conversionSizeBytes`.
- Set `download.huggingFaceTokenSecretRef` only for private repositories; the secret must provide `token`.
- Add the annotation `koldun.gorizond.io/force-size-rerun` to trigger a sizing rerun when artifacts change.

### Ingress
`Ingress` resources let the operator render the ingress/backend bundle and publish NATS details for dependent `Dllama` objects:

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
    dispatcherImage: ghcr.io/gorizond/koldun:latest
    replicaPower: 2
    nats:
      url: nats://koldun:k0ldun@nats.default:4222
      kvBucket: koldun_ttl
      modelsBucket: koldun_models
      tokensBucket: koldun_tokens
      modelPrefix: model/
      tokenPrefix: token/
    sessionScaling:
      minDllamas: 1
      maxDllamas: 4
      scaleUpBacklog: 2
      scaleDownIdleSeconds: 120
    conversationTTL: 10m
    responseTimeout: 2m
  service:
    port: 8082
  route:
    host: koldun.localtest.me
    path: /
    ingressClassName: traefik
```

- The controller produces the backend Deployment (`--mode=ingress`), Service, and Kubernetes Ingress automatically.
- Set `spec.backend.extraArgs` for advanced flags, or `spec.backend.hashSecret` to enable HMAC hashing.
- Choosing `spec.service.type: LoadBalancer` or providing TLS annotations maps directly to the rendered Kubernetes Ingress.

## Development Workflow
- Follow `AGENTS.md` for contributor expectations, code layout, testing, and security conventions.
- Shared helpers belong in `pkg/controllers/common.go`; resource-specific logic lives in dedicated files (`root.go`, `worker.go`, etc.).
- Prefer Go table-driven tests and mocks from `go.uber.org/mock` for JetStream/Kubernetes clients.
- Avoid committing secrets; store NATS credentials and hash secrets in Kubernetes Secrets labelled `koldun.gorizond.io/token`.

### Envtest Integration Suite
Controllers rely on [`envtest`](https://book.kubebuilder.io/reference/envtest.html) binaries (`kube-apiserver`, `etcd`) when running the integration test in `pkg/controllers/dllama_reconcile_envtest_test.go`. Install the assets once and export `KUBEBUILDER_ASSETS` before running the suite:

```bash
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
# Downloads the stack compatible with controller-runtime v0.20.4 and prints the export lines
eval "$(setup-envtest use --controller-runtime-version 0.20.4 --install-dir ./bin/envtest)"

# Verify the integration test; it will skip with a helpful message if assets are missing
go test ./pkg/controllers -run TestDllamaReconciliationCreatesRootAndWorker -count=1
```

The helper in `pkg/controllers/envtest_suite_test.go` auto-discovers `KUBEBUILDER_ASSETS`; when the binaries are absent the test suite now exits early with an explicit instruction instead of noisy control-plane failures.

### Key Commands
| Purpose | Command |
| --- | --- |
| Format | `go fmt ./... && gofmt -w .` |
| Unit tests | `go test ./...` (append `-race` for data race checks) |
| Build binary | `go build ./cmd/operator` |
| Run operator | `go run ./cmd/operator --mode=operator` |
| Build/push image | `skaffold build` |

## Security & Configuration Notes
- Labels on Secrets (`koldun.gorizond.io/token=true`) trigger token mirroring into the JetStream registry bucket; the backend rejects disabled tokens (`stringData.disabled`).
- Set `backend-hash-secret` to enable HMAC-SHA256 conversation hashing; leave empty for plain SHA-256.
- The operator ensures S3 PV/PVC resources exist when `Model.spec.objectStorage` is configured; disable automatic bucket creation with `--operator-disable-bucket-ensure` when managing buckets manually.
- Update the Helm chart, Kubernetes manifests, and Dockerfile together when changing binary flags or images to avoid drift.

## Additional Resources
- Sample CRs: `k8s/examples/*.yaml` (models, dllama topologies, ingress definitions).
- Token tooling lives in `pkg/tokens`; registry helpers in `pkg/registry` show how JetStream buckets are structured.
- File an issue or PR with validation steps (`go test ./...`, Helm installation logs, kube events) to document behavioural changes.

## In Memoriam

I dedicate this repository to my grandfather, Negashev Vyacheslav Ivanovich
