# CLAUDE.md - AI Assistant Guide for Koldun

**Last Updated**: 2025-11-19
**Target Audience**: AI assistants (Claude, ChatGPT, etc.) working on this codebase

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture & Key Concepts](#architecture--key-concepts)
3. [Codebase Structure](#codebase-structure)
4. [Development Environment](#development-environment)
5. [Development Workflow](#development-workflow)
6. [Testing Philosophy](#testing-philosophy)
7. [Code Conventions](#code-conventions)
8. [Common Development Tasks](#common-development-tasks)
9. [CI/CD Pipeline](#cicd-pipeline)
10. [Troubleshooting Guide](#troubleshooting-guide)
11. [Important Files & References](#important-files--references)

---

## Project Overview

**Koldun** is a Kubernetes operator that orchestrates distributed LLM inference using distributed-llama on Kubernetes clusters.

### What It Does
- Manages **distributed LLM inference topologies** across Kubernetes nodes
- Provides an **OpenAI-compatible API** for LLM interactions
- Handles **model downloading, conversion, and storage** via S3/MinIO
- Uses **NATS JetStream** for message queuing and state management
- Coordinates **dispatcher pools** and worker scheduling

### Key Technologies
- **Language**: Go 1.24+ (toolchain 1.24.2)
- **Framework**: Kubernetes operator using Wrangler controllers
- **Messaging**: NATS JetStream for queues and KV storage
- **Storage**: MinIO/S3 for model artifacts
- **Container**: Docker, deployed via Helm charts
- **Testing**: Envtest (in-memory Kubernetes), table-driven tests, gomock

---

## Architecture & Key Concepts

### Custom Resource Definitions (CRDs)

| CRD | Purpose | Controller Location |
|-----|---------|-------------------|
| **Model** | Downloads, converts, and sizes model artifacts from Hugging Face | `pkg/controllers/model*.go` |
| **Ingress** | Declares the public ingress/backend bundle with NATS configuration | `pkg/controllers/ingress.go` |
| **Session** | Supervises dispatcher Deployments and pools of Dllama resources | `pkg/controllers/session*.go` |
| **Dllama** | Expands distributed-llama topology (root + workers) for a model | `pkg/controllers/dllama*.go` |
| **Root** | Renders the distributed-llama root coordinator Deployment/Service | `pkg/controllers/root.go` |
| **Worker** | Manages single-slot worker StatefulSets | `pkg/controllers/worker.go` |

### Binary Modes (`cmd/operator --mode`)

The single `cmd/operator` binary supports multiple runtime modes:

| Mode | Purpose | Implementation |
|------|---------|---------------|
| `operator` (default) | Registers controllers, reconciles CRDs, manages JetStream | `pkg/servers/operator/` |
| `ingress` / `backend` | OpenAI-compatible HTTP API, token auth, NATS bridging | `pkg/servers/ingress/` |
| `dispatcher` | Consumes backlog, assigns work to workers, tracks heartbeats | `pkg/servers/dispatcher/` |
| `llm` | Sidecar worker streaming completions between dllama-api and NATS | `pkg/servers/llm/` |

### Data Flow (Conversation Lifecycle)

```
HTTP Request → Ingress Backend → NATS JetStream → Dispatcher → Dllama Workers → LLM Response
     ↓              ↓                   ↓              ↓              ↓
  API Token    Conversation KV    Backlog Queue   Assignment KV   State Subjects
```

1. **Backend** validates API token, computes hash, stores conversation in JetStream KV
2. **Operator** watches KV bucket, ensures Session/Dllama resources exist
3. **Dispatcher** reads backlog, assigns to workers, tracks progress
4. **LLM workers** call dllama-api, stream chunks to NATS output subjects

### Key Directories

```
koldun/
├── cmd/operator/           # Main entrypoint (multi-mode binary)
├── pkg/
│   ├── apis/koldun.gorizond.io/v1/  # CRD type definitions
│   ├── controllers/        # Wrangler reconcilers + tests
│   ├── servers/            # Runtime servers (ingress, dispatcher, llm, operator)
│   ├── conversation/       # JetStream conversation contracts
│   ├── registry/           # JetStream KV helpers for models/tokens
│   ├── tokens/             # API token handling
│   ├── natsutil/           # NATS wrapper interfaces (for testing)
│   ├── metrics/            # Prometheus metrics handlers
│   └── api/openai/         # OpenAI compatibility layer
├── charts/koldun/          # Helm chart
├── k8s/                    # Raw Kubernetes manifests
├── context/                # Development session history (iterative workflow)
├── docs/                   # Additional documentation
├── hack/                   # Helper scripts (envtest, coverage, localstack)
├── analytics/              # Coverage baselines, metrics tracking
├── Makefile                # Build automation
├── Dockerfile              # Container image definition
└── docker-compose.test.yml # Local integration testing stack
```

---

## Codebase Structure

### Package Organization

- **`cmd/operator/`** — CLI flags, mode selection, main entrypoint
- **`pkg/apis/koldun.gorizond.io/v1/`** — CRD structs, DeepCopy methods
- **`pkg/controllers/`** — Reconciliation logic, watchers, helpers
  - `common.go` — Shared utilities across controllers
  - `*_test.go` — Table-driven tests, envtest suites
  - `envtest_suite_test.go` — Shared test setup for controllers
- **`pkg/servers/`** — HTTP servers and workers
  - Each mode has its own subdirectory
  - Tests co-located with implementation
- **`pkg/conversation/`** — JetStream KV contracts for conversations
- **`pkg/registry/`** — Model and token registry helpers
- **`pkg/natsutil/`** — NATS wrapper interfaces (enables mocking)
- **`pkg/tokens/`** — API token hashing and validation

### File Naming Conventions

- Controllers: `{resource}.go` (e.g., `dllama.go`, `worker.go`)
- Tests: `{resource}_test.go` (e.g., `dllama_test.go`)
- Envtest integration: `*_envtest_test.go`
- Helpers: `common.go`, `helpers.go`
- Generated code: `zz_generated_deepcopy.go` (auto-generated, don't edit)

---

## Development Environment

### Prerequisites

**Required:**
- Go 1.24+ (toolchain auto-managed via `go.mod`)
- Docker (for image builds)
- Kubernetes cluster access (for testing)
  - Rancher Desktop (recommended for local dev)
  - k3d/minikube/kind (alternatives)

**Optional but Recommended:**
- `direnv` (for auto-loading environment variables from `.envrc`)
- `setup-envtest` (for controller testing)
- Helm 3 (for chart installation)
- NATS CLI (for debugging JetStream)

### Initial Setup

```bash
# Clone repository
git clone https://github.com/gorizond/koldun.git
cd koldun

# Install dependencies
go mod download

# Install setup-envtest (for controller tests)
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

# Prepare envtest assets
make envtest-preflight
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"

# (Optional) Enable direnv
direnv allow

# Verify setup
make test
```

### Environment Variables

Key variables from `.envrc`:

```bash
# Envtest assets (controller testing)
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"

# NATS configuration (for integration tests)
export KOLDUN_NATS_URL="nats://koldun:koldun@127.0.0.1:4222"
export KOLDUN_DISPATCHER_NATS_URL="${KOLDUN_NATS_URL}"

# MinIO configuration (for model storage tests)
export KOLDUN_MINIO_ENDPOINT="http://127.0.0.1:9000"
export KOLDUN_MINIO_ACCESS_KEY="minio"
export KOLDUN_MINIO_SECRET_KEY="minio123"
```

---

## Development Workflow

### Building

```bash
# Format code (always run before committing)
go fmt ./... && gofmt -w .

# Build operator binary
go build ./cmd/operator

# Build Docker image
docker build -t koldun:dev .

# Build multi-arch image with Skaffold
skaffold build
```

### Running Locally

```bash
# Run as operator (controller mode)
go run ./cmd/operator --mode=operator --kubeconfig ~/.kube/config

# Run as ingress backend
KOLDUN_API_TOKEN=test123 \
go run ./cmd/operator --mode=ingress \
  --backend-namespace default \
  --backend-nats-url nats://user:pass@nats.default:4222

# Run as dispatcher
HASH_KOLDUN=abc123 \
go run ./cmd/operator --mode=dispatcher \
  --dispatcher-hash "${HASH_KOLDUN}" \
  --dispatcher-nats-url nats://koldun:k0ldun@nats.default:4222 \
  --dispatcher-backlog-subject "sessions.${HASH_KOLDUN}.requests"

# Run as LLM worker
HASH_KOLDUN=abc123 \
go run ./cmd/operator --mode=llm \
  --llm-request-subject "sessions.${HASH_KOLDUN}.dllama.0.in" \
  --llm-state-subject "sessions.${HASH_KOLDUN}.dllama.0.state"
```

### Local Development with Rancher Desktop

**Quick Start:**

```bash
# 1. Switch context
kubectl config use-context rancher-desktop

# 2. Create namespace
kubectl create namespace koldun

# 3. Build image
docker build -t koldun:dev .

# 4. Update Helm dependencies
make helm-deps

# 5. Install with local values
helm install koldun charts/koldun/ -n koldun -f values-dev.yaml --wait

# 6. Verify
kubectl get pods -n koldun

# 7. Port-forward for testing
kubectl port-forward deployment/koldun 8080:8080 -n koldun
curl http://localhost:8080/healthz  # Should return "ok"
```

**Iterative Development:**

```bash
# Rebuild after code changes
docker build -t koldun:dev .

# Upgrade Helm release
helm upgrade koldun charts/koldun/ -n koldun -f values-dev.yaml --wait

# Watch logs (may timeout due to Rancher Desktop TLS issues)
kubectl logs -f deployment/koldun -n koldun
```

**Known Limitations:**
- kubectl logs may timeout (use `kubectl port-forward` + health endpoints instead)
- Multi-worker stability requires Rosetta + VZ on ARM64 (see README)
- CPU inference is VERY slow (2-5 minutes per token)

---

## Testing Philosophy

Koldun follows a **rigorous test-driven development** approach with high coverage standards.

### Test Categories

1. **Unit Tests** — Pure logic, mocked dependencies
2. **Envtest Tests** — Controller reconciliation with in-memory Kubernetes API
3. **Integration Tests** — Real NATS/MinIO via docker-compose stack
4. **End-to-End Tests** — Full Helm deployment in k3d/Rancher Desktop

### Coverage Targets

| Package | Target | Current | Gate |
|---------|--------|---------|------|
| `pkg/controllers` | 99%+ | 99.8% | CI blocks if drops below 99% |
| `pkg/servers/ingress` | 80%+ | ~70% | Tracked via compose baseline |
| `pkg/servers/dispatcher` | 80%+ | ~70% | Tracked via compose baseline |
| Overall | 85%+ | ~77.9% | Advisory (not enforced) |

### Running Tests

```bash
# All tests
make test

# Controllers only (fast, no coverage)
make controllers-smoke

# Controllers with coverage verification (CI gate)
make controllers-coverage-check

# Integration tests (docker-compose stack)
make compose-test

# Specific package
go test ./pkg/controllers -v

# With race detector
go test ./... -race

# Generate coverage report
make coverage
# Opens coverage.html in browser
```

### Envtest Setup

Controllers use **envtest** (in-memory Kubernetes API server) for integration testing.

**First-time setup:**

```bash
# Install setup-envtest
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

# Download assets
make envtest-preflight

# Export path
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"

# Verify
make controllers-smoke
```

**CI/CD caching:**
- Cache `./bin/envtest` directory
- Set `KOLD_SKIP_ENVTEST_DOWNLOAD=1` to fail fast if assets missing

**Performance baselines:**
- Cached run: ~40-50s (macOS/Linux)
- Cold run: ~220-260s (includes toolchain download)

### Compose Integration Tests

Tests ingress and dispatcher servers against real NATS JetStream and MinIO.

```bash
# Run full integration suite
make compose-test

# Outputs:
# - compose.coverprofile (merged coverage)
# - artifacts/compose-logs.txt (docker logs)

# Keep stack running for debugging
COMPOSE_TEST_KEEP_STACK=1 make compose-test

# Manual cleanup
make compose-test-down

# Update coverage baseline (after improving coverage)
make compose-update-baseline
```

**CI Integration:**
- `.github/workflows/compose-ingress.yaml` runs on every push
- Fails if coverage drops below baseline in `analytics/compose_coverage_baseline.json`
- Warns if coverage increases without updating baseline

### Test Patterns

**Table-Driven Tests:**

```go
func TestFunctionName(t *testing.T) {
    tests := []struct {
        name    string
        input   InputType
        want    OutputType
        wantErr bool
    }{
        {
            name:    "successful case",
            input:   InputType{...},
            want:    OutputType{...},
            wantErr: false,
        },
        {
            name:    "error case",
            input:   InputType{...},
            want:    OutputType{},
            wantErr: true,
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := FunctionName(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if !reflect.DeepEqual(got, tt.want) {
                t.Errorf("got %v, want %v", got, tt.want)
            }
        })
    }
}
```

**Mocking with gomock:**

```go
// Generate mocks
//go:generate go run go.uber.org/mock/mockgen -destination=mocks/mock_nats.go -package=mocks github.com/gorizond/koldun/pkg/natsutil JetStreamContext

// Use in tests
ctrl := gomock.NewController(t)
defer ctrl.Finish()

mockJS := mocks.NewMockJetStreamContext(ctrl)
mockJS.EXPECT().KeyValue("bucket").Return(mockKV, nil)
```

**Envtest Reconciliation Tests:**

```go
func TestReconciler(t *testing.T) {
    // Setup is handled by envtest_suite_test.go

    // Create test resource
    resource := &koldunv1.MyResource{
        ObjectMeta: metav1.ObjectMeta{
            Name:      "test",
            Namespace: "default",
        },
        Spec: koldunv1.MyResourceSpec{...},
    }
    err := k8sClient.Create(ctx, resource)
    require.NoError(t, err)

    // Wait for reconciliation
    Eventually(func() bool {
        var updated koldunv1.MyResource
        err := k8sClient.Get(ctx, client.ObjectKeyFromObject(resource), &updated)
        if err != nil {
            return false
        }
        return updated.Status.Ready == true
    }, timeout, interval).Should(BeTrue())
}
```

### Acceptable Coverage Gaps

Some code paths are intentionally **not tested**:

**pkg/controllers (99.8% coverage):**
- `isConnectionClosed` without `isBucketMissing` — requires real infrastructure failure
- `reconnectFn` in mocked tests — actual reconnect never executes in unit tests
- `openConnection` JetStream errors — requires real infrastructure failure

**pkg/natsutil (74.1% coverage):**
- Wrapper functions (0% each) — thin delegation to underlying NATS types, no business logic

These represent infrastructure error paths or trivial wrappers that would require integration/e2e tests to cover meaningfully.

---

## Code Conventions

### Go Style

- **Formatting**: Run `go fmt ./... && gofmt -w .` before every commit
- **Indentation**: Tabs (enforced by gofmt)
- **Naming**:
  - **Variables**: camelCase for locals, PascalCase for exported
  - **Functions**: PascalCase for exported, camelCase for private
  - **Files**: lowercase with underscores (`dllama_reconcile.go`)
  - **CLI flags**: kebab-case with mode prefix (`--dispatcher-nats-url`)

### Code Organization

**Controllers:**
- Shared helpers → `pkg/controllers/common.go`
- Resource-specific logic → `pkg/controllers/{resource}.go`
- Tests beside implementation → `pkg/controllers/{resource}_test.go`

**Servers:**
- Mode-specific → `pkg/servers/{mode}/server.go`
- Helpers → `pkg/servers/{mode}/helpers.go`
- Tests co-located → `pkg/servers/{mode}/server_test.go`

### Error Handling

```go
// Good: wrap errors with context
if err := doSomething(); err != nil {
    return fmt.Errorf("failed to do something: %w", err)
}

// Good: structured logging
log.WithFields(logrus.Fields{
    "resource": req.NamespacedName,
    "error":    err,
}).Error("reconciliation failed")

// Bad: swallow errors silently
_ = doSomething()

// Bad: panic in production code
panic("this should never happen")
```

### Logging

```go
import "github.com/sirupsen/logrus"

// Use structured logging
log.WithFields(logrus.Fields{
    "namespace": obj.Namespace,
    "name":      obj.Name,
    "hash":      hash,
}).Info("reconciling resource")

// Levels: Debug, Info, Warn, Error
log.Debug("verbose debugging info")
log.Info("normal operation")
log.Warn("something unexpected but recoverable")
log.Error("operation failed")
```

### Commit Messages

**Format:**
```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `test`: Add/modify tests
- `refactor`: Code restructuring (no behavior change)
- `docs`: Documentation changes
- `chore`: Build/tooling updates

**Examples:**
```
feat(controllers): add dispatcher autoscaling

Implements automatic scaling of dispatcher replicas based on NATS backlog depth.

Closes #123

---

fix(llm): increase health check timeout for CPU inference

CPU-based inference takes 2-5 minutes per token, causing health check
failures. Increase interval to 60s and failure threshold to 10.

Fixes #456

---

test(controllers): achieve 99.8% coverage in pkg/controllers

Adds edge case tests for connection recovery and bucket loss scenarios.
```

### Documentation

**Code Comments:**
- Document **why**, not what (code should be self-explanatory)
- Use GoDoc format for exported functions
- Include examples for complex logic

```go
// EnsureDllamaTopology creates or updates the distributed-llama topology
// for the given model. It spawns a Root coordinator Deployment and Worker
// StatefulSets based on the replicaPower calculation.
//
// The function is idempotent and safe to call multiple times. It returns
// an error only if the topology cannot be created or updated.
func EnsureDllamaTopology(ctx context.Context, dllama *koldunv1.Dllama) error {
    // Implementation
}
```

### Security Practices

**DO:**
- Store credentials in Kubernetes Secrets
- Label token secrets: `koldun.gorizond.io/token=true`
- Use HMAC-SHA256 for conversation hashing (via `--backend-hash-secret`)
- Validate inputs from external sources (API requests, NATS messages)

**DON'T:**
- Commit secrets, tokens, or credentials to Git
- Log sensitive data (tokens, hashes, user content)
- Trust user input without validation
- Use plain SHA-256 for security-critical hashing

---

## Common Development Tasks

### Adding a New Controller

1. **Define CRD types** in `pkg/apis/koldun.gorizond.io/v1/types.go`
2. **Implement reconciler** in `pkg/controllers/{resource}.go`
3. **Add watcher** in `pkg/servers/operator/server.go`
4. **Write tests** in `pkg/controllers/{resource}_test.go`
5. **Update CRD manifests** in `k8s/crds/` and `charts/koldun/templates/`
6. **Document** in README.md and AGENTS.md

**Example:**

```go
// pkg/controllers/myresource.go
package controllers

import (
    koldunv1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
    "github.com/rancher/wrangler/v3/pkg/generic"
)

func RegisterMyResourceController(
    ctx context.Context,
    clients *Clients,
) error {
    controller := &myResourceController{
        client: clients.Koldun.Koldun().V1().MyResource(),
    }

    clients.Koldun.Koldun().V1().MyResource().OnChange(
        ctx, "my-resource-controller", controller.OnChange,
    )

    return nil
}

type myResourceController struct {
    client koldunv1.MyResourceController
}

func (c *myResourceController) OnChange(
    key string, obj *koldunv1.MyResource,
) (*koldunv1.MyResource, error) {
    if obj == nil || obj.DeletionTimestamp != nil {
        return obj, nil
    }

    // Reconciliation logic here

    return obj, nil
}
```

### Adding a New CLI Flag

1. **Define flag** in `cmd/operator/main.go`:

```go
var (
    myNewFlag = flag.String("my-new-flag", "default", "Description of flag")
)
```

2. **Pass to mode initialization**:

```go
case "mymode":
    return mymode.Run(ctx, &mymode.Config{
        MyNewFlag: *myNewFlag,
    })
```

3. **Update manifests**:
   - `k8s/{mode}-deploy.yaml`
   - `charts/koldun/templates/{mode}-deployment.yaml`
   - `charts/koldun/values.yaml`

4. **Document** in README.md

### Debugging Controllers

**Local debugging:**

```bash
# Run operator with verbose logging
go run ./cmd/operator --mode=operator --kubeconfig ~/.kube/config -v=4

# Watch resources
kubectl get sessions,dllamas,models -A -w

# Check controller logs
kubectl logs deployment/koldun -n koldun -f

# Inspect resource status
kubectl describe dllama my-dllama -n default
```

**Common issues:**

- **Reconciliation loops**: Check for status updates that trigger re-reconciliation
- **Stuck resources**: Look for missing dependencies (Model, NATS connection)
- **RBAC errors**: Ensure ServiceAccount has proper permissions

### Working with NATS JetStream

**Debugging commands:**

```bash
# Connect to NATS pod
kubectl exec -it koldun-nats-0 -n koldun -- /bin/sh

# List streams
nats stream list

# Inspect stream
nats stream info sessions

# List KV buckets
nats kv list

# Get KV entry
nats kv get koldun_ttl conversation_hash_xyz

# Watch stream messages
nats stream view sessions
```

**Testing locally:**

```bash
# Start local NATS with JetStream
docker run -d -p 4222:4222 nats:latest -js

# Connect with NATS CLI
nats context save local --server nats://localhost:4222
nats context select local
```

### Updating Dependencies

```bash
# Update specific dependency
go get github.com/nats-io/nats.go@latest

# Update all dependencies (be careful!)
go get -u ./...

# Tidy and verify
go mod tidy
go mod verify

# Test after updates
make test
make controllers-smoke
```

### Helm Chart Development

```bash
# Update dependencies (NATS, MinIO, CSI)
make helm-deps

# Lint chart
make helm-lint

# Render templates locally
make helm-template

# Test installation in k3d
make helm-test-integration

# Install/upgrade locally
helm upgrade --install koldun charts/koldun/ \
  -n koldun --create-namespace \
  -f values-dev.yaml \
  --wait
```

---

## CI/CD Pipeline

### GitHub Actions Workflows

| Workflow | Trigger | Purpose | Location |
|----------|---------|---------|----------|
| **CI Build** | Every push | Run envtest, build/push Docker image | `.github/workflows/ci-build.yaml` |
| **Compose Ingress** | Every push | Run integration tests with NATS/MinIO | `.github/workflows/compose-ingress.yaml` |
| **Helm Integration** | Every push | Test Helm chart in k3d | `.github/workflows/helm-integration.yaml` |
| **Release** | Tag push | Build release artifacts | `.github/workflows/release.yml` |

### CI Build Workflow

**Steps:**
1. **Controllers Envtest** — Run `./hack/ci-envtest.sh` (controllers-smoke)
   - Uses cached `bin/envtest` assets
   - Blocks build if tests fail
   - Enforces 99%+ coverage
2. **Build and Push Image** — Multi-arch Docker build (amd64, arm64)
   - Pushes to `ghcr.io/gorizond/koldun`
   - Tags: `v0.0.{run_number}`, `sha-{commit}`, branch name

**Caching:**
- Go modules: `actions/setup-go` with `cache: true`
- Envtest assets: `actions/cache` on `bin/envtest` directory
- Docker layers: BuildKit cache with `cache-from` / `cache-to`

### Compose Integration Workflow

**Steps:**
1. Start docker-compose stack (NATS + MinIO + k3s)
2. Wait for services to be ready
3. Run `make compose-test` (ingress + dispatcher tests)
4. Generate coverage report
5. Upload artifacts (logs, coverage)
6. **Fail if:**
   - Coverage drops below baseline (`analytics/compose_coverage_baseline.json`)
   - Coverage increases but baseline not updated

**Artifacts:**
- `compose.coverprofile` — Merged coverage data
- `compose.coverage.txt` — Human-readable coverage report
- `artifacts/compose-logs.txt` — Docker logs for debugging

### Helm Integration Workflow

**Steps:**
1. Create k3d cluster
2. Install Helm chart with test values
3. Verify all pods are Running
4. Port-forward and test health endpoints
5. Clean up cluster

### Release Workflow

**Trigger:** Push tag matching `v*` pattern

**Steps:**
1. Build multi-arch Docker image
2. Push to `ghcr.io/gorizond/koldun:{tag}`
3. Generate release notes
4. Create GitHub release

---

## Troubleshooting Guide

### Common Issues

#### Envtest Hanging or Timeout

**Symptoms:**
- `make controllers-smoke` hangs indefinitely
- Tests timeout after 10 minutes

**Causes:**
1. Missing `KUBEBUILDER_ASSETS` environment variable
2. Corrupted envtest binaries
3. Deadlock from `t.Parallel()` + coverage (fixed in Session 52)

**Solutions:**
```bash
# Re-download envtest assets
make envtest-preflight

# Export path
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"

# Verify binaries exist
ls "$KUBEBUILDER_ASSETS"  # Should show kube-apiserver, etcd

# Run without coverage (faster)
make controllers-smoke

# If still hanging, check for t.Parallel() issues
grep -r "t.Parallel()" pkg/controllers/
```

#### NATS Connection Errors in Tests

**Symptoms:**
- `ErrConnectionRefused` or `nats: no servers available`
- Dispatcher tests failing with connection errors

**Causes:**
- Docker compose stack not running
- Wrong `KOLDUN_NATS_URL` / `KOLDUN_DISPATCHER_NATS_URL`

**Solutions:**
```bash
# Ensure compose stack is up
make compose-test-up

# Check NATS is running
docker compose -f docker-compose.test.yml ps

# Verify connectivity
curl http://localhost:4222

# Set environment variables
export KOLDUN_NATS_URL="nats://koldun:koldun@127.0.0.1:4222"
export KOLDUN_DISPATCHER_NATS_URL="${KOLDUN_NATS_URL}"

# Run tests
go test ./pkg/servers/dispatcher -v
```

#### Rancher Desktop kubectl logs Timeout

**Symptoms:**
- `kubectl logs` hangs or fails with TLS handshake error
- Logs work but very slow

**Causes:**
- Known Rancher Desktop VM networking issue
- TLS handshake failures between host and VM

**Workarounds:**
```bash
# Use port-forward instead
kubectl port-forward deployment/koldun 8080:8080 -n koldun
curl http://localhost:8080/healthz

# Or exec into pod
kubectl exec -it deployment/koldun -n koldun -- /bin/sh

# Or use kubectl cp to get logs
kubectl cp koldun/koldun-xyz:/tmp/logfile.txt ./logfile.txt
```

#### LLM Sidecar Health Check Failures

**Symptoms:**
- Root pods restarting frequently
- Health check failures during inference
- Exit code 137 (OOMKilled) or 139 (SIGSEGV)

**Causes:**
- CPU inference blocks dllama-api health endpoint (2-5 min per token)
- Health check too aggressive (interval 15s, threshold 4)
- Insufficient memory or CPU instructions (ARM64 without Rosetta)

**Solutions:**

**1. Health check tolerance (implemented in v0.1.0):**
```go
// pkg/servers/llm/server.go
StartupProbe:  60s interval, 10 failures = ~10 min grace period
LivenessProbe: 60s interval, 10 failures = ~10 min tolerance
```

**2. ARM64 stability (Rancher Desktop):**
```
Settings → Virtual Machine
✅ Enable VZ (Virtualization.framework)
✅ Enable Rosetta support
Restart Rancher Desktop
```

**3. Rate limiting:**
- Send **one request at a time** (CPU inference is VERY slow)
- Check NATS backlog before sending: `nats stream info`
- Monitor dispatcher logs for active requests

#### Compose Coverage Below Baseline

**Symptoms:**
- CI job fails with "Coverage decreased"
- Local `make compose-test` shows lower coverage than baseline

**Causes:**
- Code changes reduced test coverage
- Baseline file out of date

**Solutions:**
```bash
# Run compose tests locally
make compose-test

# Check coverage report
go tool cover -func compose.coverprofile | tail -n 1

# If coverage is acceptable, update baseline
make compose-update-baseline

# Commit updated baseline
git add analytics/compose_coverage_baseline.json
git commit -m "chore(analytics): update compose coverage baseline to X.X%"
```

#### Docker Image Build Failures

**Symptoms:**
- `skaffold build` fails
- Multi-arch build errors

**Causes:**
- Missing BuildKit
- Network issues pulling base images
- Insufficient disk space

**Solutions:**
```bash
# Enable BuildKit
export DOCKER_BUILDKIT=1

# Build single-arch locally first
docker build -t koldun:dev .

# For multi-arch, ensure buildx
docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 -t koldun:dev .

# Check disk space
df -h
docker system prune -a  # Clean up old images
```

---

## Important Files & References

### Must-Read Documentation

1. **README.md** — Project overview, architecture, getting started
2. **AGENTS.md** — Development guidelines, testing, code conventions
3. **CHANGELOG.md** — Version history and changes
4. **context/0_REASONING_TASK.md** — Iterative development workflow (if working on long-term features)

### Key Configuration Files

- **go.mod** — Go dependencies and toolchain version
- **Makefile** — Build automation and common tasks
- **Dockerfile** — Container image definition
- **values-dev.yaml** — Local Helm chart values
- **docker-compose.test.yml** — Integration test stack
- **.envrc** — Environment variables (direnv)

### Development Guides

Located in `context/guides/`:

- **testing.md** — Comprehensive testing guide, TDD workflow, coverage targets
- **monitoring.md** — Prometheus metrics, Grafana dashboards, health checks
- **architecture.md** — System design, NATS flows, scaling patterns
- **analytics.md** — Coverage tracking, SLA reporting, performance baselines
- **clean_architecture.md** — Development phases, handoff procedures
- **ci-envtest.md** — Envtest setup for CI/CD environments

### Session History

Located in `context/iterations/`:

- **INDEX.md** — Chronological index of all development sessions (46+ sessions)
- Individual session reports with detailed progress tracking

### Helper Scripts

Located in `hack/`:

- **print-kubebuilder-assets.sh** — Auto-detect envtest assets path
- **ci-envtest.sh** — CI checklist for envtest setup
- **check-controller-coverage.sh** — Verify controllers meet 99% threshold
- **update-compose-coverage-baseline.sh** — Update coverage baseline
- **test-helm-integration.sh** — End-to-end Helm test in k3d

### Analytics

Located in `analytics/`:

- **compose_coverage_baseline.json** — Tracked baseline for compose integration tests

---

## Development Philosophy

### Iterative Development (from context/0_REASONING_TASK.md)

This project follows a **cyclical technical specification (ЦТЗ)** approach:

1. **Phase 0**: Check for incomplete sessions in `context/iterations/`
2. **Phase 1-6**: Implement, test, document, commit
3. **Handoff**: Update `next_session` block for continuity
4. **Restart**: Begin next iteration

**For AI assistants:**
- ALWAYS read `context/0_REASONING_TASK.md` at session start
- Check `context/iterations/` for incomplete work before starting new tasks
- Update session reports with `next_session` block when handing off
- Use Agor MCP tools for session management if available

### Test-Driven Development

**The Rule:** Tests come FIRST, then implementation.

1. Write failing test
2. Implement minimal code to pass
3. Refactor while keeping tests green
4. Measure coverage, target 99%+ for controllers

**Coverage Enforcement:**
- Controllers: **99%+ required** (CI blocks if below)
- Servers: **80%+ target** (tracked via compose baseline)
- Overall: **85%+ advisory**

### Continuous Improvement

Every session should:
- ✅ Increase test coverage (even by 0.1%)
- ✅ Add tests for new code before implementation
- ✅ Fix bugs with regression tests first
- ✅ Update documentation when behavior changes
- ✅ Run full test suite before committing

---

## Quick Reference Commands

```bash
# Testing
make test                          # All tests
make controllers-smoke             # Controllers without coverage (fast)
make controllers-coverage-check    # Verify 99%+ controllers coverage
make compose-test                  # Integration tests (NATS + MinIO)
make compose-update-baseline       # Update coverage baseline

# Building
go build ./cmd/operator            # Build binary
docker build -t koldun:dev .       # Build image
skaffold build                     # Multi-arch image

# Helm
make helm-deps                     # Update dependencies
make helm-lint                     # Lint chart
make helm-template                 # Render templates
make helm-test-integration         # Test in k3d

# Envtest
make envtest-preflight             # Setup envtest assets
export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"

# Formatting
go fmt ./... && gofmt -w .         # Format code

# Coverage
make coverage                      # Generate coverage report
go tool cover -func=coverage.out   # View coverage by function
```

---

## Support & Resources

### Internal Documentation
- [README.md](README.md) — Getting started, architecture
- [AGENTS.md](AGENTS.md) — Development guidelines
- [docs/ci-envtest.md](docs/ci-envtest.md) — Envtest troubleshooting
- [context/guides/](context/guides/) — Detailed development guides

### External Resources
- [Kubernetes Operator Best Practices](https://sdk.operatorframework.io/docs/best-practices/)
- [Wrangler Framework](https://github.com/rancher/wrangler)
- [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
- [Envtest Documentation](https://book.kubebuilder.io/reference/envtest.html)
- [distributed-llama](https://github.com/b4rtaz/distributed-llama)

### Getting Help

1. **Check this file** (CLAUDE.md) for common patterns
2. **Search session history** in `context/iterations/INDEX.md`
3. **Read relevant guide** in `context/guides/`
4. **Check troubleshooting section** above
5. **Ask in context** with specific error messages and logs

---

**Last Updated**: 2025-11-19
**Maintainer**: Koldun Development Team
**License**: See repository LICENSE file
