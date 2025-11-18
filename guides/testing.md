# Testing Guide for Koldun Operator

This guide covers testing strategies for the Koldun distributed LLM inference platform, including unit tests, integration tests, and end-to-end testing workflows.

## Table of Contents

- [Overview](#overview)
- [Unit Testing](#unit-testing)
- [Integration Testing](#integration-testing)
- [End-to-End Testing](#end-to-end-testing)
- [CI/CD Pipeline](#cicd-pipeline)
- [Local Development Testing](#local-development-testing)

## Overview

The Koldun platform uses a comprehensive testing strategy:

1. **Unit Tests** - Fast, isolated tests for individual components
2. **Integration Tests** - Test interaction between components (Helm, Kubernetes)
3. **End-to-End Tests** - Full workflow testing from HTTP API to LLM inference

**Test Coverage**:
- Unit tests: 100% passing (all packages)
- Controllers: envtest-based integration tests
- Helm charts: k3d-based integration tests
- E2E: Manual/automated workflow validation

## Unit Testing

### Running Unit Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test ./pkg/controllers/...
go test ./pkg/servers/...

# Run with coverage
go test -cover ./...
```

### Test Organization

- Tests live alongside source code: `*_test.go`
- Use table-driven tests where applicable
- Mock external dependencies (NATS, Kubernetes)

**Example test locations**:
- `pkg/controllers/*_test.go` - Controller reconciliation logic
- `pkg/servers/llm/server_test.go` - LLM server tests
- `pkg/tokens/*_test.go` - Token validation tests

## Integration Testing

### Helm Integration Tests

Helm integration tests validate chart deployment in a k3d cluster:

```bash
# Run Helm integration tests
./hack/test-helm-integration.sh

# With debug output
DEBUG=true ./hack/test-helm-integration.sh

# Test with CSI S3 driver
TEST_CSI_S3=true ./hack/test-helm-integration.sh
```

**What it tests**:
- Helm chart syntax and dependencies
- CRD installation
- Operator deployment
- NATS and MinIO setup
- Basic reconciliation

**CI workflow**: `.github/workflows/helm-integration.yaml`

### Controller Integration Tests (envtest)

Controllers use envtest for integration testing with a real Kubernetes API:

```bash
# Run envtest checklist
./hack/ci-envtest.sh

# Direct envtest run
go test ./pkg/controllers/... -v
```

**CI workflow**: `.github/workflows/ci-build.yaml` (controllers-envtest job)

## End-to-End Testing

### Overview

E2E tests validate the complete request flow:

```
HTTP Request → Backend → NATS KV → Operator → Session/Dllama/Root/Worker CRs
  → Kubernetes Pods → Dispatcher → NATS Streams → LLM Workers → dllama-api
  → Inference → Response
```

### Prerequisites

**Local environment requirements**:
- Kubernetes cluster (Rancher Desktop, k3d, minikube, or kind)
- kubectl configured with cluster access
- Helm 3.x
- NATS with JetStream enabled
- MinIO or S3-compatible storage
- Pre-converted model files (or download capability)

**Cluster setup**:
```bash
# Create namespace
kubectl create namespace koldun

# Install Koldun with Helm
helm upgrade --install koldun charts/koldun/ -n koldun -f values-dev.yaml --wait
```

### E2E Test Workflow

#### 1. Verify Infrastructure

```bash
# Check pods are running
kubectl get pods -n koldun

# Verify CRDs installed
kubectl get crd | grep koldun

# Check NATS and MinIO
kubectl get pods -n koldun | grep -E "(nats|minio)"
```

Expected output:
```
koldun-XXXXXXX-XXXXX                 1/1     Running
koldun-minio-XXXXXXXXX-XXXXX        1/1     Running
koldun-nats-0                        2/2     Running
```

#### 2. Create Model CR

```bash
# Apply Model CR for Qwen3 0.6B (or TinyLlama)
kubectl apply -f - <<EOF
apiVersion: koldun.gorizond.io/v1
kind: Model
metadata:
  name: qwen3-0.6b
  namespace: koldun
spec:
  localPath: qwen3-0.6b
  preConverted: true
  preConvertedSizeBytes: 1237000000
  preConvertedSizeHuman: "14 GB"
  launchOptions:
    - "--model"
    - "/model/dllama_model_qwen3_0.6b_q40.m"
    - "--tokenizer"
    - "/model/dllama_tokenizer_qwen3_0.6b.t"
    - "--buffer-float-type"
    - "q80"
  objectStorage:
    endpoint: http://koldun-minio:9000
    bucketForSource: koldun-models
    bucketForConvert: koldun-models
    secretRef:
      name: minio-credentials
      namespace: koldun
  conversion:
    weightsFloatType: q40
EOF

# Wait for model to be ready
kubectl wait --for=condition=Ready model/qwen3-0.6b -n koldun --timeout=300s

# Verify model status
kubectl get model qwen3-0.6b -n koldun
```

Expected status:
```
NAME         READY   DOWNLOAD    CONVERSION
qwen3-0.6b   True    Succeeded   NotRequested
```

#### 3. Create Ingress CR

```bash
# Apply Ingress CR
kubectl apply -f - <<EOF
apiVersion: koldun.gorizond.io/v1
kind: Ingress
metadata:
  name: qwen3-ingress
  namespace: koldun
spec:
  backend:
    image: koldun:dev
    dispatcherImage: koldun:dev
    rootImage: koldun:dev
    workerImage: koldun:dev
    allowAnonymous: true
    conversationTTL: 10m
    responseTimeout: 5m
    replicaPower: 2
    sessionScaling:
      minDllamas: 1
      maxDllamas: 2
      scaleUpBacklog: 3
      scaleDownIdleSeconds: 300
    nats:
      url: "nats://koldun-nats:4222"
      kvBucket: koldun_ttl
      modelsBucket: koldun_models
      tokensBucket: koldun_tokens
      modelPrefix: "model/"
      tokenPrefix: "token/"
  route:
    host: qwen3.koldun.dev
  service:
    port: 8082
EOF

# Wait for backend to be ready
kubectl wait --for=condition=Ready ingress.koldun.gorizond.io/qwen3-ingress -n koldun --timeout=60s

# Verify backend pod
kubectl get pods -n koldun | grep qwen3-ingress-backend
```

#### 4. Port Forward to Backend

```bash
# Port forward to backend service
kubectl port-forward -n koldun svc/qwen3-ingress-backend 8082:8082 &

# Verify backend is accessible
curl http://localhost:8082/v1/models
```

Expected response:
```json
{
  "data": [
    {
      "id": "koldun/qwen3-0.6b",
      "object": "model",
      "created": 1763460826,
      "owned_by": "koldun",
      "namespace": "koldun",
      "size_bytes": 1237000000,
      "size_human": "14 GB"
    }
  ],
  "object": "list"
}
```

#### 5. Send Chat Completion Request

```bash
# Send test request
curl -X POST http://localhost:8082/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-token-123" \
  -d '{
    "model": "koldun/qwen3-0.6b",
    "messages": [
      {"role": "user", "content": "Say hello in one word"}
    ],
    "max_tokens": 5
  }'
```

**Note**: CPU inference takes 2-5 minutes for response. This is expected behavior.

#### 6. Monitor Resource Creation

While the request is processing, monitor automatic resource creation:

```bash
# Watch for Session/Dllama/Root/Worker creation
watch -n 2 "kubectl get sessions,dllamas,roots,workers -n koldun"

# Check pod creation
watch -n 2 "kubectl get pods -n koldun | grep session-"

# View backend logs
kubectl logs -n koldun deployment/qwen3-ingress-backend --tail=20 -f

# View operator logs
kubectl logs -n koldun deployment/koldun --tail=20 -f
```

Expected resources created (within 7-15 seconds):
```
NAME                                                   HASH                         MODEL
session.koldun.gorizond.io/session-a3452f00...        a3452f008fe9a3f0...          qwen3-0.6b

NAME                                                   READY   READYWORKERS   REPLICAPOWER
dllama.koldun.gorizond.io/session-a3452f00-dllama-... True    3              2

NAME                                                   AGE
root.koldun.gorizond.io/session-a3452f00-dllama-...  20s

NAME                                                   AGE
worker.koldun.gorizond.io/session-a3452f00-dllama-... 20s
```

Expected pods created:
```
session-a3452f00...-dispatcher-XXXXX         1/1     Running
session-a3452f00...-dllama-XXXX-0            1/1     Running   (worker 0)
session-a3452f00...-dllama-XXXX-1            1/1     Running   (worker 1)
session-a3452f00...-dllama-XXXX-2            1/1     Running   (worker 2)
session-a3452f00...-XXXXX-root-0             2/2     Running   (root: dllama + llm)
```

**Topology validated**: 1 root + 3 workers ✅

#### 7. Verify Dispatcher Assignment

```bash
# Check dispatcher logs
kubectl logs -n koldun deployment/session-XXXXX-dispatcher

# Look for "dispatcher dispatched assignment" messages
```

Expected log entries:
```
dispatcher dispatched assignment assignmentId=assign-1763460910461332134 worker=session-a3452f00-dllama-m8cr8
```

#### 8. Verify LLM Worker Processing

```bash
# Find root pod name
ROOT_POD=$(kubectl get pods -n koldun -l koldun.gorizond.io/component=root -o name | head -1)

# Check LLM sidecar logs
kubectl logs -n koldun $ROOT_POD -c llm

# Look for "processing request" message
```

Expected log entries:
```
dllama-api sidecar is ready endpoint="http://127.0.0.1:9999/v1/models"
subscribed to request stream subject=sessions.a3452f00...
processing request assignmentId=assign-1763460910461332134 model=koldun/qwen3-0.6b
```

#### 9. Wait for Response

The request will complete after 2-5 minutes (CPU inference). The response will be streamed back through NATS to the backend.

#### 10. Verify Session Cleanup

After `conversationTTL` expires (default: 10 minutes), resources should be cleaned up:

```bash
# Wait for TTL expiration
sleep 600

# Verify resources are deleted
kubectl get sessions,dllamas,roots,workers,pods -n koldun | grep session-
```

Resources should be automatically deleted by the operator.

### E2E Test Success Criteria

✅ **All criteria must pass**:

1. Model CR reaches Ready status
2. Ingress CR creates backend pod
3. Backend responds to `/v1/models` with model list
4. POST `/v1/chat/completions` creates conversation in NATS KV
5. Operator automatically creates Session/Dllama/Root/Worker CRs (within 10 seconds)
6. Kubernetes creates pods: 1 dispatcher + 1 root + 3 workers (within 20 seconds)
7. Dispatcher assigns work to workers via NATS
8. LLM worker processes inference request
9. Response is returned to client (2-5 minutes for CPU inference)
10. Resources are cleaned up after TTL expiration

### Known Limitations (CPU Inference)

⚠️ **Important**: CPU inference is VERY slow (2-5 minutes per request)

**Best practices**:
- **Never send multiple requests to the same root pod** - causes timeout
- **Use NATS queues** for load management
- **Monitor backlog** before sending requests
- **One request at a time** on local machines
- Check system load before testing

**Health Check Tolerance (Fixed in Session 49)**:
- LLM sidecar health checks now tolerate slow CPU inference
- Health check interval: **60s** (previously 15s)
- Failure threshold: **10 consecutive failures** (previously 4)
- Grace period: **~10 minutes** before pod restart
- This prevents premature evictions during long-running inference requests
- See `cmd/operator/main.go:176-177` and `pkg/servers/llm/server.go:38-39` for configuration

See `context/0_REASONING_TASK.md` lines 363-378 for detailed CPU inference rules.

## CI/CD Pipeline

### Current CI Workflows

1. **ci-build.yaml** - Unit tests and image build
   - Run

s envtest for controllers
   - Builds multi-arch Docker images
   - Pushes to ghcr.io

2. **helm-integration.yaml** - Helm chart integration
   - Deploys to k3d cluster
   - Validates CRDs and basic reconciliation
   - Runs on chart changes

3. **release.yml** - Release automation
   - Triggered on version tags
   - Builds and signs Docker images
   - Publishes Helm charts

### Future: E2E CI Testing

**Planned workflow** (not yet implemented):
- Create `.github/workflows/e2e-test.yaml`
- Use k3d cluster with NATS and MinIO
- Download small test model (TinyLlama 1.1B)
- Execute full E2E workflow
- Validate request completion
- Clean up resources

**Challenges**:
- CPU inference too slow for CI (2-5 minutes)
- Need GPU runners or mock inference
- Large model downloads (bandwidth/time)

**Possible solutions**:
- Use tiny model (Qwen3 0.6B or smaller)
- Mock dllama-api responses for fast testing
- Test resource creation only (skip inference)
- Use self-hosted GPU runners

## Local Development Testing

### Quick Test Loop

```bash
# 1. Build operator image
docker build -t koldun:dev .

# 2. Deploy/upgrade with Helm
helm upgrade koldun charts/koldun/ -n koldun -f values-dev.yaml --wait

# 3. Run unit tests
go test ./...

# 4. Test specific component
kubectl logs -n koldun deployment/koldun --tail=50 -f

# 5. Manual E2E test (follow workflow above)
```

### Development Best Practices

1. **Always run unit tests before commit**:
   ```bash
   go test ./... && git commit
   ```

2. **Verify Helm chart syntax**:
   ```bash
   helm lint charts/koldun
   helm template charts/koldun | kubectl apply --dry-run=client -f -
   ```

3. **Check operator logs for errors**:
   ```bash
   kubectl logs -n koldun deployment/koldun | grep -E "(ERROR|WARN)"
   ```

4. **Monitor NATS streams**:
   ```bash
   kubectl exec -n koldun koldun-nats-0 -c nats -- nats stream ls
   kubectl exec -n koldun koldun-nats-0 -c nats -- nats stream info KOLDUN_CONVERSATIONS
   ```

5. **Clean up test resources**:
   ```bash
   kubectl delete sessions,dllamas,roots,workers --all -n koldun
   ```

## Troubleshooting

### Tests Failing

**Unit tests fail**:
```bash
# Clean cache and re-run
go clean -testcache
go test -v ./path/to/failing/package
```

**Envtest fails**:
```bash
# Re-download envtest binaries
rm -rf bin/envtest
go test ./pkg/controllers/...
```

### E2E Issues

**Backend not responding**:
```bash
# Check pod status
kubectl get pods -n koldun | grep ingress-backend
kubectl logs -n koldun deployment/qwen3-ingress-backend

# Verify port-forward
lsof -i :8082
```

**No Session created**:
```bash
# Check operator logs
kubectl logs -n koldun deployment/koldun | grep -A 5 "conversation"

# Verify NATS connection
kubectl exec -n koldun koldun-nats-0 -c nats -- nats kv ls
kubectl exec -n koldun koldun-nats-0 -c nats -- nats kv get koldun_ttl
```

**Pods not created**:
```bash
# Check Session status
kubectl describe session -n koldun

# Check for events
kubectl get events -n koldun --sort-by='.lastTimestamp' | tail -20
```

**Request timeout**:
- Check dispatcher logs: `kubectl logs deployment/session-XXX-dispatcher -n koldun`
- Verify workers are ready: `kubectl get workers -n koldun`
- Check LLM sidecar: `kubectl logs POD_NAME -c llm -n koldun`
- CPU inference is slow - wait 5+ minutes

### Resource Cleanup

**Manual cleanup**:
```bash
# Delete all session resources
kubectl delete sessions,dllamas,roots,workers --all -n koldun

# Delete specific session
kubectl delete session session-HASH -n koldun
```

**Full reset**:
```bash
# Uninstall Helm release
helm uninstall koldun -n koldun

# Delete namespace
kubectl delete namespace koldun

# Reinstall
kubectl create namespace koldun
helm install koldun charts/koldun/ -n koldun -f values-dev.yaml
```

## References

- [Koldun Operator README](../README.md)
- [Cyclic Task Specification](../context/0_REASONING_TASK.md)
- [Session 48 E2E Test Report](../context/iterations/2025-11-18_15-17_session_48.md)
- [Helm Chart Values](../charts/koldun/values.yaml)
- [CI Workflows](../.github/workflows/)

---

**Last updated**: 2025-11-18 (Session 49)
**Test coverage**: 100% unit tests passing
**E2E status**: Manually validated, CI integration planned
