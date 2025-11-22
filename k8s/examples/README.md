# Koldun Example Manifests

This directory contains production-ready example manifests for deploying Koldun.

## Quick Start (Local Development)

```bash
# 1. Create namespace and secrets
kubectl create namespace koldun
kubectl apply -f secret-minio.yaml

# 2. Deploy a model
kubectl apply -f model-tinyllama.yaml
# or for Qwen3:
kubectl apply -f model-qwen3-0.6b.yaml

# 3. Wait for model to be ready
kubectl wait --for=condition=Ready model/tinyllama -n koldun --timeout=600s

# 4. Create ingress for API access
kubectl apply -f ingress-local.yaml

# 5. Port-forward and test
kubectl port-forward svc/local-ingress-backend 8082:8082 -n koldun
curl http://localhost:8082/v1/models
```

## Files

| File | Description | Use Case |
|------|-------------|----------|
| `secret-minio.yaml` | MinIO credentials | Required for Model CRs |
| `model-tinyllama.yaml` | TinyLlama 1.1B model | Small model for testing (~500MB) |
| `model-qwen3-0.6b.yaml` | Qwen3 0.6B model | Lightweight model with reasoning (~1GB) |
| `model-e2e-test.yaml` | E2E test model (pre-converted) | CI/CD pipelines, fast validation |
| `ingress-local.yaml` | Local development ingress | 1 worker, no scaling |
| `ingress-production.yaml` | Production ingress | Multi-worker with auto-scaling |

## Model Selection

### E2E Test Model (Pre-Converted)
- **File**: `model-e2e-test.yaml`
- **Size**: Minimal (stub PVC)
- **Download**: None (pre-converted mode)
- **Good for**: CI/CD pipelines, GitHub Actions, fast validation
- **Features**:
  - Skips Hugging Face download
  - No conversion jobs
  - Instant Ready status
  - Works in airgapped environments
- **Use when**: Testing infrastructure without real models

### TinyLlama (Recommended for Testing)
- Size: ~500MB
- Memory: ~1GB per worker
- Good for: Quick tests, CI/CD, development

### Qwen3 0.6B (Recommended for Production Testing)
- Size: ~1GB
- Memory: ~2GB per worker
- Good for: E2E testing with reasoning capabilities
- Outputs `<think>` reasoning tokens

## Ingress Configuration

### Local Development (`ingress-local.yaml`)
- Single worker (replicaPower=0)
- No auto-scaling
- Short TTL for quick cleanup
- Uses local `koldun:dev` image

### Production (`ingress-production.yaml`)
- Multiple workers (replicaPower=2 = 4 workers)
- Auto-scaling based on backlog
- Longer TTL for persistent sessions
- Prometheus metrics enabled
- TLS ready

## Testing the API

```bash
# List available models
curl http://localhost:8082/v1/models

# Chat completion
curl -X POST http://localhost:8082/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "koldun/tinyllama",
    "messages": [
      {"role": "user", "content": "Hello, how are you?"}
    ],
    "max_tokens": 50,
    "temperature": 0.7
  }'

# Stream completion (Server-Sent Events)
curl -X POST http://localhost:8082/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "koldun/tinyllama",
    "messages": [{"role": "user", "content": "Tell me a joke"}],
    "stream": true
  }'
```

## Common Issues

1. **Model stuck in Pending**: Check MinIO credentials and network connectivity
2. **Worker crashes**: Reduce replicaPower or increase memory limits
3. **Slow response**: Check NATS connection and dispatcher logs
4. **No models listed**: Ensure Model CR has `status.conditions.Ready=True`

## Cleanup

```bash
kubectl delete ingress local-ingress -n koldun
kubectl delete model tinyllama -n koldun
kubectl delete secret minio-creds -n koldun
```
