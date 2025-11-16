# Helm Chart Integration Tests

This document describes how to run integration tests for the Koldun Helm chart using k3d.

## Overview

The integration test (`hack/test-helm-integration.sh`) creates a local Kubernetes cluster using k3d and installs the Koldun Helm chart with its dependencies (NATS, MinIO, CSI S3).

## Prerequisites

- Docker
- k3d (https://k3d.io/)
- Helm 3.x
- kubectl

## Local Testing

### Quick Start

```bash
make helm-test-integration
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLUSTER_NAME` | `koldun-test` | k3d cluster name |
| `TIMEOUT` | `600s` | Helm install timeout |
| `DEBUG` | `false` | Enable verbose debug output |
| `SKIP_OPERATOR` | `true` | Skip operator pod deployment |
| `TEST_CSI_S3` | `false` | Enable CSI S3 driver testing |
| `BUILD_IMAGE` | `false` | Build and test operator image locally |
| `IMAGE_TAG` | `test` | Tag for locally built operator image |

### Examples

```bash
# Run with debug output
DEBUG=true make helm-test-integration

# Test with locally built operator image
BUILD_IMAGE=true make helm-test-integration

# Test CSI S3 driver (requires privileged mode)
TEST_CSI_S3=true make helm-test-integration

# Custom cluster name
CLUSTER_NAME=my-test make helm-test-integration
```

## CI/CD

### GitHub Actions

The workflow runs automatically on:
- Push to any branch (if changes in `charts/` or `hack/test-helm-integration.sh`)
- Pull requests to `main`
- Manual trigger via `workflow_dispatch`

Workflow file: `.github/workflows/helm-integration.yaml`

### Manual Trigger Options

When running manually from GitHub Actions UI:
- **Build and test operator image**: Build Docker image and test full deployment
- **Test CSI S3 driver**: Enable CSI S3 driver testing (may fail without privileged containers)

## What Gets Tested

1. **Helm Dependencies**
   - Chart dependency resolution
   - Helm lint validation

2. **NATS Deployment**
   - NATS pod starts successfully
   - JetStream enabled (memory storage for tests)
   - NATS server running

3. **MinIO Deployment**
   - MinIO pod starts successfully
   - Standalone mode (no persistence for tests)
   - Service accessible

4. **CSI S3 Driver** (optional)
   - Driver installation
   - StorageClass creation
   - Note: May require privileged mode for full functionality

5. **Koldun CRDs**
   - All CRDs installed (dllamas, ingresses, models, roots, sessions, workers)

6. **Koldun Operator** (optional)
   - Operator pod deployment
   - Health check endpoint

7. **Helm Upgrade**
   - Idempotency test
   - No errors on re-apply

## Test Duration

- Typical run: 3-5 minutes locally
- CI environment: 5-10 minutes (includes image pulls)

## Troubleshooting

### Common Issues

1. **Image pull timeout**
   - Increase `TIMEOUT` environment variable
   - Check internet connectivity

2. **PVC binding issues**
   - Test uses memory storage for NATS
   - MinIO persistence disabled by default

3. **CSI S3 not registering**
   - CSI drivers require privileged mode in k3d
   - Skipped by default (`TEST_CSI_S3=false`)

4. **Operator pod not starting**
   - Image may not exist yet
   - Use `BUILD_IMAGE=true` to build locally
   - Or set `SKIP_OPERATOR=true` (default)

### Debug Mode

Enable verbose output:

```bash
DEBUG=true ./hack/test-helm-integration.sh
```

This will show:
- Pod status during wait
- PVC status
- Recent events
- Full error messages

### Manual Cleanup

If test fails and cluster is not cleaned up:

```bash
k3d cluster delete koldun-test
```

## Makefile Targets

```bash
make helm-deps              # Update Helm dependencies
make helm-lint              # Lint Helm chart
make helm-template          # Render templates (first 100 lines)
make helm-test-integration  # Run full k3d integration test
```

## Architecture

```
┌─────────────────────────────────────┐
│         k3d Cluster                 │
│                                     │
│  ┌─────────┐    ┌─────────┐        │
│  │  NATS   │    │  MinIO  │        │
│  │(JetStream)│  │(standalone)      │
│  └─────────┘    └─────────┘        │
│                                     │
│  ┌─────────┐    ┌─────────┐        │
│  │  Koldun │    │  CSI S3 │        │
│  │  CRDs   │    │(optional)│       │
│  └─────────┘    └─────────┘        │
│                                     │
│  ┌─────────────────────────┐        │
│  │   Koldun Operator       │        │
│  │   (optional)            │        │
│  └─────────────────────────┘        │
└─────────────────────────────────────┘
```

## Snapshot (2025-11-16)

- NATS: Memory storage, no PVC
- MinIO: Standalone, no persistence
- CSI S3: Disabled by default
- Operator: Skipped by default
- Test time: ~3 minutes
- All CRDs verified
- Helm upgrade idempotent
