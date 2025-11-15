# CI envtest cache & smoke checklist

This checklist describes the minimum sequence every CI runner must execute
before running controller tests. The goal: guarantee that the `envtest`
binaries (`kube-apiserver`, `etcd`) are present, `KUBEBUILDER_ASSETS` is set,
and `make controllers-smoke` is reproducible.

For GitHub Actions and most CI systems prefer `./hack/ci-envtest.sh`, which
executes every step below (installs `setup-envtest`, warms binaries, exports
`KUBEBUILDER_ASSETS`, sets `KOLD_SKIP_ENVTEST_DOWNLOAD`, and runs
`make controllers-smoke`). The remaining sections document what the script
enforces so bespoke runners can reproduce the behavior manually if needed.

## Universal recipe

1. **Restore `./bin/envtest` from cache.** Use a key that depends on at least
   `go.mod`, `pkg/controllers/envtest_suite_test.go`, and the runner OS so a
   controller-runtime upgrade automatically invalidates the cache.
2. **Ensure `setup-envtest` is installed.** In CI just run
   `go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest`
   right before the suite.
3. **Warm binaries via `make envtest-preflight`.** The target downloads missing
   control-plane files, verifies both binaries exist, and prints
   `export KUBEBUILDER_ASSETS="…"`.
4. **Export the environment variable.** Either add a step:
   ```bash
   export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
   ls "$KUBEBUILDER_ASSETS"  # sanity check kube-apiserver/etcd
   ```
   or reuse the value printed earlier. Without it `go test ./pkg/controllers`
   will attempt to download binaries again.
5. **Run `make controllers-smoke`.** The target executes
   `go test ./pkg/controllers -count=1 -timeout=10m`, re-exporting
   `KUBEBUILDER_ASSETS` so tests behave identically locally and in CI.
6. **(Optional) Fail fast.** Set `export KOLD_SKIP_ENVTEST_DOWNLOAD=1` so the
   runner immediately reports missing binaries instead of downloading them.

## GitHub Actions template

```yaml
jobs:
  controllers-envtest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true

      - name: Restore envtest cache
        uses: actions/cache@v4
        with:
          path: bin/envtest
          key: ${{ runner.os }}-envtest-${{ hashFiles('go.mod', 'pkg/controllers/envtest_suite_test.go') }}
          restore-keys: |
            ${{ runner.os }}-envtest-

      - name: Run controllers envtest checklist
        run: ./hack/ci-envtest.sh
```

### controllers-envtest job snapshot (2025-11-16)

- Re-running `./hack/ci-envtest.sh` locally (mirrors the `controllers-envtest` workflow) reported `go test ./pkg/controllers` finishing in **73.5 s** (coverage 99.8%) wall-clock time with the warmed macOS cache, which lines up with the ≈45 s local `/usr/bin/time -p make controllers-smoke` baseline once you add the extra log noise and setup work CI performs.
- The flake in `TestConversationReconcilerRetriesBucketEnsureWhenJetStreamUnavailable` (missing `"failed to reconnect to NATS, will retry"` log) has been fixed by moving `blockDialer.Store(true)` before `js.DeleteKeyValue()`, ensuring the reconnect loop hits the failure path reliably.
- **Automatic cross-platform asset selection**: The test suite's `scoreKubebuilderAsset()` function prioritizes assets matching `runtime.GOOS` and `runtime.GOARCH`. In a Linux container both `bin/envtest/k8s/1.32.0-darwin-arm64` and `bin/envtest/k8s/1.32.0-linux-arm64` can coexist — the suite automatically selects the Linux binary (score 3 for linux+arm64 vs score 1 for darwin matching only arch). No manual renaming of darwin assets is required.
- Detailed cached vs cold measurements for macOS and Linux runners now live in the README under the Envtest quick start section so CI operators can compare their timings without combing through workflow logs.

## Self-hosted runner / other CI template

On bespoke runners you can execute the same helper script:

```bash
./hack/ci-envtest.sh
```

If you need additional cache priming logic, the following bash snippet shows
the underlying sequence:

```bash
#!/usr/bin/env bash
set -euo pipefail

if [[ -d cache/bin-envtest ]]; then
  rsync -a cache/bin-envtest/ bin/envtest/
fi

go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
make envtest-preflight

export KUBEBUILDER_ASSETS="$(./hack/print-kubebuilder-assets.sh)"
export KOLD_SKIP_ENVTEST_DOWNLOAD=1
make controllers-smoke

tar -C bin -czf cache/bin-envtest.tar.gz envtest  # обновите кеш
```

You can run the script locally to confirm the runner reproduces the same
`bin/envtest/k8s/<version>-<os>-<arch>` path and that the smoke test works
before executing the rest of `go test ./...`.

## Envtest FAQ

- **`envtest assets unavailable` / missing `KUBEBUILDER_ASSETS`.** Run the
  [quick start steps](../README.md#envtest-quick-start-new-machinerunner) to
  install the binaries (`make envtest-preflight`, export
  `KUBEBUILDER_ASSETS`, wrap `make controllers-smoke` with `/usr/bin/time -p`
  to verify the ≈45 s macOS / ≈44 s Linux cached runtimes captured in the README
  table (the same runner used to report ~60 s before we trimmed the parity test).
  If the helper prints a different path on each run, ensure `./bin/envtest` is
  cached or mounted persistently.
- **`conversation bucket missing; reconnecting` never disappears even after restarting JetStream.**
  The controllers execute `TestConversationReconcilerRestoresBucketAfterRepeatedOfflineDeletion`
  during `make controllers-smoke`, which simulates shutting JetStream down,
  deleting the KV store on disk twice, and waiting for the reconnection loop
  to recreate the bucket. When you see this warning locally, re-run the
  [quick start](../README.md#envtest-quick-start-new-machinerunner) sequence
  to ensure the embedded server starts cleanly and compare the wall-clock
  time (≈45 s cached, ≈4 minutes on the very first run because of Go/toolchain
  downloads) with the baselines printed in README. If the warning persists,
  wipe `./bin/envtest` and re-run `make envtest-preflight` so the helper can
  repopulate the control-plane binaries.
- **`nats connection closed; reconnecting` repeating in tests.** The suite now
  exercises a full JetStream shutdown/restart (`TestConversationReconcilerRecoversAfterJetStreamRestart`)
  to prove reconnection is stable, so persistent loops generally mean your
  local NATS port is blocked by another process. Stop stray `nats-server`
  instances, rerun the quick start, and confirm the smoke test still finishes
  within the baseline window.
- **`conversation bucket missing; reconnecting` or `failed to reconnect to NATS, will retry`.**
  These warnings occur when the KV bucket disappears mid-run (for example,
  manual `nats kv rm` or deleting the JetStream store while the suite is
  paused). Let the controller finish its reconnect loop; it will recreate the
  bucket automatically. If the logs never switch to `conversation reconciler reconnected to NATS`,
  rerun `make controllers-smoke` so the helper resets the embedded server and
  poll interval.
- **`controllers-smoke` suddenly takes 90+ seconds.** The suite now includes
  `TestConversationReconcilerMaintainsRecoveryTimeAcrossOutages`, which deletes
  the bucket while blocking dials (synthetic outage) and wipes the JetStream
  store twice to simulate a real restart. After the Session 52 optimization each
  recovery loop completes in ≈1–3 s (still guarded by a 15 s upper bound), so a
  longer run generally signals envtest downloads or disk throttling. If your run
  slows down, compare the durations logged by the test with the README baseline
  to spot missing caches.
