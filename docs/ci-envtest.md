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
   `go test ./pkg/controllers -count=1 -timeout=5m`, re-exporting
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
