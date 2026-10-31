# distributed-llama patches

`Dockerfile` builds dllama from the fork `gorizond/distributed-llama`, pinned by
the annotated tag `v0.16.5-koldun.3` (commit `163d705`). That tag already
contains all patches below, so the `Dockerfile` no longer clones upstream or
applies `*.patch`. This directory is kept as the source of truth for the patches
and their history.

- `0001-worker-readiness-and-http-resilience.patch`:
  - worker writes `$DLLAMA_READY_FILE` after `weightReader.read()` (and removes
    it on reload), backing the Kubernetes readiness probe;
  - `dllama-api` catches HTTP client exceptions, answers 503 and keeps serving;
  - HTTP client sockets get a 20m (configurable) `SO_RCVTIMEO`.
- `0002-progress-logging.patch`:
  - `dllama-api` logs prefill and generation progress with elapsed time and
    tok/s, so long prompts/answers are observable;
  - env-gated: `DLLAMA_PROGRESS_EVERY` (prefill, tokens) and
    `DLLAMA_GEN_PROGRESS_EVERY` (generation, tokens); `0`/unset disables each.
- `0004-nbatches.patch`:
  - prefill batch size is configurable via `DLLAMA_N_BATCHES` (1..256, default
    32); parsed once in the shared CLI args, so root and workers agree;
  - used by the experiment that sets `DLLAMA_N_BATCHES=64` on root and workers.

All patches are already in fork tag `v0.16.5-koldun.3`, so the `Dockerfile`
does not apply them.

When bumping `DL_VERSION`, re-apply the patches to a clean upstream checkout,
resolve conflicts, and regenerate:

```sh
git clone --depth 1 --branch <version> https://github.com/b4rtaz/distributed-llama.git /tmp/dl
cd /tmp/dl && git apply /path/to/0001-worker-readiness-and-http-resilience.patch
# edit, test with: make dllama dllama-api
git diff > /path/to/0001-worker-readiness-and-http-resilience.patch
```

Then run `docker build --no-cache --target dllama-builder .`.
