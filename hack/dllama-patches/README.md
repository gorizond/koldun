# distributed-llama patches

`Dockerfile` builds dllama from the fork `gorizond/distributed-llama`, pinned by
the annotated tag `v0.16.5-koldun.1` (commit `e4d522a6`). That tag already
contains `0001-worker-readiness-and-http-resilience.patch`, so the `Dockerfile`
no longer clones upstream or applies `*.patch`. This directory is kept as the
source of truth for the patch and its history.

- `0001-worker-readiness-and-http-resilience.patch`:
  - worker writes `$DLLAMA_READY_FILE` after `weightReader.read()` (and removes
    it on reload), backing the Kubernetes readiness probe;
  - `dllama-api` catches HTTP client exceptions, answers 503 and keeps serving;
  - HTTP client sockets get a 20m (configurable) `SO_RCVTIMEO`.

When bumping `DL_VERSION`, re-apply the patches to a clean upstream checkout,
resolve conflicts, and regenerate:

```sh
git clone --depth 1 --branch <version> https://github.com/b4rtaz/distributed-llama.git /tmp/dl
cd /tmp/dl && git apply /path/to/0001-worker-readiness-and-http-resilience.patch
# edit, test with: make dllama dllama-api
git diff > /path/to/0001-worker-readiness-and-http-resilience.patch
```

Then run `docker build --no-cache --target dllama-builder .`.
