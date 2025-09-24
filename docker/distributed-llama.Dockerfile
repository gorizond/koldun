# syntax=docker/dockerfile:1.4
ARG DL_REPOSITORY=https://github.com/b4rtaz/distributed-llama.git
ARG DL_VERSION=main

FROM alpine:3.19 AS build
ARG DL_REPOSITORY
ARG DL_VERSION
RUN apk add --no-cache git make go build-base bash
WORKDIR /src
RUN git clone --depth 1 --branch "${DL_VERSION}" "${DL_REPOSITORY}" .
RUN make dllama && make dllama-api && \
    DLLAMA_BIN=$(find . -maxdepth 4 -type f -name dllama -perm /111 | head -n1) && \
    DLLAMA_API_BIN=$(find . -maxdepth 4 -type f -name dllama-api -perm /111 | head -n1) && \
    install -Dm755 "$DLLAMA_BIN" /out/dllama && \
    install -Dm755 "$DLLAMA_API_BIN" /out/dllama-api

FROM alpine:3.19
RUN apk add --no-cache libstdc++ libgcc
COPY --from=build /out/dllama /usr/local/bin/dllama
COPY --from=build /out/dllama-api /usr/local/bin/dllama-api
ENTRYPOINT ["/usr/local/bin/dllama"]