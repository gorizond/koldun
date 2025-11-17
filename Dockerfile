ARG DL_REPOSITORY=https://github.com/b4rtaz/distributed-llama.git
ARG DL_VERSION=v0.16.3

FROM alpine:latest AS dllama-builder
ARG DL_REPOSITORY
ARG DL_VERSION
RUN apk add --no-cache git make build-base
WORKDIR /src
RUN git clone --depth 1 --branch "${DL_VERSION}" "${DL_REPOSITORY}" .
# TERMUX_VERSION disables -march=native to avoid AVX/AVX2/AVX512 instructions
# that may not be supported in virtualized environments (like Rancher Desktop Lima VM)
# Additional CXXFLAGS disable ARM NEON/DOTPROD for broader compatibility
# -fno-tree-vectorize disables auto-vectorization
# -DDISABLE_NEON_DOTPROD disables ARM NEON DOTPROD intrinsics (if supported by dllama)
# -mno-outline-atomics avoids problematic atomic operations on older ARM
RUN TERMUX_VERSION=1 CXXFLAGS="-O2 -fno-tree-vectorize -fno-slp-vectorize -mno-outline-atomics" make dllama && \
    TERMUX_VERSION=1 CXXFLAGS="-O2 -fno-tree-vectorize -fno-slp-vectorize -mno-outline-atomics" make dllama-api && \
    DLLAMA_BIN=$(find . -maxdepth 4 -type f -name dllama -perm /111 | head -n1) && \
    DLLAMA_API_BIN=$(find . -maxdepth 4 -type f -name dllama-api -perm /111 | head -n1) && \
    install -Dm755 "$DLLAMA_BIN" /out/dllama && \
    install -Dm755 "$DLLAMA_API_BIN" /out/dllama-api

# Use the official Golang image as a base image
FROM golang:1.24.2 AS builder

WORKDIR /workspace

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download module dependencies; cached unless module files change
RUN go mod download

# Copy only the Go sources required to build the operator
COPY cmd ./cmd
COPY pkg ./pkg

# Build the Go app
RUN mkdir -p /out && CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/koldun ./cmd/operator

# Start a new stage from scratch
FROM alpine:latest

RUN apk add --no-cache libstdc++ libgcc
# Copy the Pre-built binary file from the previous stage
COPY --from=dllama-builder /out/dllama /usr/local/bin/dllama
COPY --from=dllama-builder /out/dllama-api /usr/local/bin/dllama-api
COPY --from=builder /out/koldun /koldun

# Command to run the executable
ENTRYPOINT ["/koldun"]
