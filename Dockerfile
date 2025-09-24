ARG DL_REPOSITORY=https://github.com/b4rtaz/distributed-llama.git
ARG DL_VERSION=v0.16.2

FROM alpine:latest AS dllama-builder
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

# Use the official Golang image as a base image
FROM golang:1.24.2 AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -o koldun .

# Start a new stage from scratch
FROM alpine:latest

RUN apk add --no-cache libstdc++ libgcc
# Copy the Pre-built binary file from the previous stage
COPY --from=dllama-builder /out/dllama /usr/local/bin/dllama
COPY --from=dllama-builder /out/dllama-api /usr/local/bin/dllama-api
COPY --from=builder /app/koldun /koldun

# Command to run the executable
ENTRYPOINT ["/koldun"]