# syntax=docker/dockerfile:1.7
# execution-service
#
# Multi-stage build with three independent final targets — cmd/server,
# cmd/worker and cmd/connector-worker are separate deployments/pods that scale
# independently (LLD §1.6), even though they share this one Dockerfile/builder
# stage.
#
#   Stage 1 (builder)          — compiles all three binaries with private module access via build secret.
#   Stage 2 (server)           — distroless runtime image for cmd/server.
#   Stage 3 (worker)           — distroless runtime image for cmd/worker.
#   Stage 4 (connector-worker) — distroless runtime image for cmd/connector-worker.
#
# Build (CI, target selects the binary):
#   docker build --target server --secret id=go_private_token,env=GO_PRIVATE_TOKEN -t execution-service-server:$TAG .
#   docker build --target worker --secret id=go_private_token,env=GO_PRIVATE_TOKEN -t execution-service-worker:$TAG .
#   docker build --target connector-worker --secret id=go_private_token,env=GO_PRIVATE_TOKEN -t execution-service-connector-worker:$TAG .
#
# Base images are digest-pinned (see .docker-digests). Re-pin with
# `make pin-base-images` — it preserves each FROM line's own `AS <stage>`
# regardless of how many stages share a base image.

FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder

ARG BUILD_VERSION=dev
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC \
    GONOSUMDB=github.com/BCBP-SOLUTIONS-FZC-LLC/*

WORKDIR /build

# git required for private module access
RUN apk add --no-cache git ca-certificates

RUN --mount=type=secret,id=go_private_token \
    git config --global credential.helper store && \
    echo "https://x-access-token:$(cat /run/secrets/go_private_token)@github.com" > ~/.git-credentials && \
    chmod 600 ~/.git-credentials

COPY go.mod go.sum* ./
RUN go mod download && go mod verify

COPY . .

# Strip debug symbols to minimize binary size.
RUN go build \
    -ldflags="-w -s -X main.version=${BUILD_VERSION}" \
    -trimpath \
    -o /build/bin/server \
    ./cmd/server
RUN go build \
    -ldflags="-w -s -X main.version=${BUILD_VERSION}" \
    -trimpath \
    -o /build/bin/worker \
    ./cmd/worker
RUN go build \
    -ldflags="-w -s -X main.version=${BUILD_VERSION}" \
    -trimpath \
    -o /build/bin/connector-worker \
    ./cmd/connector-worker

# gcr.io/distroless/static-debian12: no shell, no libc, no package manager.
# Runs as nonroot (uid 65532) by default.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35 AS server

COPY --from=builder /build/bin/server /server

EXPOSE 8080 9090

ENTRYPOINT ["/server"]

FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35 AS worker

COPY --from=builder /build/bin/worker /worker

EXPOSE 8081

ENTRYPOINT ["/worker"]

# cmd/connector-worker polls a Valkey Stream and reports outcomes back over
# HTTP — it serves nothing inbound but its own /healthz + /metrics
# (WORKER_HEALTH_PORT, default 8081).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35 AS connector-worker

COPY --from=builder /build/bin/connector-worker /connector-worker

EXPOSE 8081

ENTRYPOINT ["/connector-worker"]
