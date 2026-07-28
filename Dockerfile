# syntax=docker/dockerfile:1.7
# execution-service
#
# Multi-stage build with two independent final targets — cmd/server and
# cmd/worker are separate deployments/pods that scale independently (LLD §1.6),
# even though they share this one Dockerfile/builder stage.
#
#   Stage 1 (builder) — compiles both binaries with private module access via build secret.
#   Stage 2 (server)  — distroless runtime image for cmd/server.
#   Stage 3 (worker)  — distroless runtime image for cmd/worker.
#
# Build (CI, target selects the binary):
#   docker build --target server --secret id=go_private_token,env=GO_PRIVATE_TOKEN -t execution-service-server:$TAG .
#   docker build --target worker --secret id=go_private_token,env=GO_PRIVATE_TOKEN -t execution-service-worker:$TAG .
#
# TODO before real deployment: pin base image digests via `make pin-base-images`
# (deferred — no docker-build/trivy CI job exists yet at this Tier-0 stage).

FROM golang:1.26-alpine AS builder

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

# gcr.io/distroless/static-debian12: no shell, no libc, no package manager.
# Runs as nonroot (uid 65532) by default.
FROM gcr.io/distroless/static-debian12:nonroot AS server

COPY --from=builder /build/bin/server /server

EXPOSE 8080 9090

ENTRYPOINT ["/server"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker

COPY --from=builder /build/bin/worker /worker

EXPOSE 8081

ENTRYPOINT ["/worker"]
