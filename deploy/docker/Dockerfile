# syntax=docker/dockerfile:1
#
# Builds one jobsys command into a small image:
#   docker build -f deploy/docker/Dockerfile --build-arg SERVICE=api .
#   docker build -f deploy/docker/Dockerfile --build-arg SERVICE=worker .

FROM golang:1.26-alpine AS build
WORKDIR /src

# Download modules in their own layer so code changes don't refetch them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
COPY web ./web

ARG SERVICE
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" || { echo "build with --build-arg SERVICE=api or SERVICE=worker"; exit 1; } && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/$SERVICE

# Alpine rather than distroless so `docker exec ... sh` works while learning,
# and so the API healthcheck can use busybox wget.
FROM alpine:3.22
RUN adduser -D -u 10001 app && mkdir -p /data/output && chown app /data/output
USER app
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
