# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/echo-server ./cmd/echo-server

FROM alpine:3.21
ARG TARGETARCH=amd64
RUN apk add --no-cache ffmpeg ca-certificates tzdata su-exec curl && \
    CF_ARCH=$([ "${TARGETARCH}" = "arm64" ] && echo "arm64" || echo "amd64") && \
    curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${CF_ARCH}" -o /usr/local/bin/cloudflared && \
    chmod +x /usr/local/bin/cloudflared
WORKDIR /app
COPY --from=builder /out/echo-server /usr/local/bin/echo-server
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/echo-server /usr/local/bin/entrypoint.sh

ENV ECHO_CONFIG_DIR=/config \
    ECHO_MUSIC_DIR=/music \
    ECHO_API_PORT=48721 \
    ECHO_PARTY_PORT=53333 \
    ECHO_PARTY_HTTPS_PORT=53334 \
    ECHO_SCAN_INTERVAL=30m \
    PUID=99 \
    PGID=100

VOLUME ["/config", "/music"]
EXPOSE 48721 53333 53334

LABEL net.unraid.docker.managed="dockerman" \
      net.unraid.docker.webui="http://[IP]:[PORT:48721]/" \
      net.unraid.docker.icon="http://[IP]:[PORT:48721]/icon.png" \
      org.opencontainers.image.title="My Echo Server" \
      org.opencontainers.image.description="Self-hosted Hi-Fi music indexer, Apple TV backend, Web Player & Party Mode Jukebox for My Echo"

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${ECHO_API_PORT}/healthz >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/usr/local/bin/echo-server"]
