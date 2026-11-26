#!/bin/sh
set -e

PUID="${PUID:-99}"
PGID="${PGID:-100}"
CONFIG_DIR="${ECHO_CONFIG_DIR:-/config}"

mkdir -p "$CONFIG_DIR"

if [ "$(id -u)" = "0" ] && [ "$PUID" != "0" ]; then
    chown -R "${PUID}:${PGID}" "$CONFIG_DIR" 2>/dev/null || true
    exec su-exec "${PUID}:${PGID}" "$@"
fi

exec "$@"
