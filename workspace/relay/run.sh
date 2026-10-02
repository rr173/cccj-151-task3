#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export RELAY_PORT="${RELAY_PORT:-8090}"
export RELAY_DB="${RELAY_DB:-$(pwd)/data/relay.db}"
exec python3 app/server.py
