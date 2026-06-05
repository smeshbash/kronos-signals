#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Kronos Trading System — module launcher
# Sources .env and runs a module inside the project venv.
# Called by supervisor as the command for every [program:kronos-*] entry.
#
# Usage: run_module.sh <script.py>
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$DIR/.env" ]]; then
    echo "ERROR: $DIR/.env not found. Run install.sh and fill in .env before starting." >&2
    exit 1
fi

# Export all vars from .env into the environment
set -a
# shellcheck source=/dev/null
source "$DIR/.env"
set +a

# Replace this shell with the Python process so supervisor tracks the right PID
exec "$DIR/.venv/bin/python" "$DIR/$1"
