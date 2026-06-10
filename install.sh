#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Kronos Trading System — Ubuntu LTS bare-metal installer
# Tested: Ubuntu 22.04 LTS (Jammy), Ubuntu 24.04 LTS (Noble)
# Hardware: Intel i5 8th gen + NVIDIA MX series (Optimus)
#           PyTorch runs CPU-only — no CUDA driver required, device stays stable.
#           To enable GPU inference later: set KRONOS_SHADOW_DEVICE=cuda in .env
#           and rerun:  bash install.sh --cuda
#
# One-time setup:
#   git clone https://github.com/smeshbash/kronos-signals.git --branch linux kronos
#   cd kronos
#   bash install.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

KRONOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$KRONOS_DIR/.venv"
LOG_DIR="$KRONOS_DIR/logs"
SUPERVISOR_CONF="/etc/supervisor/conf.d/kronos.conf"
USE_CUDA=false

for arg in "$@"; do
    [[ "$arg" == "--cuda" ]] && USE_CUDA=true
done

# ── Guard: must not run as root ───────────────────────────────────────────────
if [[ "$EUID" -eq 0 ]]; then
    echo "✗  Run as your normal user, not root (sudo is called internally where needed)."
    exit 1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║      Kronos Trading System — Ubuntu LTS Installer       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
[[ "$USE_CUDA" == "true" ]] && echo "  Mode: CUDA (MX series GPU)" || echo "  Mode: CPU-only (safe default)"
echo ""

# ── 1/7  System packages ──────────────────────────────────────────────────────
echo "─── 1/7  System packages"
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    python3 python3-venv python3-pip python3-dev \
    supervisor git curl build-essential \
    libsqlite3-dev ca-certificates
echo "  ✓ done"

# ── 2/7  Directories ──────────────────────────────────────────────────────────
echo "─── 2/7  Directories"
mkdir -p "$KRONOS_DIR/data" "$KRONOS_DIR/models" "$LOG_DIR"
echo "  ✓ data/  models/  logs/"

# ── 3/7  .env ─────────────────────────────────────────────────────────────────
echo "─── 3/7  Environment file"
if [[ ! -f "$KRONOS_DIR/.env" ]]; then
    cp "$KRONOS_DIR/.env.example" "$KRONOS_DIR/.env"
    echo "  ✓ .env created from .env.example"
    echo ""
    echo "  ⚠  REQUIRED: edit .env before starting modules."
    echo "     Fill in: KRONOS_API_KEY, KRONOS_API_SECRET,"
    echo "              KRONOS_TELEGRAM_BOT_TOKEN, KRONOS_TELEGRAM_CHAT_ID"
    echo "     Command:  nano $KRONOS_DIR/.env"
    echo ""
else
    echo "  ✓ .env already exists (not overwritten)"
fi

# ── 4/7  Python virtual environment ───────────────────────────────────────────
echo "─── 4/7  Python virtual environment"
if [[ ! -d "$VENV" ]]; then
    python3 -m venv "$VENV"
    echo "  ✓ venv created at .venv/"
else
    echo "  ✓ venv exists (reusing)"
fi
"$VENV/bin/pip" install --quiet --upgrade pip setuptools wheel

# ── 5/7  PyTorch ──────────────────────────────────────────────────────────────
echo "─── 5/7  PyTorch"
if [[ "$USE_CUDA" == "true" ]]; then
    # CUDA 12.1 — compatible with MX150/MX230/MX250/MX330 (Pascal/Turing)
    # Requires: sudo apt install nvidia-driver-535 (reboot, then rerun with --cuda)
    TORCH_INDEX="https://download.pytorch.org/whl/cu121"
    echo "  Installing PyTorch CUDA 12.1..."
else
    TORCH_INDEX="https://download.pytorch.org/whl/cpu"
    echo "  Installing PyTorch CPU (no GPU driver required)..."
fi
"$VENV/bin/pip" install --quiet torch torchvision --index-url "$TORCH_INDEX"
echo "  ✓ torch installed"

# ── 6/7  Python dependencies ──────────────────────────────────────────────────
echo "─── 6/7  Python dependencies"
"$VENV/bin/pip" install --quiet -r "$KRONOS_DIR/requirements.txt"
echo "  ✓ all packages installed"

# ── 7/7  Supervisor ───────────────────────────────────────────────────────────
echo "─── 7/7  Supervisor"
chmod +x "$KRONOS_DIR/run_module.sh"

# Inject absolute paths into the template → /etc/supervisor/conf.d/kronos.conf
sed \
    -e "s|__KRONOS_DIR__|$KRONOS_DIR|g" \
    -e "s|__LOG_DIR__|$LOG_DIR|g" \
    "$KRONOS_DIR/supervisor.conf.template" \
    | sudo tee "$SUPERVISOR_CONF" > /dev/null
sudo chmod 644 "$SUPERVISOR_CONF"

# Enable supervisor (idempotent) and reload
sudo systemctl enable supervisor --now 2>/dev/null || sudo service supervisor start || true
sleep 1
sudo supervisorctl reread
sudo supervisorctl update
echo "  ✓ supervisor config installed and reloaded"

# ── Summary ────────────────────────────────────────────────────────────────────
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                   Install complete ✓                    ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "  Next steps:"
echo ""
echo "  1. Fill in secrets:"
echo "       nano $KRONOS_DIR/.env"
echo ""
echo "  2. Start all modules:"
echo "       make start"
echo ""
echo "  3. Check status:"
echo "       make status"
echo ""
echo "  4. Dashboard:"
echo "       http://$LOCAL_IP:8050"
echo ""
echo "  If dashboard is unreachable, open the port:"
echo "       sudo ufw allow 8050/tcp"
echo ""
echo "  All commands:  make help"
echo ""
