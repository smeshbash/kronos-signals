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

# ── 1/8  System packages ──────────────────────────────────────────────────────
echo "─── 1/8  System packages"
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    python3 python3-venv python3-pip python3-dev \
    supervisor git curl build-essential \
    libsqlite3-dev ca-certificates
echo "  ✓ done"

# ── 1b  cloudflared ───────────────────────────────────────────────────────────
echo "─── 1b  cloudflared (Cloudflare tunnel)"
if ! command -v cloudflared &>/dev/null; then
    ARCH=$(dpkg --print-architecture)
    curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}.deb" \
        -o /tmp/cloudflared.deb
    sudo dpkg -i /tmp/cloudflared.deb
    rm -f /tmp/cloudflared.deb
    echo "  ✓ cloudflared installed"
else
    echo "  ✓ cloudflared already installed ($(cloudflared --version 2>&1 | head -1))"
fi

# ── 2/8  Directories ──────────────────────────────────────────────────────────
echo "─── 2/8  Directories"
mkdir -p "$KRONOS_DIR/data" "$KRONOS_DIR/models" "$LOG_DIR" "$KRONOS_DIR/.cache/huggingface"
echo "  ✓ data/  models/  logs/  .cache/huggingface/"

# ── 3/8  .env ─────────────────────────────────────────────────────────────────
echo "─── 3/8  Environment file"
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

# ── 4/8  Python virtual environment ───────────────────────────────────────────
echo "─── 4/8  Python virtual environment"
if [[ ! -d "$VENV" ]]; then
    python3 -m venv "$VENV"
    echo "  ✓ venv created at .venv/"
else
    echo "  ✓ venv exists (reusing)"
fi
"$VENV/bin/pip" install --quiet --upgrade pip setuptools wheel

# ── 5/8  PyTorch ──────────────────────────────────────────────────────────────
echo "─── 5/8  PyTorch"
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

# ── 6/8  Python dependencies ──────────────────────────────────────────────────
echo "─── 6/8  Python dependencies"
"$VENV/bin/pip" install --quiet -r "$KRONOS_DIR/requirements.txt"
echo "  ✓ all packages installed"

# ── 7/8  Upstream Kronos model (vendor/kronos) ────────────────────────────────
# M13–M16 foundation model generators import from vendor/kronos at startup.
# Without this directory they silently disable themselves and produce no signals.
echo "─── 7/8  Upstream Kronos model"
KRONOS_UPSTREAM="https://github.com/shiyu-coder/Kronos.git"
for SUBDIR in vendor/kronos Kronos; do
    TARGET="$KRONOS_DIR/$SUBDIR"
    if [[ -d "$TARGET/.git" ]]; then
        echo "  ✓ $SUBDIR already cloned — pulling latest..."
        git -C "$TARGET" pull --ff-only || true
    else
        echo "  Cloning upstream Kronos model into $SUBDIR..."
        mkdir -p "$(dirname "$TARGET")"
        git clone "$KRONOS_UPSTREAM" "$TARGET"
    fi
done
if [[ -f "$KRONOS_DIR/vendor/kronos/requirements.txt" ]]; then
    "$VENV/bin/pip" install --quiet -r "$KRONOS_DIR/vendor/kronos/requirements.txt"
    echo "  ✓ vendor/kronos requirements installed"
fi
echo "  ✓ upstream Kronos model ready"

# ── 8/9  HuggingFace foundation model weights ─────────────────────────────────
# M13/M15 use NeoQuasar/Kronos-mini (~500 MB); M14/M16 use NeoQuasar/Kronos-base (~500 MB).
# Downloaded into .cache/huggingface/ so supervisor (running as current user) can access them.
# HF_HUB_DISABLE_IMPLICIT_TOKEN avoids PermissionError if a root-owned token file exists.
echo "─── 8/9  HuggingFace foundation model weights"
export HF_HOME="$KRONOS_DIR/.cache/huggingface"
export HF_HUB_DISABLE_IMPLICIT_TOKEN=1

for MODEL in NeoQuasar/Kronos-mini NeoQuasar/Kronos-base; do
    MODEL_SLUG="${MODEL//\//__}"
    if [[ -d "$HF_HOME/hub/models--${MODEL_SLUG}" ]]; then
        echo "  ✓ $MODEL already cached"
    else
        echo "  Downloading $MODEL from HuggingFace (~500 MB)..."
        "$VENV/bin/python" -c "
from huggingface_hub import snapshot_download
snapshot_download('$MODEL')
print('  done')
" || echo "  ⚠  Download failed — generator will retry on first run"
    fi
done
echo "  ✓ HuggingFace model weights ready"

# ── 9/9  Supervisor ───────────────────────────────────────────────────────────
echo "─── 9/9  Supervisor"
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
echo "  ✓ supervisor config installed and reloaded (9/9)"

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
