#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Kronos Trading System — Linux VPS deployment script
# Run as root or a user with sudo access.
# Usage: bash deploy.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_URL="https://github.com/SmeshBash/kronos-signals.git"
KRONOS_UPSTREAM="https://github.com/shiyu-coder/Kronos.git"
APP_DIR="/app/kronos"
VENV_DIR="/app/venv"
LOG_DIR="/var/log/kronos"
KRONOS_USER="kronos"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── 0. Must be run as root or sudo ───────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
  exec sudo bash "$0" "$@"
fi

info "Starting Kronos deployment on $(hostname) — $(date)"

# ── 1. System packages ────────────────────────────────────────────────────────
info "Installing system packages..."
apt-get update -qq
apt-get install -y -qq \
    python3 python3-venv python3-dev \
    git supervisor build-essential curl

info "Python version: $(python3 --version)"

# ── 2. Create kronos system user ──────────────────────────────────────────────
if ! id "$KRONOS_USER" &>/dev/null; then
    info "Creating user '$KRONOS_USER'..."
    useradd -r -m -d "$APP_DIR" -s /bin/bash "$KRONOS_USER"
else
    info "User '$KRONOS_USER' already exists."
fi

# ── 3. Clone / update main repo ───────────────────────────────────────────────
mkdir -p /app
if [[ -d "$APP_DIR/.git" ]]; then
    info "Repo already cloned — pulling latest..."
    sudo -u "$KRONOS_USER" git -C "$APP_DIR" pull --ff-only
else
    info "Cloning Kronos signals repo..."
    # Dir may exist (created by useradd as home dir) but not be a git repo — wipe and reclone
    rm -rf "$APP_DIR"
    git clone "$REPO_URL" "$APP_DIR"
    chown -R "$KRONOS_USER:$KRONOS_USER" "$APP_DIR"
fi

# ── 4. Clone upstream Kronos model repo (vendor + Kronos dirs) ────────────────
for SUBDIR in vendor/kronos Kronos; do
    TARGET="$APP_DIR/$SUBDIR"
    if [[ -d "$TARGET/.git" ]]; then
        info "$SUBDIR already cloned — pulling latest..."
        sudo -u "$KRONOS_USER" git -C "$TARGET" pull --ff-only || true
    else
        info "Cloning upstream Kronos model into $SUBDIR..."
        mkdir -p "$(dirname "$TARGET")"
        git clone "$KRONOS_UPSTREAM" "$TARGET"
        chown -R "$KRONOS_USER:$KRONOS_USER" "$TARGET"
    fi
done

# ── 5. Python virtual environment ────────────────────────────────────────────
if [[ ! -d "$VENV_DIR" ]]; then
    info "Creating Python virtualenv at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    chown -R "$KRONOS_USER:$KRONOS_USER" "$VENV_DIR"
else
    info "Virtualenv already exists."
fi

# ── 6. Install Python dependencies ───────────────────────────────────────────
info "Installing PyTorch (CPU build)..."
sudo -u "$KRONOS_USER" "$VENV_DIR/bin/pip" install --quiet --upgrade pip
sudo -u "$KRONOS_USER" "$VENV_DIR/bin/pip" install --quiet \
    torch torchvision \
    --index-url https://download.pytorch.org/whl/cpu

info "Installing Kronos requirements..."
sudo -u "$KRONOS_USER" "$VENV_DIR/bin/pip" install --quiet \
    -r "$APP_DIR/requirements.txt"

if [[ -f "$APP_DIR/vendor/kronos/requirements.txt" ]]; then
    info "Installing vendor/kronos requirements..."
    sudo -u "$KRONOS_USER" "$VENV_DIR/bin/pip" install --quiet \
        -r "$APP_DIR/vendor/kronos/requirements.txt"
fi

# ── 7. Create required directories ───────────────────────────────────────────
info "Creating directories..."
mkdir -p "$LOG_DIR"
chown "$KRONOS_USER:$KRONOS_USER" "$LOG_DIR"

sudo -u "$KRONOS_USER" mkdir -p \
    "$APP_DIR/data/reports" \
    "$APP_DIR/models"

# ── 8. Create .env from template if not present ───────────────────────────────
if [[ ! -f "$APP_DIR/.env" ]]; then
    info "Creating .env from .linuxenv template..."
    cp "$APP_DIR/.linuxenv" "$APP_DIR/.env"
    chown "$KRONOS_USER:$KRONOS_USER" "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    warn "IMPORTANT: Edit $APP_DIR/.env and fill in your secrets before starting!"
    warn "  Required: KRONOS_API_KEY, KRONOS_API_SECRET"
    warn "  Optional: KRONOS_TELEGRAM_BOT_TOKEN, KRONOS_TELEGRAM_CHAT_ID"
else
    info ".env already exists — skipping."
fi

# ── 9. Copy .env to system EnvironmentFile ────────────────────────────────────
info "Installing /etc/kronos.env..."
cp "$APP_DIR/.env" /etc/kronos.env
chmod 600 /etc/kronos.env
chown root:root /etc/kronos.env

# ── 10. Create systemd unit ───────────────────────────────────────────────────
info "Writing systemd unit /etc/systemd/system/kronos-supervisor.service..."
cat > /etc/systemd/system/kronos-supervisor.service <<'EOF'
[Unit]
Description=Kronos Trading System (Supervisord)
After=network.target

[Service]
Type=forking
User=root
EnvironmentFile=/etc/kronos.env
ExecStart=/usr/bin/supervisord -c /app/kronos/supervisord.conf
ExecStop=/usr/bin/supervisorctl -c /app/kronos/supervisord.conf shutdown
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# ── 11. Initialize database ───────────────────────────────────────────────────
if [[ ! -f "$APP_DIR/data/kronos.db" ]]; then
    info "Initialising database..."
    cd "$APP_DIR"
    sudo -u "$KRONOS_USER" \
        KRONOS_DB_PATH="$APP_DIR/data/kronos.db" \
        "$VENV_DIR/bin/python" -c "from db import init_db; init_db()"
else
    info "Database already exists — skipping init."
fi

# ── 12. UFW firewall ──────────────────────────────────────────────────────────
if command -v ufw &>/dev/null; then
    info "Configuring UFW firewall..."
    ufw allow 22/tcp   comment 'SSH'   2>/dev/null || true
    ufw allow 8050/tcp comment 'Kronos dashboard' 2>/dev/null || true
    ufw --force enable 2>/dev/null || true
fi

# ── 13. Enable & reload systemd ───────────────────────────────────────────────
info "Enabling kronos-supervisor service..."
systemctl daemon-reload
systemctl enable kronos-supervisor

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Kronos deployment complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""

if [[ ! -f "$APP_DIR/models/kronos_model.pt" ]]; then
    warn "Model weights NOT found at $APP_DIR/models/kronos_model.pt"
    warn "Transfer from Windows before starting:"
    warn "  scp D:/projects/kronos/models/kronos_model.pt $(whoami)@$(hostname -I | awk '{print $1}'):$APP_DIR/models/"
    warn "  sudo chown kronos:kronos $APP_DIR/models/kronos_model.pt"
    echo ""
fi

echo "  Next steps:"
echo "  1. Fill in secrets:  nano $APP_DIR/.env  &&  cp $APP_DIR/.env /etc/kronos.env"
echo "  2. Transfer model weights (see warning above if shown)"
echo "  3. Start Kronos:     sudo systemctl start kronos-supervisor"
echo "  4. Check status:     sudo supervisorctl -c $APP_DIR/supervisord.conf status"
echo "  5. Dashboard:        http://$(hostname -I | awk '{print $1}'):8050"
echo ""
