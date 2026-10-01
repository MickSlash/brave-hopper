#!/usr/bin/env bash
# ==============================================================================
# Stream CDN Production Installation & Setup Script
# Supports: Ubuntu 20.04+, Debian 11+, RHEL/Rocky 8+, AlmaLinux, Arch Linux
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

if [ "$EUID" -ne 0 ]; then
    log_error "This script must be run as root (use sudo)."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${1:-$SCRIPT_DIR/../target/release}"

INSTALL_CONTROL=false
INSTALL_EDGE=false
INSTALL_ORIGIN=false

if [ "$#" -eq 0 ]; then
    INSTALL_CONTROL=true
    INSTALL_EDGE=true
    INSTALL_ORIGIN=true
else
    for arg in "$@"; do
        case "$arg" in
            --control) INSTALL_CONTROL=true ;;
            --edge)    INSTALL_EDGE=true ;;
            --origin)  INSTALL_ORIGIN=true ;;
            --all)
                INSTALL_CONTROL=true
                INSTALL_EDGE=true
                INSTALL_ORIGIN=true
                ;;
        esac
    done
fi

log_info "Creating streamcdn system user and group..."
if ! getent group streamcdn >/dev/null 2>&1; then
    groupadd --system streamcdn
    log_info "Created group streamcdn"
fi

if ! getent passwd streamcdn >/dev/null 2>&1; then
    useradd --system \
        --gid streamcdn \
        --no-create-home \
        --shell /usr/sbin/nologin \
        --comment "Stream CDN Service User" \
        streamcdn
    log_info "Created user streamcdn"
fi

log_info "Creating required directory hierarchy..."
mkdir -p /etc/stream-cdn
mkdir -p /var/log/stream-cdn
mkdir -p /var/lib/stream-control
mkdir -p /var/cache/stream-edge

chmod 0750 /etc/stream-cdn
chown root:streamcdn /etc/stream-cdn

chmod 0750 /var/log/stream-cdn
chown streamcdn:streamcdn /var/log/stream-cdn

chmod 0750 /var/lib/stream-control
chown streamcdn:streamcdn /var/lib/stream-control

chmod 0750 /var/cache/stream-edge
chown streamcdn:streamcdn /var/cache/stream-edge

# Install binaries and units
if [ "$INSTALL_CONTROL" = true ]; then
    log_info "Installing stream-control..."
    if [ -f "$BIN_DIR/stream-control" ]; then
        install -m 0755 "$BIN_DIR/stream-control" /usr/local/bin/stream-control
    else
        log_warn "Binary $BIN_DIR/stream-control not found, skipping binary copy."
    fi

    if [ ! -f /etc/stream-cdn/control.toml ]; then
        cp "$SCRIPT_DIR/configs/control.prod.toml" /etc/stream-cdn/control.toml
        chmod 0640 /etc/stream-cdn/control.toml
        chown root:streamcdn /etc/stream-cdn/control.toml
        log_info "Created default /etc/stream-cdn/control.toml (Please customize secrets!)"
    fi

    cp "$SCRIPT_DIR/systemd/stream-control.service" /etc/systemd/system/stream-control.service
    chmod 0644 /etc/systemd/system/stream-control.service
fi

if [ "$INSTALL_EDGE" = true ]; then
    log_info "Installing stream-edge..."
    if [ -f "$BIN_DIR/stream-edge" ]; then
        install -m 0755 "$BIN_DIR/stream-edge" /usr/local/bin/stream-edge
    else
        log_warn "Binary $BIN_DIR/stream-edge not found, skipping binary copy."
    fi

    if [ ! -f /etc/stream-cdn/edge.toml ]; then
        cp "$SCRIPT_DIR/configs/edge.prod.toml" /etc/stream-cdn/edge.toml
        chmod 0640 /etc/stream-cdn/edge.toml
        chown root:streamcdn /etc/stream-cdn/edge.toml
        log_info "Created default /etc/stream-cdn/edge.toml (Please customize edge ID!)"
    fi

    cp "$SCRIPT_DIR/systemd/stream-edge.service" /etc/systemd/system/stream-edge.service
    chmod 0644 /etc/systemd/system/stream-edge.service
fi

if [ "$INSTALL_ORIGIN" = true ]; then
    log_info "Installing stream-origin..."
    if [ -f "$BIN_DIR/stream-origin" ]; then
        install -m 0755 "$BIN_DIR/stream-origin" /usr/local/bin/stream-origin
    else
        log_warn "Binary $BIN_DIR/stream-origin not found, skipping binary copy."
    fi

    if [ ! -f /etc/stream-cdn/origin.env ]; then
        cp "$SCRIPT_DIR/configs/origin.env" /etc/stream-cdn/origin.env
        chmod 0640 /etc/stream-cdn/origin.env
        chown root:streamcdn /etc/stream-cdn/origin.env
    fi

    cp "$SCRIPT_DIR/systemd/stream-origin.service" /etc/systemd/system/stream-origin.service
    chmod 0644 /etc/systemd/system/stream-origin.service
fi

# Logrotate installation
if [ -d /etc/logrotate.d ]; then
    log_info "Installing logrotate rule..."
    cp "$SCRIPT_DIR/logrotate.d/stream-cdn" /etc/logrotate.d/stream-cdn
    chmod 0644 /etc/logrotate.d/stream-cdn
fi

# Reload systemd
if command -v systemctl >/dev/null 2>&1; then
    log_info "Reloading systemd daemon..."
    systemctl daemon-reload
    log_success "Systemd units installed and daemon reloaded."
fi

log_success "Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Review and adjust secrets in /etc/stream-cdn/ (control.toml, edge.toml, origin.env)"
echo "  2. Enable and start services:"
if [ "$INSTALL_CONTROL" = true ]; then
    echo "       systemctl enable --now stream-control"
fi
if [ "$INSTALL_EDGE" = true ]; then
    echo "       systemctl enable --now stream-edge"
fi
if [ "$INSTALL_ORIGIN" = true ]; then
    echo "       systemctl enable --now stream-origin"
fi
echo "  3. Check logs:"
echo "       journalctl -u stream-edge -f"
