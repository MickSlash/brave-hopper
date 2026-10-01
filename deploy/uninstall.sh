#!/usr/bin/env bash
# ==============================================================================
# Stream CDN Uninstallation Script
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}[ERROR]${NC} This script must be run as root."
    exit 1
fi

echo -e "${BLUE}[INFO]${NC} Stopping and disabling Stream CDN services..."
systemctl stop stream-edge stream-control stream-origin 2>/dev/null || true
systemctl disable stream-edge stream-control stream-origin 2>/dev/null || true

echo -e "${BLUE}[INFO]${NC} Removing systemd units..."
rm -f /etc/systemd/system/stream-control.service
rm -f /etc/systemd/system/stream-edge.service
rm -f /etc/systemd/system/stream-origin.service
systemctl daemon-reload

echo -e "${BLUE}[INFO]${NC} Removing binaries..."
rm -f /usr/local/bin/stream-control
rm -f /usr/local/bin/stream-edge
rm -f /usr/local/bin/stream-origin
rm -f /usr/local/bin/stream-bench

echo -e "${BLUE}[INFO]${NC} Removing logrotate configuration..."
rm -f /etc/logrotate.d/stream-cdn

echo -e "${GREEN}[SUCCESS]${NC} Stream CDN binaries and services uninstalled."
echo "Note: /var/cache/stream-edge and /etc/stream-cdn were preserved."
echo "To completely purge data, run: rm -rf /var/cache/stream-edge /etc/stream-cdn /var/log/stream-cdn"
