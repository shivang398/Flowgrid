#!/bin/bash
# ============================================================
#  Flowgrid — Install as System Service (run once)
#  This makes the cluster start AUTOMATICALLY on every boot
#  Usage: bash deploy/install_service.sh
# ============================================================

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${YELLOW}Installing Flowgrid as a system service...${NC}"

# Copy service files
sudo cp "$DEPLOY_DIR/flowgrid-master.service"    /etc/systemd/system/
sudo cp "$DEPLOY_DIR/flowgrid-worker@.service"   /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable services (auto-start on boot)
sudo systemctl enable flowgrid-master
sudo systemctl enable flowgrid-worker@1
sudo systemctl enable flowgrid-worker@2
sudo systemctl enable flowgrid-worker@3

# Start services NOW
sudo systemctl start flowgrid-master
sleep 4
sudo systemctl start flowgrid-worker@1
sudo systemctl start flowgrid-worker@2
sudo systemctl start flowgrid-worker@3

echo -e "${GREEN}"
echo "══════════════════════════════════════════"
echo "  ✅  Flowgrid installed as system service"
echo "══════════════════════════════════════════"
echo "  Cluster will now auto-start on every boot."
echo ""
echo "  Useful commands:"
echo "  sudo systemctl status flowgrid-master"
echo "  sudo systemctl status flowgrid-worker@1"
echo "  sudo systemctl restart flowgrid-master"
echo "  sudo systemctl stop flowgrid-master"
echo "  sudo journalctl -u flowgrid-master -f"
echo ""
echo "  Dashboard → http://localhost:8080"
echo -e "══════════════════════════════════════════${NC}"
