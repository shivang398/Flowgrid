#!/bin/bash
# ============================================================
#  Flowgrid — One-Shot Cluster Launcher
#  Run this from the project root:  bash start_cluster.sh
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

MASTER_HOST="127.0.0.1"
MASTER_PORT=9999
METRICS_PORT=8000
DASHBOARD_PORT=8080
NUM_WORKERS=3

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'

echo -e "\n${CYAN}╔══════════════════════════════════════════╗"
echo -e "║      FLOWGRID — Cluster Launcher         ║"
echo -e "╚══════════════════════════════════════════╝${NC}\n"

# --- Step 1: Clean up ---
echo -e "${YELLOW}[1/4] Killing any existing cluster processes...${NC}"
pkill -f "python3 -m master.master" 2>/dev/null || true
pkill -f "python3 -m worker.worker" 2>/dev/null || true
sleep 1

# Release ports
fuser -k ${MASTER_PORT}/tcp 2>/dev/null || true
fuser -k ${METRICS_PORT}/tcp 2>/dev/null || true
fuser -k ${DASHBOARD_PORT}/tcp 2>/dev/null || true
sleep 1
echo -e "  ${GREEN}✓ Ports cleared${NC}"

# --- Step 2: Start Master ---
echo -e "\n${YELLOW}[2/4] Starting Master Node...${NC}"
nohup python3 -m master.master \
    --host 0.0.0.0 \
    --port $MASTER_PORT \
    --metrics-port $METRICS_PORT \
    --dashboard-port $DASHBOARD_PORT \
    > master.log 2>&1 &
MASTER_PID=$!
echo "$MASTER_PID" > .master.pid

# Wait for master to be ready
for i in $(seq 1 10); do
    if grep -q "Master fully initialized" master.log 2>/dev/null; then
        echo -e "  ${GREEN}✓ Master is UP (PID: $MASTER_PID) → Port $MASTER_PORT${NC}"
        break
    fi
    sleep 1
done

if ! grep -q "Master fully initialized" master.log 2>/dev/null; then
    echo -e "  ${RED}✗ Master failed to start! Check master.log${NC}"
    tail -n 10 master.log
    exit 1
fi

# --- Step 3: Start Workers ---
echo -e "\n${YELLOW}[3/4] Starting $NUM_WORKERS Workers...${NC}"
for i in $(seq 1 $NUM_WORKERS); do
    nohup python3 -m worker.worker \
        --master-host $MASTER_HOST \
        --master-port $MASTER_PORT \
        > "worker${i}.log" 2>&1 &
    echo "$!" >> .worker.pids
    sleep 0.5
done

# Wait for all workers to register
echo -n "  Waiting for workers to register"
for i in $(seq 1 15); do
    COUNT=$(grep -c "successfully registered" master.log 2>/dev/null || echo 0)
    if [ "$COUNT" -ge "$NUM_WORKERS" ]; then
        echo ""
        echo -e "  ${GREEN}✓ All $NUM_WORKERS workers registered!${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

# --- Step 4: Verify & Show Status ---
echo -e "\n${YELLOW}[4/4] Cluster Verification...${NC}"
echo ""
echo -e "  ${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
grep "successfully registered" master.log | while read line; do
    WORKER=$(echo "$line" | grep -oP 'Worker \w+')
    echo -e "  ${GREEN}✓ $WORKER ONLINE${NC}"
done
echo -e "  ${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${CYAN}Dashboard   →  http://localhost:$DASHBOARD_PORT${NC}"
echo -e "  ${CYAN}Prometheus  →  http://localhost:$METRICS_PORT${NC}"
echo -e "  ${CYAN}Master API  →  localhost:$MASTER_PORT${NC}"

# --- Optional: Auto-run benchmark ---
echo ""
echo -e "${YELLOW}Running Monte Carlo benchmark to warm up dashboard...${NC}"
python3 -m benchmarks.industry_benchmark --bench monte_carlo &

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗"
echo -e "║  ✅  Cluster is LIVE!                    ║"
echo -e "║  Open http://localhost:8080 NOW          ║"
echo -e "╚══════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}To stop the cluster: bash stop_cluster.sh${NC}"

# Keep script alive so workers don't die
wait $MASTER_PID
