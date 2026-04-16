#!/bin/bash
# ============================================================
#  Flowgrid — Cluster Shutdown
#  Run: bash stop_cluster.sh
# ============================================================

echo "Stopping Flowgrid cluster..."
pkill -f "python3 -m master.master" 2>/dev/null && echo "  ✓ Master stopped" || echo "  · Master not running"
pkill -f "python3 -m worker.worker" 2>/dev/null && echo "  ✓ Workers stopped" || echo "  · Workers not running"
pkill -f "industry_benchmark"        2>/dev/null || true
rm -f .master.pid .worker.pids
echo "Cluster stopped."
