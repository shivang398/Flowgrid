"""
Flowgrid — Industry-Grade Benchmark Suite
==========================================
Simulates 5 real-world distributed computing scenarios:

  1. Financial Risk (Monte Carlo)
  2. ML Hyperparameter Search (Grid Search)
  3. Data Pipeline (ETL Transform)
  4. Security/Crypto (Hash Crunch)
  5. Mixed I/O + CPU (Chaos Load)

Run:
    python3 -m benchmarks.industry_benchmark
"""

import sys, time, statistics, argparse
sys.path.insert(0, '.')
from client.flowgrid_client import FlowgridClient

MASTER_HOST = "localhost"
MASTER_PORT = 9999

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def submit_batch(client, tasks):
    """Submit a list of (fn, *args) tuples and return task IDs."""
    ids = []
    for fn, *args in tasks:
        tid = client.submit_task(fn, *args)
        ids.append(tid)
    return ids

def wait_for_results(client, task_ids, timeout=120):
    """Poll until all tasks complete or timeout."""
    results = {}
    deadline = time.time() + timeout
    pending = list(task_ids)
    while pending and time.time() < deadline:
        still_pending = []
        for tid in pending:
            r = client.get_result(tid)
            if r is not None:
                results[tid] = r
            else:
                still_pending.append(tid)
        pending = still_pending
        if pending:
            time.sleep(0.2)
    return results, pending  # pending = timed-out tasks

def print_header(title):
    width = 60
    print("\n" + "═" * width)
    print(f"  🏭  {title}")
    print("═" * width)

def print_result(label, value, unit=""):
    print(f"  {'·':1} {label:<35} {value} {unit}")

# ─────────────────────────────────────────────
# Benchmark 1: Monte Carlo Financial Risk
# ─────────────────────────────────────────────

def bench_monte_carlo(client, n_tasks=30):
    print_header("BENCHMARK 1 — Monte Carlo Financial Risk")
    print(f"  Simulating {n_tasks} parallel risk trials (100k samples each)...")

    tasks = [("monte_carlo_pi", 100_000) for _ in range(n_tasks)]
    t0 = time.time()
    ids = submit_batch(client, tasks)
    results, timed_out = wait_for_results(client, ids)
    elapsed = time.time() - t0

    values = [v for v in results.values() if isinstance(v, float)]
    avg_pi = statistics.mean(values) if values else 0
    stdev  = statistics.stdev(values) if len(values) > 1 else 0

    print_result("Tasks submitted", n_tasks)
    print_result("Tasks completed", len(results))
    print_result("Timed out", len(timed_out))
    print_result("Total wall time", f"{elapsed:.2f}", "s")
    print_result("Throughput", f"{len(results)/elapsed:.1f}", "tasks/s")
    print_result("Avg π estimate", f"{avg_pi:.5f}")
    print_result("Std deviation", f"{stdev:.5f}")
    print_result("Error from π", f"{abs(avg_pi - 3.14159265):.5f}")

# ─────────────────────────────────────────────
# Benchmark 2: ML Hyperparameter Grid Search
# ─────────────────────────────────────────────

def bench_hyperparameter_search(client):
    print_header("BENCHMARK 2 — ML Hyperparameter Grid Search")
    lrs  = [0.001, 0.01, 0.1, 0.5, 1.0]
    regs = [0.0001, 0.001, 0.01, 0.1, 1.0]
    combos = [(lr, reg) for lr in lrs for reg in regs]
    n_tasks = len(combos)
    print(f"  Grid search over {n_tasks} (lr × reg) combinations...")

    tasks = [("hyperparameter_search", lr, reg) for lr, reg in combos]
    t0 = time.time()
    ids = submit_batch(client, tasks)
    results, timed_out = wait_for_results(client, ids)
    elapsed = time.time() - t0

    scores = [v["score"] for v in results.values() if isinstance(v, dict) and "score" in v]
    best   = max(scores) if scores else 0
    worst  = min(scores) if scores else 0

    print_result("Configs evaluated", n_tasks)
    print_result("Completed", len(results))
    print_result("Wall time", f"{elapsed:.2f}", "s")
    print_result("Throughput", f"{len(results)/elapsed:.1f}", "tasks/s")
    print_result("Best score", f"{best:.6f}")
    print_result("Worst score", f"{worst:.6f}")

# ─────────────────────────────────────────────
# Benchmark 3: ETL Data Pipeline
# ─────────────────────────────────────────────

def bench_etl_pipeline(client, n_tasks=40):
    print_header("BENCHMARK 3 — ETL Data Pipeline")
    print(f"  Running {n_tasks} parallel ETL transforms (10k records each)...")

    tasks = [("etl_transform", 10_000, i+1) for i in range(n_tasks)]
    t0 = time.time()
    ids = submit_batch(client, tasks)
    results, timed_out = wait_for_results(client, ids)
    elapsed = time.time() - t0

    total_records = sum(v["count"] for v in results.values() if isinstance(v, dict) and "count" in v)

    print_result("Batches dispatched", n_tasks)
    print_result("Batches completed", len(results))
    print_result("Total records processed", f"{total_records:,}")
    print_result("Wall time", f"{elapsed:.2f}", "s")
    print_result("Record throughput", f"{total_records/elapsed:,.0f}", "records/s")
    print_result("Task throughput", f"{len(results)/elapsed:.1f}", "tasks/s")

# ─────────────────────────────────────────────
# Benchmark 4: Cryptographic Hash Crunch
# ─────────────────────────────────────────────

def bench_hash_crunch(client, n_tasks=20):
    print_header("BENCHMARK 4 — Security / Cryptographic Hash Chain")
    print(f"  Running {n_tasks} SHA-256 chains (5000 rounds each)...")

    tasks = [("hash_crunch", f"flowgrid_payload_{i}", 5000) for i in range(n_tasks)]
    t0 = time.time()
    ids = submit_batch(client, tasks)
    results, timed_out = wait_for_results(client, ids, timeout=180)
    elapsed = time.time() - t0

    total_hashes = len(results) * 5000
    print_result("Tasks submitted", n_tasks)
    print_result("Completed", len(results))
    print_result("Wall time", f"{elapsed:.2f}", "s")
    print_result("Throughput", f"{len(results)/elapsed:.1f}", "tasks/s")
    print_result("Total SHA-256 ops", f"{total_hashes:,}")
    print_result("Hash ops/sec", f"{total_hashes/elapsed:,.0f}", "hashes/s")

# ─────────────────────────────────────────────
# Benchmark 5: Mixed Chaos Load (I/O + CPU)
# ─────────────────────────────────────────────

def bench_chaos_load(client, n_tasks=60):
    print_header("BENCHMARK 5 — Mixed Chaos Load (I/O + CPU)")
    import random
    print(f"  Firing {n_tasks} mixed tasks (sort, io_sim, monte_carlo)...")

    task_pool = [
        ("sort_benchmark",   50_000),
        ("io_simulation",    16, 200),
        ("monte_carlo_pi",   50_000),
        ("etl_transform",    5_000, 3),
        ("hash_crunch",      "chaos", 1000),
    ]
    tasks = [random.choice(task_pool) for _ in range(n_tasks)]
    t0 = time.time()
    ids = submit_batch(client, tasks)
    results, timed_out = wait_for_results(client, ids, timeout=180)
    elapsed = time.time() - t0

    print_result("Tasks submitted", n_tasks)
    print_result("Completed", len(results))
    print_result("Timed out / failed", len(timed_out))
    print_result("Success rate", f"{100*len(results)/n_tasks:.1f}", "%")
    print_result("Wall time", f"{elapsed:.2f}", "s")
    print_result("Throughput", f"{len(results)/elapsed:.1f}", "tasks/s")

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Flowgrid Industry Benchmark Suite")
    parser.add_argument("--host", default=MASTER_HOST)
    parser.add_argument("--port", type=int, default=MASTER_PORT)
    parser.add_argument("--bench", default="all",
        help="Run: all | monte_carlo | hyperparam | etl | hash | chaos")
    args = parser.parse_args()

    print("\n" + "█" * 60)
    print("  FLOWGRID — INDUSTRY BENCHMARK SUITE")
    print("  Connecting to cluster at {}:{}".format(args.host, args.port))
    
    import psutil, platform
    print(f"  Local HW: {platform.processor()} | {psutil.cpu_count()} Cores | {round(psutil.virtual_memory().total / (1024**3), 1)}GB RAM")
    print("█" * 60)

    client = FlowgridClient(args.host, args.port)
    client.connect()
    
    # RBAC: Authenticate before running benchmarks
    try:
        client.authenticate("flowgrid_admin_123")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        sys.exit(1)

    try:
        if args.bench in ("all", "monte_carlo"):
            bench_monte_carlo(client)
        if args.bench in ("all", "hyperparam"):
            bench_hyperparameter_search(client)
        if args.bench in ("all", "etl"):
            bench_etl_pipeline(client)
        if args.bench in ("all", "hash"):
            bench_hash_crunch(client)
        if args.bench in ("all", "chaos"):
            bench_chaos_load(client)
    finally:
        client.disconnect()

    print("\n" + "═" * 60)
    print("  ✅  All benchmarks complete.")
    print("═" * 60 + "\n")

if __name__ == "__main__":
    main()
