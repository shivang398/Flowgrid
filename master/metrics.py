from prometheus_client import Gauge, Counter, Histogram, start_http_server
from common import get_logger

logger = get_logger("master.metrics")

# --- Metrics Definitions ---

# Cluster Health
QUEUE_SIZE = Gauge("flowgrid_queue_size", "Current number of tasks in the pending queue")
WORKERS_ACTIVE = Gauge("flowgrid_workers_active", "Number of currently registered and alive workers")

# Task Throughput
TASKS_SUBMITTED = Counter("flowgrid_tasks_submitted_total", "Total number of tasks received from clients")
TASKS_PROCESSED = Counter("flowgrid_tasks_processed_total", "Tasks completed or failed", ["status"])

# Performance
TASK_LATENCY = Histogram(
    "flowgrid_task_latency_seconds", 
    "End-to-end task execution latency",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, float("inf")]
)

class MasterMetrics:
    """
    High-level wrapper for Prometheus metrics collection.
    """
    @staticmethod
    def start_server(port: int = 8000):
        """Starts the Prometheus scrape endpoint."""
        try:
            start_http_server(port)
            logger.info(f"Prometheus metrics server started on port {port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")

    @staticmethod
    def update_queue_size(size: int):
        QUEUE_SIZE.set(size)

    @staticmethod
    def update_worker_count(count: int):
        WORKERS_ACTIVE.set(count)

    @staticmethod
    def task_submitted():
        TASKS_SUBMITTED.inc()

    @staticmethod
    def task_completed(status: str, latency: float = None):
        TASKS_PROCESSED.labels(status=status).inc()
        if latency is not None:
            TASK_LATENCY.observe(latency)
