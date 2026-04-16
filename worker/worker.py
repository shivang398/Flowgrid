import threading
import time
import signal
from typing import Optional

from common import Task, get_logger, current_timestamp
from .executor.executor import TaskExecutor
from .communication.client import WorkerCommunicationClient

logger = get_logger("worker.core")

class WorkerNode:
    """
    The primary worker entity that coordinates execution and communication.
    """
    def __init__(
        self, 
        worker_id: str, 
        master_host: str, 
        master_port: int, 
        heartbeat_interval: float = 3.0
    ):
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval
        
        self.executor = TaskExecutor()
        self.comm_client = WorkerCommunicationClient(master_host, master_port)
        
        self._running = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._current_load = 0
        self._load_lock = threading.Lock()
        
        # Register standard compute functions
        self.executor.register("add", lambda x, y: x + y)
        self.executor.register("multiply", lambda x, y: x * y)
        self.executor.register("sleep", time.sleep)
        
        # Register industry-grade compute functions
        import math, hashlib, json, random
        
        def matrix_multiply(n):
            """CPU-bound: NxN matrix multiply — used in ML/linear algebra"""
            A = [[random.random() for _ in range(n)] for _ in range(n)]
            B = [[random.random() for _ in range(n)] for _ in range(n)]
            C = [[sum(A[i][k]*B[k][j] for k in range(n)) for j in range(n)] for i in range(n)]
            return C[0][0]
        
        def monte_carlo_pi(samples):
            """CPU-bound: Monte Carlo Pi estimation — financial risk modeling"""
            inside = sum(
                1 for _ in range(samples)
                if random.random()**2 + random.random()**2 <= 1.0
            )
            return 4 * inside / samples
        
        def hash_crunch(data, rounds):
            """CPU-bound: Hash chain — simulates cryptographic/security workloads"""
            h = data.encode() if isinstance(data, str) else str(data).encode()
            for _ in range(rounds):
                h = hashlib.sha256(h).digest()
            return h.hex()
        
        def etl_transform(records, multiplier):
            """Data pipeline: filter → transform → aggregate — ETL workload"""
            filtered = [r for r in range(records) if r % 2 == 0]
            transformed = [r * multiplier for r in filtered]
            return {"count": len(transformed), "sum": sum(transformed), "avg": sum(transformed) / max(len(transformed), 1)}
        
        def sort_benchmark(n):
            """Memory + CPU: Sort large list — simulates data warehousing"""
            data = [random.randint(0, 1_000_000) for _ in range(n)]
            return sorted(data)[-1]
        
        def hyperparameter_search(lr, reg):
            """Simulates ML hyperparameter evaluation"""
            score = math.exp(-lr) * math.log1p(reg) + random.gauss(0, 0.01)
            return {"lr": lr, "reg": reg, "score": round(score, 6)}
        
        def io_simulation(payload_kb, latency_ms):
            """Simulates I/O-bound network/disk work (used in web scrapers, DB ops)"""
            time.sleep(latency_ms / 1000.0)
            data = "x" * (payload_kb * 1024)
            return {"bytes_processed": len(data), "latency_ms": latency_ms}
        
        self.executor.register("matrix_multiply", matrix_multiply)
        self.executor.register("monte_carlo_pi", monte_carlo_pi)
        self.executor.register("hash_crunch", hash_crunch)
        self.executor.register("etl_transform", etl_transform)
        self.executor.register("sort_benchmark", sort_benchmark)
        self.executor.register("hyperparameter_search", hyperparameter_search)
        self.executor.register("io_simulation", io_simulation)

    def start(self):
        """Starts the worker operations."""
        if self._running:
            return
            
        logger.info(f"Starting Worker {self.worker_id}...")
        self._running = True
        
        # 1. Register with Master
        self.comm_client.register_with_master(self.worker_id)
        
        # 2. Start heartbeat thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, 
            daemon=True,
            name="Heartbeat-Thread"
        )
        self._heartbeat_thread.start()
        
        # 3. Enter main task loop (usually running in current thread)
        try:
            self._main_loop()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Gracefully shuts down the worker."""
        if not self._running:
            return
        logger.info(f"Shutting down Worker {self.worker_id}...")
        self._running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2.0)
        logger.info("Worker shutdown complete.")

    def _heartbeat_loop(self):
        """Background thread to keep Master updated on health and load."""
        while self._running:
            try:
                with self._load_lock:
                    load = self._current_load
                self.comm_client.send_heartbeat(self.worker_id, load)
            except Exception as e:
                logger.error(f"Heartbeat failure: {str(e)}")
            time.sleep(self.heartbeat_interval)

    def _main_loop(self):
        """Sequential polling for tasks (simplest implementation)."""
        logger.info("Worker entering main task loop.")
        while self._running:
            try:
                # Poll Master for a new task (blocks on recv)
                task = self.comm_client.poll_task(self.worker_id)
                
                if task:
                    self._process_single_task(task)
            except Exception as e:
                logger.error(f"Error in task loop: {e}")
                time.sleep(1.0)

    def _process_single_task(self, task: Task):
        """Executes one task and reports the result."""
        with self._load_lock:
            self._current_load += 1
            
        try:
            result = self.executor.execute(task)
            self.comm_client.send_result(task.id, result, is_error=False)
        except Exception as e:
            logger.error(f"Task {task.id} failed: {str(e)}")
            self.comm_client.send_result(task.id, str(e), is_error=True)
        finally:
            with self._load_lock:
                self._current_load = max(0, self._current_load - 1)

if __name__ == "__main__":
    import argparse
    import uuid
    
    parser = argparse.ArgumentParser(description="Flowgrid Worker Node")
    parser.add_argument("--id", default=str(uuid.uuid4())[:8], help="Unique worker ID")
    parser.add_argument("--master-host", default="localhost", help="Host of the Master node")
    parser.add_argument("--master-port", type=int, default=9999, help="Port of the Master node")
    
    args = parser.parse_args()
    
    worker = WorkerNode(
        worker_id=args.id,
        master_host=args.master_host,
        master_port=args.master_port
    )
    
    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()
