import signal
import sys
import argparse
import time
import threading
from typing import Optional

from common import get_logger
from master.queue import TaskQueue
from master.worker_manager import WorkerManager
from master.result_manager import ResultManager
from master.scheduler.core import Scheduler
from master.fault_tolerance.manager import FaultToleranceManager
from master.autoscaler.manager import Autoscaler
from master.auth.manager import AuthManager
from master.network.server import MasterServer
from master.metrics import MasterMetrics
from master.http_gateway import HttpGateway

logger = get_logger("master.main")

class Master:
    """
    Central Controller for the Flowgrid Master node.
    Orchestrates the lifecycle of all internal subsystems.
    """
    def __init__(
        self, 
        host: str = "0.0.0.0",
        port: int = 9999,
        metrics_port: int = 8000,
        dashboard_port: int = 8080,
        stale_timeout: float = 10.0,
        timeout_threshold: float = 60.0,
        check_interval: float = 5.0
    ):
        self.host = host
        self.port = port
        self.metrics_port = metrics_port
        self.dashboard_port = dashboard_port
        
        # 1. Initialize Thread-Safe Data Stores
        self.task_queue = TaskQueue()
        self.worker_manager = WorkerManager(stale_timeout=stale_timeout)
        self.result_manager = ResultManager(task_queue=self.task_queue)
        self.auth_manager = AuthManager()
        
        # 2. Initialize Logic Engines
        self.scheduler = Scheduler(
            task_queue=self.task_queue,
            worker_manager=self.worker_manager
        )
        
        self.fault_tolerance = FaultToleranceManager(
            task_queue=self.task_queue,
            result_manager=self.result_manager,
            worker_manager=self.worker_manager,
            timeout_threshold=timeout_threshold,
            check_interval=check_interval
        )
        
        self.autoscaler = Autoscaler(
            task_queue=self.task_queue,
            worker_manager=self.worker_manager
        )
        
        # 3. Initialize Networking Interface
        self.server = MasterServer(
            worker_manager=self.worker_manager,
            result_manager=self.result_manager,
            task_queue=self.task_queue,
            auth_manager=self.auth_manager,
            host=self.host,
            port=self.port
        )
        
        # 4. Initialize HTTP Dashboard Bridge
        self.http_gateway = HttpGateway(self, port=self.dashboard_port)
        
        self._running = False

    def start(self):
        """Sequentially launches all Master subsystems."""
        if self._running:
            return
        
        logger.info("--- Starting Flowgrid Master ---")
        self._running = True
        
        # Logic engines before networking
        self.scheduler.start()
        self.fault_tolerance.start()
        self.autoscaler.start()
        
        # Start metrics server
        MasterMetrics.start_server(self.metrics_port)
        
        # Start background stats updater
        self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True, name="Stats-Thread")
        self._stats_thread.start()
        
        # Start Dashboard Gateway
        self.http_gateway.start()
        
        self.server.start()
        
        logger.info(f"Master fully initialized and listening on {self.host}:{self.port}")

    def _stats_loop(self):
        """Periodic background loop to update global Prometheus gauges."""
        while self._running:
            try:
                # Update Gauges
                MasterMetrics.update_queue_size(self.task_queue.get_queue_size())
                
                # Active workers count
                workers = self.worker_manager.get_available_workers()
                MasterMetrics.update_worker_count(len(workers))
                
                # Sleep between updates to avoid overhead
                time.sleep(2.0)
            except Exception as e:
                logger.error(f"Error in stats loop: {e}")

    def stop(self):
        """Performs a clean shutdown of all background threads and sockets."""
        if not self._running:
            return
            
        logger.info("--- Initiating Master Shutdown ---")
        self._running = False
        
        # Order matters: stop traffic before stopping logic
        self.server.stop()
        self.http_gateway.stop()
        self.autoscaler.stop()
        self.fault_tolerance.stop()
        self.scheduler.stop()
        
        logger.info("Master shutdown complete.")

def handle_sigterm(master: Master):
    """Signal handler wrapper for clean exits."""
    def signal_handler(sig, frame):
        logger.warning(f"Received signal {sig}. Shutting down...")
        master.stop()
        sys.exit(0)
    return signal_handler

def main():
    """CLI entry point for the Master node."""
    parser = argparse.ArgumentParser(description="Flowgrid Master Node")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind for incoming connections")
    parser.add_argument("--port", type=int, default=9999, help="Port to bind for incoming connections")
    parser.add_argument("--metrics-port", type=int, default=8000, help="Port for Prometheus telemetry")
    parser.add_argument("--dashboard-port", type=int, default=8080, help="Port for the real-time visualization")
    parser.add_argument("--stale-timeout", type=float, default=10.0, help="Seconds before a worker is marked OFFLINE")
    parser.add_argument("--task-timeout", type=float, default=60.0, help="Seconds before a running task is retried")
    
    args = parser.parse_args()
    
    master = Master(
        host=args.host,
        port=args.port,
        metrics_port=args.metrics_port,
        dashboard_port=args.dashboard_port,
        stale_timeout=args.stale_timeout,
        timeout_threshold=args.task_timeout
    )
    
    # Setup signal handlers for graceful exit
    signal.signal(signal.SIGINT, handle_sigterm(master))
    signal.signal(signal.SIGTERM, handle_sigterm(master))
    
    try:
        master.start()
        # Keep the main process alive until interrupted
        while True:
            time.sleep(1.0)
    except Exception as e:
        logger.critical(f"Catastrophic failure in Master: {e}")
        master.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
