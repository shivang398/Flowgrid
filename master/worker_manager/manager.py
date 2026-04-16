import threading
from typing import Dict, List, Optional, Any

from common import (
    Worker, WorkerStatus, Task, Message, MessageType, 
    current_timestamp, get_logger
)
from common.network.connection import TcpConnection
from .interface import WorkerManagerInterface
from .exceptions import WorkerNotFoundError, WorkerAlreadyExistsError

logger = get_logger("master.worker_manager")

class WorkerManager(WorkerManagerInterface):
    """
    Thread-safe authoritative source of distributed worker states.
    Fulfills the WorkerManagerInterface required by the master Scheduler.
    """
    
    def __init__(self, stale_timeout: float = 10.0):
        self._stale_timeout = stale_timeout
        self._lock = threading.RLock()
        self._workers: Dict[str, Worker] = {}
        self._connections: Dict[str, TcpConnection] = {}

    def register_worker(self, worker_id: str) -> Worker:
        """Adds a new worker to the tracking system and turns its state to IDLE."""
        with self._lock:
            if worker_id in self._workers:
                worker = self._workers[worker_id]
                if worker.status == WorkerStatus.OFFLINE:
                     # A re-connection of an offline worker
                     worker.update_heartbeat(load=0)
                     logger.info(f"Re-registered previously offline worker: {worker_id}")
                     return worker
                else:
                     logger.warning(f"Registration collision for active worker: {worker_id}")
                     raise WorkerAlreadyExistsError(f"Worker {worker_id} is already registered and active.")
            
            worker = Worker(worker_id=worker_id)
            self._workers[worker_id] = worker
            logger.info(f"Registered new worker: {worker_id}")
            return worker

    def register_connection(self, worker_id: str, connection: TcpConnection):
        """Associates an active TCP connection with a registered worker."""
        with self._lock:
            if worker_id not in self._workers:
                raise WorkerNotFoundError(f"Worker {worker_id} must be registered before adding connection.")
            self._connections[worker_id] = connection
            logger.info(f"Active connection established for worker: {worker_id}")

    def remove_worker(self, worker_id: str) -> None:
        """Completely deregisters and drops a worker from tracking."""
        with self._lock:
            if worker_id in self._workers:
                del self._workers[worker_id]
                logger.info(f"Removed worker completely: {worker_id}")

    def update_worker_status(self, worker_id: str, load: int) -> Worker:
        """
        Processes standard remote heartbeats.
        Updates internal load state and refreshes the timestamp natively.
        """
        with self._lock:
            if worker_id not in self._workers:
                raise WorkerNotFoundError(f"Worker {worker_id} not found in tracking map.")
                
            worker = self._workers[worker_id]
            worker.update_heartbeat(load=load)
            return worker

    def _filter_stale_workers(self) -> None:
        """
        Internal mechanism to scrub stale workers implicitly.
        Assumes lock is held by the caller.
        """
        now = current_timestamp()
        for worker_id, worker in self._workers.items():
            if worker.status != WorkerStatus.OFFLINE:
                if (now - worker.last_heartbeat) > self._stale_timeout:
                    worker.mark_offline()
                    logger.warning(f"Worker {worker_id} marked OFFLINE due to heartbeat timeout.")

    def get_available_workers(self) -> List[Worker]:
        """
        Returns all active workers. Auto-scrubs stale ones implicitly.
        """
        with self._lock:
            self._filter_stale_workers()
            return [w for w in self._workers.values() if w.status != WorkerStatus.OFFLINE]

    def get_least_loaded_worker(self) -> Optional[Worker]:
        """
        Quick utility abstraction strictly isolating the absolute most available node natively.
        """
        with self._lock:
            available = self.get_available_workers()
            if not available:
                return None
            return min(available, key=lambda w: w.load)

    def assign_task(self, worker_id: str, task: Task) -> bool:
        """
        Interface method designed to dispatch a task cleanly to the remote worker context.
        This provides a stub execution layer. Eventually, this will interface with the gRPC/Networking components.
        """
        with self._lock:
            if worker_id not in self._workers:
                 return False
                 
            worker = self._workers[worker_id]
            if worker.status == WorkerStatus.OFFLINE:
                 return False
                 
            logger.info(f"Transmitting task {task.id} over the wire to worker {worker_id}.")
            
            conn = self._connections.get(worker_id)
            if not conn:
                logger.error(f"No active connection found for worker {worker_id}.")
                return False
                
            try:
                # Dispatch TASK message
                msg = Message(type=MessageType.TASK, task_id=task.id, payload=task)
                conn.send_message(msg)
                return True
            except Exception as e:
                logger.error(f"Failed to transmit task {task.id} to {worker_id}: {e}")
                return False
