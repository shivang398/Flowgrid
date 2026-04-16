import threading
import time
from typing import Optional

from common import get_logger, TaskStatus, WorkerStatus, current_timestamp
from master.queue import TaskQueue
from master.worker_manager import WorkerManager
from master.result_manager import ResultManager

logger = get_logger("master.fault_tolerance")

class FaultToleranceManager:
    """
    Background service that handles task retries, timeout detection, 
    and cleanup when workers fail.
    """
    def __init__(
        self, 
        task_queue: TaskQueue, 
        result_manager: ResultManager, 
        worker_manager: WorkerManager,
        timeout_threshold: float = 60.0,
        check_interval: float = 5.0
    ):
        self.task_queue = task_queue
        self.result_manager = result_manager
        self.worker_manager = worker_manager
        self.timeout_threshold = timeout_threshold
        self.check_interval = check_interval
        
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Starts the fault tolerance monitoring loop."""
        if self._running:
            return
        logger.info("Initializing Master Fault Tolerance Manager.")
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="FaultTolerance-Thread")
        self._thread.start()

    def stop(self, timeout: float = 2.0):
        """Stops the fault tolerance monitoring loop."""
        logger.info("Stopping Master Fault Tolerance Manager.")
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _run(self):
        """Periodic background checks for timeouts and failures."""
        while self._running:
            try:
                self.detect_timeouts()
                self.check_worker_failures()
            except Exception as e:
                logger.error(f"Error in FaultTolerance loop: {str(e)}")
            
            time.sleep(self.check_interval)

    def detect_timeouts(self):
        """
        Scans all tasks currently in the registry and identifies 
        those that have exceeded the timeout threshold.
        """
        now = current_timestamp()
        # We access the internal dictionary of ResultManager (assumes we can)
        # Or better yet, we might want ResultManager to expose a way to iterate active tasks
        # For now, we'll assume we can get them.
        
        # Note: In a real system, ResultManager should provide a thread-safe iterator
        with self.result_manager._lock:
            for task_id, task in list(self.result_manager._tasks.items()):
                if task.status == TaskStatus.RUNNING:
                    if task.started_at and (now - task.started_at) > self.timeout_threshold:
                        logger.warning(f"Task {task_id} timed out after {now - task.started_at:.1f}s. Retrying...")
                        self.retry_task(task, reason="Task timed out.")

    def check_worker_failures(self):
        """
        Checks for tasks associated with workers that are now OFFLINE.
        """
        # Trigger lazy stale cleanup in WorkerManager
        self.worker_manager.get_available_workers()
        
        with self.result_manager._lock:
            for task_id, task in list(self.result_manager._tasks.items()):
                if task.status == TaskStatus.RUNNING and task.worker_id:
                    worker = self.worker_manager._workers.get(task.worker_id)
                    if worker and worker.status == WorkerStatus.OFFLINE:
                        logger.warning(f"Task {task_id} was on worker {task.worker_id} which is now OFFLINE. Retrying...")
                        self.retry_task(task, reason=f"Worker {task.worker_id} failed.")

    def retry_task(self, task, reason: str):
        """
        Increments the retry counter and pushes the task back to the queue if limits allow.
        """
        if task.mark_failed(reason):
            # Succesfully incremented retries and state is PENDING
            logger.info(f"Task {task.id} requeued for retry (Attempt {task.retries}/{task.max_retries}).")
            self.task_queue.requeue(task)
        else:
            logger.error(f"Task {task.id} exceeded max retries ({task.max_retries}). Final state: FAILED.")
