import threading
from typing import Dict, Any, Optional
from common import Task, TaskStatus, get_logger, current_timestamp
from .exceptions import TaskNotFoundError, ResultAlreadyExistsError
from master.metrics import MasterMetrics

logger = get_logger("master.result_manager")

class ResultManager:
    """
    Authoritative store for task results. Tracks task lifecycle from
    pending/running to completed/failed.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._tasks: Dict[str, Task] = {}
        self._recent_latencies = []
        self._max_latency_history = 50

    def register_task(self, task: Task):
        """Registers a task in the result manager's tracking registry."""
        with self._lock:
            self._tasks[task.id] = task
            logger.debug(f"Registered task {task.id} in ResultManager.")

    def store_result(self, task_id: str, result: Any, is_partial: bool = False):
        """
        Stores a partial or final result for a task.
        If is_partial is False, the task is marked as COMPLETED.
        """
        with self._lock:
            if task_id not in self._tasks:
                logger.error(f"Cannot store result for unknown task: {task_id}")
                raise TaskNotFoundError(f"Task {task_id} not found.")

            task = self._tasks[task_id]

            if task.status == TaskStatus.COMPLETED or task.status == TaskStatus.FAILED:
                logger.warning(f"Task {task_id} is already in a terminal state ({task.status}). Ignoring result.")
                return

            if is_partial:
                logger.debug(f"Received partial result for task {task_id}: {result}")
                # For now, we just update the result field. In a more complex system,
                # we might have a separate list of partial outputs.
                task.result = result
            else:
                logger.info(f"Received final result for task {task_id}.")
                task.mark_completed(result)
                
                # Metrics
                latency = None
                if task.started_at:
                    latency = current_timestamp() - task.started_at
                    self._recent_latencies.append(latency)
                    if len(self._recent_latencies) > self._max_latency_history:
                        self._recent_latencies.pop(0)
                        
                MasterMetrics.task_completed("COMPLETED", latency=latency)

    def mark_task_failed(self, task_id: str, error: str):
        """Explicitly marks a task as FAILED."""
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(f"Task {task_id} not found.")
            
            task = self._tasks[task_id]
            task.mark_failed(error)
            logger.info(f"Task {task_id} marked as FAILED in ResultManager.")
            
            # Metrics
            MasterMetrics.task_completed("FAILED")

    def get_result(self, task_id: str) -> Optional[Any]:
        """Returns the result if the task is completed, else None or partial."""
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(f"Task {task_id} not found.")
            
            task = self._tasks[task_id]
            return task.result if task.status == TaskStatus.COMPLETED else None

    def get_task(self, task_id: str) -> Task:
        """Returns the full Task object."""
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(f"Task {task_id} not found.")
            return self._tasks[task_id]

    def get_task_status(self, task_id: str) -> TaskStatus:
        """Helper to quickly check current state of a task."""
        with self._lock:
            if task_id not in self._tasks:
                raise TaskNotFoundError(f"Task {task_id} not found.")
            return self._tasks[task_id].status

    def get_avg_latency(self) -> float:
        """Returns the rolling average latency in milliseconds."""
        with self._lock:
            if not self._recent_latencies:
                return 0.0
            avg_sec = sum(self._recent_latencies) / len(self._recent_latencies)
            return round(avg_sec * 1000, 2)
