import queue
import threading
from dataclasses import dataclass, field
from typing import Optional, List, Set, Dict

from common import Task, TaskStatus, get_logger
from .exceptions import QueueOverflowError, QueueEmptyError

logger = get_logger("master.queue")

@dataclass(order=True)
class _QueueItem:
    """Wrapper to enable correct priority sorting in the queue."""
    priority: int
    created_at: float
    task: Task = field(compare=False)

class TaskQueue:
    """
    A thread-safe priority queue with DAG dependency support.
    """
    def __init__(self, maxsize: int = 0):
        self._queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=maxsize)
        self._lock = threading.Lock()
        
        # DAG Tracking
        self._blocked_tasks: List[Task] = []
        self._completed_task_ids: Set[str] = set()
        self._task_priorities: Dict[str, int] = {} # Remember original priorities for unblocking

    def enqueue(self, task: Task, priority: int = 10, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Pushes a task into the queue if dependencies are met, else holds it.
        """
        with self._lock:
            # 1. Check if all dependencies are already met
            unmet_deps = [dep_id for dep_id in task.depends_on if dep_id not in self._completed_task_ids]
            
            if unmet_deps:
                logger.info(f"Task {task.id} blocked by dependencies: {unmet_deps}")
                task.status = TaskStatus.PENDING # Keep as pending but not queued
                self._blocked_tasks.append(task)
                self._task_priorities[task.id] = priority
                return True

            # 2. Dependencies met, proceed to normal enqueue
            return self._push_to_internal_queue(task, priority, block, timeout)

    def _push_to_internal_queue(self, task: Task, priority: int, block: bool, timeout: Optional[float]) -> bool:
        """Helper to physically push a task into the PriorityQueue."""
        task.status = TaskStatus.PENDING
        item = _QueueItem(priority=priority, created_at=task.created_at, task=task)
        try:
            self._queue.put(item, block=block, timeout=timeout)
            logger.info(f"Enqueued task {task.id} (priority: {priority})")
            return True
        except queue.Full:
            logger.warning(f"Queue overflow for task {task.id}")
            raise QueueOverflowError(f"TaskQueue is full.")

    def notify_completion(self, task_id: str):
        """
        Notifies the queue that a task has finished. 
        Re-evaluates blocked tasks and releases them if ready.
        """
        with self._lock:
            self._completed_task_ids.add(task_id)
            
            still_blocked = []
            for task in self._blocked_tasks:
                # Check if this task is now ready
                unmet = [d for d in task.depends_on if d not in self._completed_task_ids]
                if not unmet:
                    logger.info(f"Unblocking task {task.id} as all dependencies are met.")
                    priority = self._task_priorities.get(task.id, 10)
                    self._push_to_internal_queue(task, priority, block=False, timeout=None)
                else:
                    still_blocked.append(task)
            
            self._blocked_tasks = still_blocked

    def dequeue(self, block: bool = True, timeout: Optional[float] = None) -> Task:
        """Pulls the highest priority task."""
        try:
            item: _QueueItem = self._queue.get(block=block, timeout=timeout)
            logger.debug(f"Dequeued task {item.task.id}")
            return item.task
        except queue.Empty:
            raise QueueEmptyError("TaskQueue is empty.")

    def requeue(self, task: Task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Requeues with highest priority (0)."""
        return self._push_to_internal_queue(task, priority=0, block=block, timeout=timeout)

    def get_queue_size(self) -> int:
        return self._queue.qsize()
