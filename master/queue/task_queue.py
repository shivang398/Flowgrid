import queue
from dataclasses import dataclass, field
from typing import Optional

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
    A thread-safe priority queue for tasks.
    Lower priority number means higher precedence (e.g., priority 1 runs before priority 10).
    Same priorities are broken identically via the task's created_at timestamp.
    """
    def __init__(self, maxsize: int = 0):
        self._queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=maxsize)

    def enqueue(self, task: Task, priority: int = 10, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Pushes a task into the queue.
        
        Raises QueueOverflowError if the queue is full and block is False or timeout completes.
        Returns True on success.
        """
        # Status transition
        task.status = TaskStatus.PENDING
        
        item = _QueueItem(priority=priority, created_at=task.created_at, task=task)
        try:
            self._queue.put(item, block=block, timeout=timeout)
            logger.info(f"Enqueued task {task.id} with priority {priority}")
            return True
        except queue.Full:
            logger.warning(f"Queue overflow when attempting to enqueue task {task.id}")
            raise QueueOverflowError(f"TaskQueue is full, cannot enqueue task {task.id}.")

    def dequeue(self, block: bool = True, timeout: Optional[float] = None) -> Task:
        """
        Pulls the highest priority task from the queue.
        
        Raises QueueEmptyError if the queue is empty and block is False or timeout completes.
        """
        try:
            item: _QueueItem = self._queue.get(block=block, timeout=timeout)
            logger.debug(f"Dequeued task {item.task.id}")
            return item.task
        except queue.Empty:
            raise QueueEmptyError("TaskQueue is empty.")

    def requeue(self, task: Task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Requeues an existing task, commonly used for retries. 
        Will automatically be given highest normal priority (0) so retries execute first.
        """
        # Requeue prioritizes failures (0 -> highest logic)
        logger.info(f"Requeuing task {task.id}")
        return self.enqueue(task, priority=0, block=block, timeout=timeout)

    def get_queue_size(self) -> int:
        """Returns the approximate size of the queue."""
        return self._queue.qsize()
