from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple
from ..utils.time_tracking import current_timestamp
from ..utils.identifiers import generate_idempotency_key

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class Task:
    id: str
    function_name: str
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    retries: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=current_timestamp)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Any] = None

    def __post_init__(self):
        # Validation and coercion
        if isinstance(self.status, str) and not isinstance(self.status, TaskStatus):
            self.status = TaskStatus(self.status)
        if self.idempotency_key is None:
            self.idempotency_key = generate_idempotency_key(self.function_name, *self.args, **self.kwargs)
        if isinstance(self.args, list):
            self.args = tuple(self.args)

    def mark_running(self, worker_id: str):
        """Transitions task to RUNNING state and associates with a worker."""
        self.status = TaskStatus.RUNNING
        self.worker_id = worker_id
        self.started_at = current_timestamp()

    def mark_completed(self, result: Any):
        """Transitions task to COMPLETED state and saves the result."""
        self.status = TaskStatus.COMPLETED
        self.completed_at = current_timestamp()
        self.result = result

    def mark_failed(self, error: str):
        """Transitions task to FAILED state and increments retries, returning True if it can be retried."""
        self.error = error
        if self.retries < self.max_retries:
            self.retries += 1
            self.status = TaskStatus.PENDING
            return True
        self.status = TaskStatus.FAILED
        self.completed_at = current_timestamp()
        return False
