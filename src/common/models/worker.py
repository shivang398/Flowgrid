from dataclasses import dataclass, field
from enum import Enum
from ..utils.time_tracking import current_timestamp

class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"

@dataclass
class Worker:
    worker_id: str
    status: WorkerStatus = WorkerStatus.IDLE
    load: int = 0  # Number of currently executing tasks
    cpu_usage: float = 0.0  # Percentage (0-100)
    memory_usage: float = 0.0  # Percentage (0-100)
    gpu_available: bool = False # Whether node has NVIDIA GPU
    last_heartbeat: float = field(default_factory=current_timestamp)

    def __post_init__(self):
        # Validation and coercion
        if isinstance(self.status, str) and not isinstance(self.status, WorkerStatus):
            self.status = WorkerStatus(self.status)

    def update_heartbeat(self, load: int, cpu_usage: float = 0.0, memory_usage: float = 0.0, gpu_available: bool = False):
        """Updates worker heartbeat and load information."""
        self.last_heartbeat = current_timestamp()
        self.load = load
        self.cpu_usage = cpu_usage
        self.memory_usage = memory_usage
        self.gpu_available = gpu_available
        
        if self.load > 0:
            self.status = WorkerStatus.BUSY
        else:
            self.status = WorkerStatus.IDLE

    def mark_offline(self):
        """Marks the worker as completely offline."""
        self.status = WorkerStatus.OFFLINE
