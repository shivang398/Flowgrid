from abc import ABC, abstractmethod
from typing import List
import threading

from common.models import Worker, Task, WorkerStatus
from .exceptions import NoWorkerAvailableError

class SchedulingStrategy(ABC):
    """Base framework for task scheduling algorithms."""
    
    @abstractmethod
    def select_worker(self, workers: List[Worker], task: Task) -> Worker:
        """
        Selects a worker from the available list to execute the task.
        Throws NoWorkerAvailableError if no worker suits the current constraints.
        """
        pass

class RoundRobinStrategy(SchedulingStrategy):
    """
    Distributes tasks cyclically across the list of available workers.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._last_index = -1

    def select_worker(self, workers: List[Worker], task: Task) -> Worker:
        if not workers:
            raise NoWorkerAvailableError("Cannot run RoundRobin, empty worker list.")
            
        with self._lock:
            # We filter out offline workers just in case the WorkerManager provided them
            viable = [w for w in workers if w.status != WorkerStatus.OFFLINE]
            if not viable:
                 raise NoWorkerAvailableError("All provided workers are offline.")
                 
            self._last_index = (self._last_index + 1) % len(viable)
            return viable[self._last_index]

class LeastLoadedStrategy(SchedulingStrategy):
    """
    Selects the worker with the lowest weighted resource score.
    Score = (task_count * 5.0) + (cpu_usage * 1.0) + (memory_usage * 0.5)
    Ties are broken cleanly by whoever appeared first in the array.
    """
    def select_worker(self, workers: List[Worker], task: Task) -> Worker:
        if not workers:
            raise NoWorkerAvailableError("Cannot run LeastLoaded, empty worker list.")
            
        viable = [w for w in workers if w.status != WorkerStatus.OFFLINE]
        
        # GPU Filtering
        if getattr(task, 'needs_gpu', False):
            viable = [w for w in viable if getattr(w, 'gpu_available', False)]
            if not viable:
                raise NoWorkerAvailableError(f"Task {task.id} requires GPU but no GPU-enabled workers are online.")

        if not viable:
             raise NoWorkerAvailableError("All provided workers are offline.")
        
        # Calculate a weighted score where lower is better.
        def calculate_score(w: Worker) -> float:
            return (w.load * 5.0) + (w.cpu_usage * 1.0) + (w.memory_usage * 0.5)

        chosen = min(viable, key=calculate_score)
        return chosen
