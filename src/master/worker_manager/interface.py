from abc import ABC, abstractmethod
from typing import List
from common import Worker, Task

class WorkerManagerInterface(ABC):
    """
    Abstract interface defining the contract that the Scheduler expects
    the Worker Manager to fulfill.
    """
    
    @abstractmethod
    def get_available_workers(self) -> List[Worker]:
        """
        Retrieves all currently connected workers that are not offline.
        """
        pass

    @abstractmethod
    def assign_task(self, worker_id: str, task: Task) -> bool:
        """
        Dispatches the task to a specific worker.
        Returns True if transmission was successful, False otherwise.
        """
        pass
