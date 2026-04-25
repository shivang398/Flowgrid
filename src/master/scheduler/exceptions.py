class NoWorkerAvailableError(Exception):
    """Raised by a scheduling strategy when no suitable workers can be found."""
    pass

class TaskAssignmentError(Exception):
    """Raised when the WorkerManager fails to assign a task across the network."""
    pass
