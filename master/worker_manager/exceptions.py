class WorkerNotFoundError(Exception):
    """Raised when an operation targets a worker that does not exist in the active registry."""
    pass

class WorkerAlreadyExistsError(Exception):
    """Raised when attempting to register a worker_id that is already actively tracked."""
    pass
