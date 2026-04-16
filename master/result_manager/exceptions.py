class TaskNotFoundError(Exception):
    """Raised when a task ID is not found in the registry."""
    pass

class ResultAlreadyExistsError(Exception):
    """Raised when a final result already exists for a task."""
    pass
