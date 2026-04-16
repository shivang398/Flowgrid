class QueueOverflowError(Exception):
    """Raised when attempting to enqueue a task into a full queue."""
    pass

class QueueEmptyError(Exception):
    """Raised when attempting to dequeue from an empty queue."""
    pass
