import traceback
from typing import Any, Callable, Dict, Tuple
from common import Task, get_logger

logger = get_logger("worker.executor")

class TaskExecutor:
    """
    Handles the safe execution of tasks by mapping function names to 
    registered callables.
    """
    def __init__(self):
        self._registry: Dict[str, Callable] = {}

    def register(self, name: str, func: Callable):
        """Registers a function to be available for cluster execution."""
        self._registry[name] = func
        logger.info(f"Registered function: {name}")

    def execute(self, task: Task) -> Any:
        """
        Executes the registered function associated with the task.
        Returns the result or raises an exception if execution fails.
        """
        if task.function_name not in self._registry:
            raise ValueError(f"Function '{task.function_name}' is not registered on this worker.")

        func = self._registry[task.function_name]
        
        # Unpack args and kwargs natively
        args = task.args or ()
        kwargs = task.kwargs or {}

        try:
            logger.debug(f"Executing task {task.id}: {task.function_name}")
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            # Capture full traceback for remote debugging
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"Task {task.id} execution failed: {error_msg}")
            raise RuntimeError(error_msg) from e
