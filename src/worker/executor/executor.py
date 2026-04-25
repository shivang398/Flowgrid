import traceback
from typing import Any, Callable, Dict, Tuple
from common import Task, get_logger
from .docker_executor import DockerExecutor

logger = get_logger("worker.executor")

class TaskExecutor:
    """
    Unified executor that routes tasks to either local Python 
    registry or the Docker container runtime.
    """
    def __init__(self):
        self._registry: Dict[str, Callable] = {}
        self._docker_executor = DockerExecutor()

    def register(self, name: str, func: Callable):
        """Registers a function for local Python execution."""
        self._registry[name] = func
        logger.info(f"Registered function: {name}")

    def execute(self, task: Task) -> Any:
        """
        Routes the task to the appropriate engine based on its payload.
        """
        # 1. If 'image' is present, use Docker
        if task.image:
            return self._docker_executor.execute(task)

        # 2. Otherwise, fall back to Python registry
        if not task.function_name:
            raise ValueError(f"Task {task.id} has neither an image nor a function_name.")

        if task.function_name not in self._registry:
            raise ValueError(f"Function '{task.function_name}' is not registered on this worker.")

        func = self._registry[task.function_name]
        args = task.args or ()
        kwargs = task.kwargs or {}

        try:
            logger.debug(f"Executing Python task {task.id}: {task.function_name}")
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"Task {task.id} execution failed: {error_msg}")
            raise RuntimeError(error_msg) from e
