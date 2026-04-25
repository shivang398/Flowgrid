import docker
import traceback
from typing import Any
from common import Task, get_logger

logger = get_logger("worker.executor.docker")

class DockerExecutor:
    """
    Executes tasks by running them inside Docker containers.
    Provides isolation and multi-language support.
    """
    def __init__(self):
        try:
            self.client = docker.from_env()
            logger.info("Docker executor initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize Docker client: {e}")
            self.client = None

    def execute(self, task: Task) -> Any:
        """
        Pulls the image (if not present) and runs the container.
        """
        if not self.client:
            raise RuntimeError("Docker daemon not accessible on this worker.")

        if not task.image:
            raise ValueError(f"Task {task.id} requested Docker execution but no image was specified.")

        try:
            logger.info(f"Task {task.id}: Pulling image {task.image}...")
            self.client.images.pull(task.image)
            
            logger.info(f"Task {task.id}: Starting container with command: {task.command}")
            
            # Configure GPU support if requested
            device_requests = []
            if task.needs_gpu:
                device_requests.append(docker.types.DeviceRequest(count=-1, capabilities=[['gpu']]))

            container = self.client.containers.run(
                image=task.image,
                command=task.command,
                environment=task.env,
                device_requests=device_requests,
                detach=False,
                stdout=True,
                stderr=True,
                remove=True
            )
            
            # For simplicity, we return the decoded stdout as the result
            result = container.decode('utf-8').strip()
            logger.info(f"Task {task.id} completed successfully.")
            return result
            
        except Exception as e:
            error_msg = f"Docker execution failed: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"Task {task.id} failed: {error_msg}")
            raise RuntimeError(error_msg) from e
