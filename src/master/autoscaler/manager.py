import time
import threading
import docker
from common import get_logger
from master.queue import TaskQueue
from master.worker_manager import WorkerManager

logger = get_logger("master.autoscaler")

class Autoscaler:
    """
    Monitors cluster load and dynamically spawns/reaps worker containers.
    """
    def __init__(
        self, 
        task_queue: TaskQueue, 
        worker_manager: WorkerManager,
        min_workers: int = 1,
        max_workers: int = 10,
        scale_threshold: int = 5,
        check_interval: float = 10.0,
        worker_image: str = "flowgrid-worker"
    ):
        self.task_queue = task_queue
        self.worker_manager = worker_manager
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_threshold = scale_threshold
        self.check_interval = check_interval
        self.worker_image = worker_image
        
        self._running = False
        self._thread = None
        
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            logger.warning(f"Autoscaler disabled: Docker daemon not accessible ({e})")
            self.docker_client = None

    def start(self):
        if not self.docker_client:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="Autoscaler-Thread")
        self._thread.start()
        logger.info("Autoscaler started.")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run(self):
        while self._running:
            try:
                self._check_and_scale()
            except Exception as e:
                logger.error(f"Autoscaler loop error: {e}")
            time.sleep(self.check_interval)

    def _check_and_scale(self):
        queue_size = self.task_queue.get_queue_size()
        active_workers = self.worker_manager.get_available_workers()
        worker_count = len(active_workers)
        
        logger.debug(f"Autoscaler check: Queue={queue_size}, Workers={worker_count}")

        # Scale UP logic
        if queue_size > self.scale_threshold and worker_count < self.max_workers:
            needed = (queue_size // self.scale_threshold)
            to_spawn = min(needed, self.max_workers - worker_count)
            if to_spawn > 0:
                logger.info(f"High load detected. Scaling UP: Spawning {to_spawn} workers...")
                for _ in range(to_spawn):
                    self._spawn_worker()

        # Scale DOWN logic (cooldown)
        elif queue_size == 0 and worker_count > self.min_workers:
            # We don't explicitly kill workers here yet to avoid cutting off tasks, 
            # but we could implement worker reaping if they stay IDLE too long.
            pass

    def _spawn_worker(self):
        try:
            # In a real setup, we'd need the master IP/host from the environment
            self.docker_client.containers.run(
                self.worker_image,
                command="python3 -m worker.worker --master-host master --master-port 9999",
                detach=True,
                network="flowgrid-net"
            )
        except Exception as e:
            logger.error(f"Failed to spawn worker: {e}")
