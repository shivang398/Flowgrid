import threading
import time
from typing import Optional

from common import get_logger, TaskStatus, WorkerStatus
from master.queue import TaskQueue, QueueEmptyError
from master.worker_manager import WorkerManagerInterface
from .strategies import SchedulingStrategy, RoundRobinStrategy
from .exceptions import NoWorkerAvailableError, TaskAssignmentError

logger = get_logger("master.scheduler")

class Scheduler:
    """
    Core brain of the master node. Continuously polls the TaskQueue and delegates 
    tasks to distributed workers using predefined algorithms.
    """
    
    def __init__(
        self, 
        task_queue: TaskQueue, 
        worker_manager: WorkerManagerInterface, 
        strategy: Optional[SchedulingStrategy] = None
    ):
        self.task_queue = task_queue
        self.worker_manager = worker_manager
        self.strategy = strategy or RoundRobinStrategy()
        
        self._running = False
        self._loop_thread: Optional[threading.Thread] = None

    def start(self):
        """Starts the background scheduling thread."""
        if self._running:
            return
        logger.info("Initializing Master Scheduler.")
        self._running = True
        self._loop_thread = threading.Thread(target=self._run, daemon=True, name="Scheduler-Thread")
        self._loop_thread.start()

    def stop(self, timeout: float = 2.0):
        """Signals the background loop to halt safely."""
        logger.info("Stopping Master Scheduler.")
        self._running = False
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=timeout)

    def _run(self):
        """Internal daemon payload processing."""
        while self._running:
            try:
                # 1. Pull next task
                task = self.task_queue.dequeue(block=True, timeout=1.0)
                
                # 2. Acquire network worker context
                candidates = self.worker_manager.get_available_workers()
                
                # 3. Algorithm selects the best worker
                chosen_worker = self.strategy.select_worker(candidates, task)
                
                # 4. Optimistically update local state representing transit
                task.mark_running(chosen_worker.worker_id)
                # We artificially increment load here until next heartbeat syncs it accurately
                chosen_worker.update_heartbeat(load=chosen_worker.load + 1)
                
                # 5. Execute network transition
                success = self.worker_manager.assign_task(chosen_worker.worker_id, task)
                
                if not success:
                    raise TaskAssignmentError(f"WorkerManager refused or failed delivery to {chosen_worker.worker_id}")
                
                logger.info(f"Task {task.id} successfully assigned to worker {chosen_worker.worker_id}.")

            except QueueEmptyError:
                # Normal operational idling.
                continue
            except NoWorkerAvailableError:
                logger.warning("No workers available to process queued tasks. Requeuing task and backing off...")
                self.task_queue.requeue(task)
                time.sleep(2.0) # Apply dynamic backoff logic
            except TaskAssignmentError as e:
                logger.error(f"Assignment fail: {str(e)}. Pushing failure into requeue logic.")
                # We revert the optimistic worker load update here if we failed delivery
                if 'chosen_worker' in locals():
                    chosen_worker.update_heartbeat(load=max(0, chosen_worker.load - 1))
                
                if task.mark_failed(str(e)):
                     self.task_queue.requeue(task)
            except Exception as e:
                # Catch-all boundary to keep thread alive during catastrophic unexpected failure
                logger.error(f"Unexpected Scheduler Fault: {str(e)}")
                if 'task' in locals():
                    self.task_queue.requeue(task)
                time.sleep(1.0)
