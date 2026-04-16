import unittest
import time
from typing import List

from common.models import Worker, Task, WorkerStatus, TaskStatus
from master.queue import TaskQueue
from master.worker_manager import WorkerManagerInterface
from master.scheduler import Scheduler, RoundRobinStrategy, LeastLoadedStrategy, NoWorkerAvailableError

class MockWorkerManager(WorkerManagerInterface):
    def __init__(self, workers: List[Worker]):
        self.workers = workers
        self.assigned_log = []
        self.fail_assignments = False

    def get_available_workers(self) -> List[Worker]:
        return self.workers

    def assign_task(self, worker_id: str, task: Task) -> bool:
        if self.fail_assignments:
            return False
        self.assigned_log.append((worker_id, task.id))
        return True

class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.queue = TaskQueue()
        self.w1 = Worker(worker_id="w1", load=2)
        self.w2 = Worker(worker_id="w2", load=0)
        self.w3 = Worker(worker_id="w3", load=1)
        self.w_offline = Worker(worker_id="w4", status=WorkerStatus.OFFLINE)
        
        self.worker_mgr = MockWorkerManager([self.w1, self.w2, self.w3, self.w_offline])

    def test_least_loaded_strategy(self):
        # We expect w2 to be picked since its load is 0
        strategy = LeastLoadedStrategy()
        t = Task(id="t1", function_name="f")
        chosen = strategy.select_worker(self.worker_mgr.get_available_workers(), t)
        self.assertEqual(chosen.worker_id, "w2")

    def test_round_robin_strategy(self):
        strategy = RoundRobinStrategy()
        t = Task(id="t1", function_name="f")
        
        c1 = strategy.select_worker(self.worker_mgr.get_available_workers(), t)
        c2 = strategy.select_worker(self.worker_mgr.get_available_workers(), t)
        c3 = strategy.select_worker(self.worker_mgr.get_available_workers(), t)
        c4 = strategy.select_worker(self.worker_mgr.get_available_workers(), t)
        
        # Ignores w4 completely
        self.assertEqual(c1.worker_id, "w1")
        self.assertEqual(c2.worker_id, "w2")
        self.assertEqual(c3.worker_id, "w3")
        self.assertEqual(c4.worker_id, "w1")  # Cycle wraps

    def test_scheduler_loop_assigns_successful(self):
        self.queue.enqueue(Task(id="101", function_name="f"))
        scheduler = Scheduler(
            task_queue=self.queue, 
            worker_manager=self.worker_mgr, 
            strategy=LeastLoadedStrategy()
        )
        scheduler.start()
        time.sleep(0.1) # allow thread to loop once
        scheduler.stop()
        
        # Task was dequeued
        self.assertEqual(self.queue.get_queue_size(), 0)
        # Assigned to least loaded (w2)
        self.assertEqual(self.worker_mgr.assigned_log[0], ("w2", "101"))
        # Load was optimistically incremented
        self.assertEqual(self.w2.load, 1)

    def test_scheduler_handles_assignment_failure(self):
        self.worker_mgr.fail_assignments = True
        t = Task(id="202", function_name="f")
        self.queue.enqueue(t)
        
        scheduler = Scheduler(
            task_queue=self.queue, 
            worker_manager=self.worker_mgr, 
            strategy=RoundRobinStrategy()
        )
        scheduler.start()
        time.sleep(0.1)
        scheduler.stop()
        
        # Original task failed but was rapidly retried until max_retries
        # Since it loops extremely fast in 0.1s, it will hit max_retries (3) and drop the task.
        self.assertEqual(self.queue.get_queue_size(), 0)
        self.assertEqual(t.status, TaskStatus.FAILED)
        self.assertEqual(t.retries, t.max_retries)

if __name__ == '__main__':
    unittest.main()
