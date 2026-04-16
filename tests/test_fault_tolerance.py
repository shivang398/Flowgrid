import unittest
import time
from common import Task, TaskStatus, WorkerStatus
from master.queue import TaskQueue
from master.result_manager import ResultManager
from master.worker_manager import WorkerManager
from master.fault_tolerance import FaultToleranceManager

class TestFaultTolerance(unittest.TestCase):
    def setUp(self):
        self.queue = TaskQueue()
        self.result_mgr = ResultManager()
        self.worker_mgr = WorkerManager()
        
        # Short timeout for testing
        self.ft_mgr = FaultToleranceManager(
            task_queue=self.queue,
            result_manager=self.result_mgr,
            worker_manager=self.worker_mgr,
            timeout_threshold=0.1,
            check_interval=0.1
        )

    def test_detect_timeouts(self):
        task = Task(id="t1", function_name="f")
        self.result_mgr.register_task(task)
        task.mark_running("w1")
        
        # Wait for timeout
        time.sleep(0.2)
        self.ft_mgr.detect_timeouts()
        
        # Task should be back in queue as PENDING
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertEqual(task.retries, 1)
        self.assertEqual(self.queue.get_queue_size(), 1)

    def test_worker_failure_handling(self):
        self.worker_mgr.register_worker("w1")
        task = Task(id="t1", function_name="f")
        self.result_mgr.register_task(task)
        task.mark_running("w1")
        
        # Simulate worker going offline
        # In our case, we just need to wait for stale timeout or manually mark offline
        self.worker_mgr._workers["w1"].mark_offline()
        
        self.ft_mgr.check_worker_failures()
        
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertEqual(self.queue.get_queue_size(), 1)

    def test_max_retries_exhaustion(self):
        task = Task(id="t1", function_name="f", max_retries=1)
        self.result_mgr.register_task(task)
        task.mark_running("w1")
        
        # Timeout once
        time.sleep(0.2)
        self.ft_mgr.detect_timeouts()
        self.assertEqual(task.retries, 1)
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertEqual(self.queue.get_queue_size(), 1)
        
        # Consume and mock second run
        self.queue.dequeue()
        task.mark_running("w1")
        
        # Timeout again
        time.sleep(0.2)
        self.ft_mgr.detect_timeouts()
        
        # Should now be FAILED, not requeued
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(self.queue.get_queue_size(), 0)

if __name__ == '__main__':
    unittest.main()
