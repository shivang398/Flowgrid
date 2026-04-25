import unittest
from common import Worker, Task, WorkerStatus
from master.scheduler.strategies import LeastLoadedStrategy

class TestResourceAwareScheduling(unittest.TestCase):
    def setUp(self):
        self.strategy = LeastLoadedStrategy()
        self.task = Task(id="test-task", function_name="add", args=[1, 2])

    def test_weighted_score_picks_lower_cpu(self):
        # Worker A: 0 tasks, but 80% CPU
        worker_a = Worker(worker_id="worker-a", load=0, cpu_usage=80.0, memory_usage=50.0)
        # Worker B: 1 task, but 10% CPU
        worker_b = Worker(worker_id="worker-b", load=1, cpu_usage=10.0, memory_usage=20.0)
        
        workers = [worker_a, worker_b]
        
        # Calculation:
        # Score A = (0 * 5.0) + (80 * 1.0) + (50 * 0.5) = 105.0
        # Score B = (1 * 5.0) + (10 * 1.0) + (20 * 0.5) = 25.0
        
        chosen = self.strategy.select_worker(workers, self.task)
        self.assertEqual(chosen.worker_id, "worker-b", "Should pick worker B despite having more tasks because its CPU is much lower.")

    def test_weighted_score_picks_idle_worker(self):
        # Both same CPU, but one has tasks
        worker_a = Worker(worker_id="worker-a", load=2, cpu_usage=20.0, memory_usage=20.0)
        worker_b = Worker(worker_id="worker-b", load=0, cpu_usage=20.0, memory_usage=20.0)
        
        workers = [worker_a, worker_b]
        chosen = self.strategy.select_worker(workers, self.task)
        self.assertEqual(chosen.worker_id, "worker-b")

if __name__ == "__main__":
    unittest.main()
