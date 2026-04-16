import unittest
import time
import threading
from common import Task
from worker.executor.executor import TaskExecutor
from worker.communication.client import WorkerCommunicationClient
from worker.worker import WorkerNode

class TestTaskExecutor(unittest.TestCase):
    def setUp(self):
        self.executor = TaskExecutor()

    def test_registration_and_execution(self):
        self.executor.register("add", lambda a, b: a + b)
        task = Task(id="t1", function_name="add", args=(5, 3))
        result = self.executor.execute(task)
        self.assertEqual(result, 8)

    def test_unregistered_function(self):
        task = Task(id="t1", function_name="unknown")
        with self.assertRaises(ValueError):
            self.executor.execute(task)

    def test_execution_failure(self):
        def fail(): raise ValueError("Intentional")
        self.executor.register("fail", fail)
        task = Task(id="t1", function_name="fail")
        with self.assertRaises(RuntimeError):
            self.executor.execute(task)

class MockCommunicationClient(WorkerCommunicationClient):
    def __init__(self, master_url):
        super().__init__(master_url)
        self.heartbeats = []
        self.results = []
        self.task_queue = []

    def send_heartbeat(self, worker_id, load):
        self.heartbeats.append((worker_id, load))
        return True

    def send_result(self, task_id, result, is_error=False):
        self.results.append((task_id, result, is_error))
        return True

    def poll_task(self, worker_id):
        if self.task_queue:
            return self.task_queue.pop(0)
        return None

class TestWorkerNode(unittest.TestCase):
    def setUp(self):
        self.worker = WorkerNode("w1", "http://localhost:8080", heartbeat_interval=0.1)
        # Injection of mock client
        self.mock_comm = MockCommunicationClient("http://localhost:8080")
        self.worker.comm_client = self.mock_comm
        self.worker.executor.register("echo", lambda x: x)

    def test_worker_lifecycle_and_task_loop(self):
        # We start the worker in a separate thread because _main_loop is blocking
        worker_thread = threading.Thread(target=self.worker.start, daemon=True)
        worker_thread.start()
        
        # 1. Verify heartbeat is happening
        time.sleep(0.3)
        self.assertGreater(len(self.mock_comm.heartbeats), 0)
        
        # 2. Add a task to the mock queue
        t = Task(id="t_test", function_name="echo", args=("hello",))
        self.mock_comm.task_queue.append(t)
        
        # 3. Wait for processing
        time.sleep(1.2) # Allow main loop to poll and process
        
        # 4. Verify result sent
        self.assertEqual(len(self.mock_comm.results), 1)
        self.assertEqual(self.mock_comm.results[0][0], "t_test")
        self.assertEqual(self.mock_comm.results[0][1], "hello")
        
        self.worker.stop()
        worker_thread.join(timeout=1.0)

if __name__ == '__main__':
    unittest.main()
