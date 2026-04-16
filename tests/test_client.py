import unittest
import threading
import time
import socket
from common import TaskStatus
from master.network.server import MasterServer
from master.worker_manager import WorkerManager
from master.result_manager import ResultManager
from master.queue import TaskQueue
from client.ray_client import RayClient

class TestRayClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Setup Master components
        cls.worker_mgr = WorkerManager()
        cls.result_mgr = ResultManager()
        cls.task_queue = TaskQueue()
        
        # Start Master Server on a random free port
        cls.server = MasterServer(cls.worker_mgr, cls.result_mgr, cls.task_queue, host="127.0.0.1", port=0)
        cls.server.start()
        
        # Wait for server to bind and get the port
        time.sleep(0.1)
        cls.host, cls.port = cls.server._server_sock.getsockname()
        cls.master_addr = (cls.host, cls.port)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        self.client = RayClient(self.host, self.port)

    def tearDown(self):
        self.client.disconnect()

    def test_submit_task(self):
        task_id = self.client.submit_task("add", 1, 2)
        self.assertIsNotNone(task_id)
        
        # Verify it went into the queue
        self.assertEqual(self.task_queue.get_queue_size(), 1)
        
        # Verify it's registered in ResultManager
        status = self.result_mgr.get_task_status(task_id)
        self.assertEqual(status, TaskStatus.PENDING)

    def test_get_result_polling(self):
        task_id = self.client.submit_task("multiply", 3, 4)
        
        # Simulate worker completing the task
        task = self.task_queue.dequeue()
        self.assertEqual(task.id, task_id)
        
        # Initially polling should return None if wait=False
        res = self.client.get_result(task_id, wait=False)
        self.assertIsNone(res)
        
        # Mark as completed
        self.result_mgr.store_result(task_id, 12)
        
        # Now fetch result with wait=True
        res = self.client.get_result(task_id, wait=True)
        self.assertEqual(res, 12)

    def test_task_failure(self):
        task_id = self.client.submit_task("error_func")
        
        # Simulate failure
        self.result_mgr.mark_task_failed(task_id, "Intentional Error")
        
        with self.assertRaises(RuntimeError) as cm:
            self.client.get_result(task_id)
        
        self.assertIn("Intentional Error", str(cm.exception))

    def test_connection_error(self):
        # Try a client connected to a wrong port
        bad_client = RayClient("127.0.0.1", 1) # Port 1 is likely closed
        with self.assertRaises(ConnectionError):
            bad_client.connect()

if __name__ == "__main__":
    unittest.main()
