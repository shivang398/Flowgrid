import unittest
import time
import threading
from common.models import WorkerStatus, Task
from master.worker_manager import WorkerManager, WorkerAlreadyExistsError, WorkerNotFoundError

class TestWorkerManager(unittest.TestCase):
    def setUp(self):
        # Using a very short stale timeout for testing
        self.manager = WorkerManager(stale_timeout=0.2)

    def test_registration(self):
        worker = self.manager.register_worker("w1")
        self.assertEqual(worker.worker_id, "w1")
        self.assertEqual(worker.status, WorkerStatus.IDLE)
        
        with self.assertRaises(WorkerAlreadyExistsError):
            self.manager.register_worker("w1")

    def test_update_status(self):
        self.manager.register_worker("w1")
        worker = self.manager.update_worker_status("w1", load=5)
        self.assertEqual(worker.load, 5)
        self.assertEqual(worker.status, WorkerStatus.BUSY)
        
        with self.assertRaises(WorkerNotFoundError):
            self.manager.update_worker_status("unknown", load=0)

    def test_get_available_workers(self):
        self.manager.register_worker("w1")
        self.manager.register_worker("w2")
        
        available = self.manager.get_available_workers()
        self.assertEqual(len(available), 2)
        
        # Test stale timeout
        time.sleep(0.3)
        available = self.manager.get_available_workers()
        self.assertEqual(len(available), 0)
        
        # Verify they are marked OFFLINE
        w1 = self.manager._workers["w1"]
        self.assertEqual(w1.status, WorkerStatus.OFFLINE)

    def test_re_registration_offline_worker(self):
        self.manager.register_worker("w1")
        time.sleep(0.3)
        self.manager.get_available_workers() # trigger stale check
        
        # Re-register
        worker = self.manager.register_worker("w1")
        self.assertEqual(worker.status, WorkerStatus.IDLE)
        self.assertEqual(len(self.manager.get_available_workers()), 1)

    def test_get_least_loaded_worker(self):
        self.manager.register_worker("w1")
        self.manager.register_worker("w2")
        self.manager.update_worker_status("w1", load=10)
        self.manager.update_worker_status("w2", load=2)
        
        least = self.manager.get_least_loaded_worker()
        self.assertEqual(least.worker_id, "w2")

    def test_remove_worker(self):
        self.manager.register_worker("w1")
        self.manager.remove_worker("w1")
        self.assertEqual(len(self.manager.get_available_workers()), 0)

    def test_concurrency(self):
        def worker_task(i):
            wid = f"worker_{i}"
            self.manager.register_worker(wid)
            for _ in range(10):
                self.manager.update_worker_status(wid, load=i)
                self.manager.get_available_workers()

        threads = []
        for i in range(20):
            t = threading.Thread(target=worker_task, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(len(self.manager.get_available_workers()), 20)

if __name__ == '__main__':
    unittest.main()
