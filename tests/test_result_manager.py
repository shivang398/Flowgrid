import unittest
import threading
from common import Task, TaskStatus
from master.result_manager import ResultManager, TaskNotFoundError

class TestResultManager(unittest.TestCase):
    def setUp(self):
        self.manager = ResultManager()
        self.task = Task(id="t1", function_name="test_func")

    def test_registration_and_status(self):
        self.manager.register_task(self.task)
        self.assertEqual(self.manager.get_task_status("t1"), TaskStatus.PENDING)

    def test_store_final_result(self):
        self.manager.register_task(self.task)
        self.manager.store_result("t1", result=42, is_partial=False)
        self.assertEqual(self.manager.get_task_status("t1"), TaskStatus.COMPLETED)
        self.assertEqual(self.manager.get_result("t1"), 42)

    def test_partial_result(self):
        self.manager.register_task(self.task)
        self.manager.store_result("t1", result="part 1", is_partial=True)
        # Status should NOT be completed
        self.assertNotEqual(self.manager.get_task_status("t1"), TaskStatus.COMPLETED)
        # get_result only returns final results in my implementation, or I can check task.result
        self.assertEqual(self.manager.get_task("t1").result, "part 1")
        
        # Store another partial
        self.manager.store_result("t1", result="part 2", is_partial=True)
        self.assertEqual(self.manager.get_task("t1").result, "part 2")

    def test_duplicate_final_result(self):
        self.manager.register_task(self.task)
        self.manager.store_result("t1", result=100, is_partial=False)
        self.assertEqual(self.manager.get_result("t1"), 100)
        
        # Subsequent final result should be ignored
        self.manager.store_result("t1", result=200, is_partial=False)
        self.assertEqual(self.manager.get_result("t1"), 100)

    def test_missing_task(self):
        with self.assertRaises(TaskNotFoundError):
            self.manager.store_result("unknown", result=0)

    def test_concurrent_updates(self):
        self.manager.register_task(self.task)
        
        def update_task():
            for i in range(100):
                self.manager.store_result("t1", result=i, is_partial=True)

        threads = []
        for _ in range(10):
            t = threading.Thread(target=update_task)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Just verify it didn't crash and status is still consistent
        self.assertIn(self.manager.get_task_status("t1"), [TaskStatus.PENDING, TaskStatus.RUNNING])

if __name__ == '__main__':
    unittest.main()
