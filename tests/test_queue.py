import unittest
import time
import threading
from common import Task, TaskStatus
from master.queue import TaskQueue, QueueOverflowError, QueueEmptyError

class TestTaskQueue(unittest.TestCase):
    def test_enqueue_dequeue(self):
        q = TaskQueue(maxsize=10)
        t = Task(id="1", function_name="func")
        
        # Enqueue successfully changes status to pending
        t.status = TaskStatus.FAILED
        self.assertTrue(q.enqueue(t))
        self.assertEqual(t.status, TaskStatus.PENDING)
        self.assertEqual(q.get_queue_size(), 1)
        
        # Dequeue retrieves identical task
        dequeued = q.dequeue()
        self.assertEqual(dequeued.id, "1")
        self.assertEqual(q.get_queue_size(), 0)

    def test_queue_overflow(self):
        q = TaskQueue(maxsize=1)
        t1 = Task(id="1", function_name="func")
        t2 = Task(id="2", function_name="func")
        
        self.assertTrue(q.enqueue(t1))
        
        with self.assertRaises(QueueOverflowError):
            q.enqueue(t2, block=False)

    def test_queue_empty(self):
        q = TaskQueue()
        with self.assertRaises(QueueEmptyError):
            q.dequeue(block=False)

    def test_priority(self):
        q = TaskQueue()
        t1 = Task(id="1", function_name="func") # Defaults to current timestamp
        time.sleep(0.01)
        t2 = Task(id="2", function_name="func")
        time.sleep(0.01)
        t3 = Task(id="3", function_name="func")
        
        # t2 has highest priority (0 vs 10 vs 20)
        q.enqueue(t1, priority=10)
        q.enqueue(t2, priority=0)
        q.enqueue(t3, priority=20)
        
        # t2 should be first
        self.assertEqual(q.dequeue().id, "2")
        # t1 should be second
        self.assertEqual(q.dequeue().id, "1")
        # t3 should be last
        self.assertEqual(q.dequeue().id, "3")

    def test_fifo_tiebreaker(self):
        q = TaskQueue()
        # Create identical timestamps to test fallback or just rely on timestamps
        t1 = Task(id="1", function_name="func", created_at=100)
        t2 = Task(id="2", function_name="func", created_at=200)
        
        # enqueue backwards to prove it relies on logic not enqueue order
        q.enqueue(t2, priority=5)
        q.enqueue(t1, priority=5)
        
        # t1 was created earlier, should execute first
        self.assertEqual(q.dequeue().id, "1")
        self.assertEqual(q.dequeue().id, "2")

    def test_requeue(self):
        q = TaskQueue()
        t1 = Task(id="1", function_name="func")
        t2 = Task(id="2", function_name="func")
        
        q.enqueue(t1, priority=10) # Normal task
        q.requeue(t2) # Requeued task (gets priority 0)
        
        self.assertEqual(q.dequeue().id, "2")

    def test_concurrency(self):
        q = TaskQueue()
        
        def producer(i: int):
            t = Task(id=str(i), function_name="func")
            q.enqueue(t)

        threads = []
        for i in range(50):
            th = threading.Thread(target=producer, args=(i,))
            threads.append(th)
            th.start()
            
        for th in threads:
            th.join()
            
        self.assertEqual(q.get_queue_size(), 50)
        
        ids_pulled = set()
        for _ in range(50):
            ids_pulled.add(q.dequeue().id)
            
        self.assertEqual(len(ids_pulled), 50)

if __name__ == '__main__':
    unittest.main()
