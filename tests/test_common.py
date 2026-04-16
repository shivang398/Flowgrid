import unittest
import time
import json
from common import (
    Task, TaskStatus, Worker, WorkerStatus,
    serialize, deserialize,
    Message, MessageType,
    generate_uuid, generate_idempotency_key, timer_context
)

class TestModels(unittest.TestCase):
    def test_task_initialization(self):
        task = Task(id="t1", function_name="my_func", args=(1, 2), kwargs={"x": 3})
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertIsNotNone(task.idempotency_key)
        self.assertEqual(task.args, (1, 2))
        self.assertEqual(task.kwargs, {"x": 3})

    def test_task_idempotency_key(self):
        task1 = Task(id="t1", function_name="func", args=(1,), kwargs={"a": 1})
        task2 = Task(id="t2", function_name="func", args=(1,), kwargs={"a": 1})
        task3 = Task(id="t3", function_name="func", args=(2,), kwargs={"a": 1})
        self.assertEqual(task1.idempotency_key, task2.idempotency_key)
        self.assertNotEqual(task1.idempotency_key, task3.idempotency_key)

    def test_task_state_transitions(self):
        task = Task(id="t1", function_name="func")
        task.mark_running()
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertIsNotNone(task.started_at)

        task.mark_completed("result_data")
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.result, "result_data")
        self.assertIsNotNone(task.completed_at)

    def test_task_retries(self):
        task = Task(id="t1", function_name="func", max_retries=2)
        can_retry_1 = task.mark_failed("error 1")
        self.assertTrue(can_retry_1)
        self.assertEqual(task.retries, 1)
        self.assertEqual(task.status, TaskStatus.PENDING)

        can_retry_2 = task.mark_failed("error 2")
        self.assertTrue(can_retry_2)
        self.assertEqual(task.retries, 2)
        
        can_retry_3 = task.mark_failed("error 3")
        self.assertFalse(can_retry_3)
        self.assertEqual(task.status, TaskStatus.FAILED)

    def test_worker_initialization(self):
        worker = Worker(worker_id="w1")
        self.assertEqual(worker.status, WorkerStatus.IDLE)
        self.assertEqual(worker.load, 0)

    def test_worker_heartbeat(self):
        worker = Worker(worker_id="w1")
        original_heartbeat = worker.last_heartbeat
        time.sleep(0.01) # ensure time difference
        worker.update_heartbeat(load=1)
        self.assertEqual(worker.status, WorkerStatus.BUSY)
        self.assertEqual(worker.load, 1)
        self.assertGreater(worker.last_heartbeat, original_heartbeat)

        worker.update_heartbeat(load=0)
        self.assertEqual(worker.status, WorkerStatus.IDLE)

        worker.mark_offline()
        self.assertEqual(worker.status, WorkerStatus.OFFLINE)


class TestProtocol(unittest.TestCase):
    def test_valid_message(self):
        msg = Message(type=MessageType.TASK, task_id="t1", payload={"data": 1})
        self.assertEqual(msg.type, MessageType.TASK)

    def test_invalid_message_no_task_id(self):
        with self.assertRaises(ValueError):
            Message(type=MessageType.TASK, payload={})
            
    def test_valid_message_no_task_id_for_heartbeat(self):
        msg = Message(type=MessageType.HEARTBEAT, payload={"load": 1})
        self.assertIsNone(msg.task_id)


class TestSerialization(unittest.TestCase):
    def test_serialize_task_and_message(self):
        task = Task(id="t1", function_name="func", args=(1, "two"))
        msg = Message(type=MessageType.TASK, task_id=task.id, payload=task.__dict__)
        
        serialized = serialize(msg)
        self.assertIsInstance(serialized, str)
        
        deserialized = deserialize(serialized)
        self.assertEqual(deserialized["type"], "TASK")
        self.assertEqual(deserialized["task_id"], "t1")
        self.assertEqual(deserialized["payload"]["id"], "t1")
        self.assertEqual(deserialized["payload"]["args"], [1, "two"])

    def test_serialize_dataclass(self):
        worker = Worker(worker_id="wx")
        serialized = serialize(worker)
        deserialized = deserialize(serialized)
        self.assertEqual(deserialized["worker_id"], "wx")
        self.assertEqual(deserialized["status"], "IDLE")

class TestUtils(unittest.TestCase):
    def test_generate_uuid(self):
        u1 = generate_uuid()
        u2 = generate_uuid()
        self.assertNotEqual(u1, u2)
        self.assertIsInstance(u1, str)

    def test_timer_context(self):
        with timer_context() as stats:
            time.sleep(0.01)
        self.assertIn("elapsed_time", stats)
        self.assertGreaterEqual(stats["elapsed_time"], 0.01)

if __name__ == '__main__':
    unittest.main()
