import unittest
import socket
import struct
import time
import threading
from common import Message, MessageType, Task, TaskStatus, current_timestamp, serialize
from common.network.connection import TcpConnection
from master.result_manager import ResultManager
from master.worker_manager import WorkerManager
from master.queue import TaskQueue

class TestNetworkChaos(unittest.TestCase):
    def test_byte_by_byte_chunking(self):
        """Tests if TcpConnection is robust enough to rebuild a message sent 1 byte at a time."""
        server_sock, client_sock = socket.socketpair()
        
        server_conn = TcpConnection(server_sock)
        
        msg = Message(type=MessageType.HEARTBEAT, payload={"id": "chunk_test", "data": "X" * 100})
        payload = serialize(msg).encode('utf-8')
        length = len(payload)
        header = struct.pack('>I', length)
        full_data = header + payload
        
        def send_slowly():
            for i in range(len(full_data)):
                client_sock.send(full_data[i:i+1])
                # No sleep needed really, just tiny sends to force recv() fragments
            client_sock.close()

        sender_thread = threading.Thread(target=send_slowly)
        sender_thread.start()
        
        received = server_conn.recv_message()
        self.assertIsNotNone(received)
        self.assertEqual(received.payload["id"], "chunk_test")
        self.assertEqual(len(received.payload["data"]), 100)
        
        sender_thread.join()
        server_conn.close()

    def test_malformed_header(self):
        """Tests that a massive/invalid length header doesn't crash the receiver."""
        server_sock, client_sock = socket.socketpair()
        server_conn = TcpConnection(server_sock)
        
        # Send a length header that claims 1GB of data
        client_sock.send(struct.pack('>I', 1024 * 1024 * 1024))
        
        # We expect recv_message to eventually return None or handle it if we add a cap.
        # Currently, TcpConnection._recv_all(n) will try to block.
        # If we didn't add a cap, it might try to allocate 1GB.
        # Let's see how it behaves. (In a real system, we'd have a cap).
        
        # We'll set a timeout so the test doesn't hang forever
        server_sock.settimeout(0.1)
        received = server_conn.recv_message()
        self.assertIsNone(received)
        
        server_conn.close()
        client_sock.close()

class TestLogicChaos(unittest.TestCase):
    def setUp(self):
        self.result_mgr = ResultManager()
        self.worker_mgr = WorkerManager()
        self.task_queue = TaskQueue()

    def test_late_result_idempotency(self):
        """
        Scenario:
        1. Task is RUNNING.
        2. Task times out (it's marked as FAILED/PENDING internally for retry).
        3. Master accepts a new worker or retry.
        4. Original worker finally sends a result for the OLD task.
        5. Goal: Ensure terminal state (COMPLETED) is preserved or 
                 late results are ignored if task was already marked for retry.
        """
        task = Task(id="race_task", function_name="compute")
        self.result_mgr.register_task(task)
        
        # 1. Start task
        task.mark_running("worker_1")
        
        # 2. Simulate logic for retry (Master marks it as failed due to timeout)
        # Note: FaultToleranceManager calls mark_failed(reason)
        task.mark_failed("Timeout")
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertEqual(task.retries, 1)
        
        # 3. Original worker sends result for the task
        # ResultManager.store_result should still store it if it's PENDING (for consistency)
        # BUT if it was COMPLETED, it should ignore.
        self.result_mgr.store_result("race_task", 42)
        
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.result, 42)
        
        # 4. Now a "Late" result arrives after completion
        # Let's say it was retried and COMPLETED already.
        # (Simulated by calling store_result again)
        self.result_mgr.store_result("race_task", 99) # This should be ignored
        self.assertEqual(task.result, 42)

if __name__ == "__main__":
    unittest.main()
