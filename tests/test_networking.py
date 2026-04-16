import unittest
import socket
import threading
import time
from common import Message, MessageType, Task, Worker
from common.network.connection import TcpConnection
from master.network.server import MasterServer
from master.worker_manager import WorkerManager
from master.result_manager import ResultManager
from worker.network.client import WorkerNetworkClient

class TestFraming(unittest.TestCase):
    def test_length_prefixed_framing(self):
        # Create a pair of connected sockets
        server_sock, client_sock = socket.socketpair()
        
        server_conn = TcpConnection(server_sock)
        client_conn = TcpConnection(client_sock)
        
        msg = Message(type=MessageType.HEARTBEAT, payload={"worker_id": "w1", "load": 5})
        
        # Send from client
        client_conn.send_message(msg)
        
        # Receive at server
        received = server_conn.recv_message()
        
        self.assertEqual(received.type, MessageType.HEARTBEAT)
        self.assertEqual(received.payload["worker_id"], "w1")
        
        server_conn.close()
        client_conn.close()

class TestNetworkingIntegration(unittest.TestCase):
    def setUp(self):
        self.worker_mgr = WorkerManager()
        self.result_mgr = ResultManager()
        self.server = MasterServer(self.worker_mgr, self.result_mgr, host="127.0.0.1", port=0)
        self.server.start()
        
        # Get the actual port assigned by OS
        self.port = self.server._server_sock.getsockname()[1]
        
    def tearDown(self):
        self.server.stop()

    def test_complete_exchange(self):
        client = WorkerNetworkClient("127.0.0.1", self.port)
        self.assertTrue(client.connect())
        
        # 1. Register
        reg_msg = Message(type=MessageType.REGISTER, payload={"worker_id": "w1"})
        client.send(reg_msg)
        
        time.sleep(0.1)
        self.assertIn("w1", self.worker_mgr._workers)
        self.assertIn("w1", self.worker_mgr._connections)
        
        # 2. Assign Task from Master side
        task = Task(id="t1", function_name="compute")
        self.result_mgr.register_task(task)  # MUST register for result tracking
        success = self.worker_mgr.assign_task("w1", task)
        self.assertTrue(success)
        
        # 3. Receive Task at Worker side
        received_msg = client.receive()
        self.assertEqual(received_msg.type, MessageType.TASK)
        self.assertEqual(received_msg.task_id, "t1")
        
        # 4. Send Result back
        res_msg = Message(type=MessageType.RESULT, task_id="t1", payload={"result": 100, "status": "COMPLETED"})
        client.send(res_msg)
        
        time.sleep(0.1)
        self.assertEqual(self.result_mgr.get_result("t1"), 100)
        
        client.close()

if __name__ == '__main__':
    unittest.main()
