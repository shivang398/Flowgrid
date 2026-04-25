import unittest
import time
import socket
from master.master import Master

class TestMasterLifecycle(unittest.TestCase):
    def test_startup_and_shutdown(self):
        # We use a random port to avoid collisions
        master = Master(host="127.0.0.1", port=0, metrics_port=0, dashboard_port=0)
        
        # 1. Start the master
        master.start()
        
        # Get the actual port
        host, port = master.server._server_sock.getsockname()
        
        # 2. Verify port is listening
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex((host, port))
            self.assertEqual(result, 0, "Master port should be open and accepting connections.")
        
        # 3. Verify subsystems are running
        self.assertTrue(master.scheduler._running)
        self.assertTrue(master.fault_tolerance._running)
        self.assertTrue(master.server._running)
        
        # 4. Stop the master
        master.stop()
        
        # 5. Verify subsystems are stopped
        self.assertFalse(master.scheduler._running)
        self.assertFalse(master.fault_tolerance._running)
        self.assertFalse(master.server._running)
        
        # 6. Verify port is closed (might take a moment for OS to release)
        time.sleep(0.1)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex((host, port))
            self.assertNotEqual(result, 0, "Master port should be closed after stop.")

if __name__ == "__main__":
    unittest.main()
