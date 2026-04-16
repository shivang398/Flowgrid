import socket
import time
from typing import Optional
from common import get_logger, Message
from common.network.connection import TcpConnection

logger = get_logger("worker.network")

class WorkerNetworkClient:
    """
    Handles the persistent TCP connection to the Master from the worker side.
    Includes robust reconnection logic with exponential backoff.
    """
    def __init__(self, master_host: str, master_port: int):
        self.master_host = master_host
        self.master_port = master_port
        
        self._conn: Optional[TcpConnection] = None
        self._running = False

    def connect(self) -> bool:
        """Attempts to establish a connection to the Master."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.master_host, self.master_port))
            self._conn = TcpConnection(sock)
            logger.info(f"Successfully connected to Master at {self.master_host}:{self.master_port}")
            return True
        except socket.error as e:
            logger.error(f"Failed to connect to Master: {e}")
            return False

    def send(self, msg: Message) -> bool:
        """Sends a message to the Master. Returns False if the connection is dead."""
        if not self._conn:
            return False
        try:
            self._conn.send_message(msg)
            return True
        except:
            self._conn = None
            return False

    def receive(self) -> Optional[Message]:
        """Blocks and receives a message. Returns None if disconnected."""
        if not self._conn:
            return None
        msg = self._conn.recv_message()
        if not msg:
            self._conn = None
        return msg

    def ensure_connected(self, max_retries: int = 5, backoff: float = 1.0):
        """Helper to guarantee connectivity before proceeding."""
        retries = 0
        while not self._conn and retries < max_retries:
            if self.connect():
                return True
            retries += 1
            logger.info(f"Retrying connection in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2 # Exponential backoff
        return self._conn is not None

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
