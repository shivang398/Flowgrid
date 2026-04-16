import socket
import time
from typing import Any, Optional, Dict, List

from common import Message, MessageType, TaskStatus, get_logger
from common.network.connection import TcpConnection

logger = get_logger("client")

class RayClient:
    """
    Client for interacting with the Flowgrid cluster.
    Provides methods for task submission and result retrieval.
    """
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._conn: Optional[TcpConnection] = None

    def connect(self):
        """Establishes a physical TCP connection to the Master."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            self._conn = TcpConnection(sock)
            logger.info(f"Connected to Flowgrid Master at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Master: {e}")
            raise ConnectionError(f"Could not connect to Master at {self.host}:{self.port}") from e

    def disconnect(self):
        """Closes the current connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Disconnected from Master.")

    def submit_task(self, function_name: str, *args: Any, **kwargs: Any) -> str:
        """
        Submits a task to the cluster and returns the unique task_id.
        """
        if not self._conn:
            self.connect()

        payload = {
            "function_name": function_name,
            "args": list(args),
            "kwargs": kwargs
        }
        msg = Message(type=MessageType.SUBMIT_TASK, payload=payload)
        
        try:
            self._conn.send_message(msg)
            response = self._conn.recv_message()
            
            if response and response.type == MessageType.ACK:
                task_id = response.payload.get("task_id")
                logger.info(f"Task submitted successfully. ID: {task_id}")
                return task_id
            else:
                error_msg = response.payload.get("error", "Unknown error during submission") if response else "No response from Master"
                raise RuntimeError(f"Task submission failed: {error_msg}")
        except Exception as e:
            logger.error(f"Error during task submission: {e}")
            self.disconnect()
            raise

    def get_result(self, task_id: str, wait: bool = True, poll_interval: float = 1.0) -> Any:
        """
        Fetches the result for a given task_id.
        If wait is True, polls until the task is completed or failed.
        """
        if not self._conn:
            self.connect()

        while True:
            msg = Message(type=MessageType.GET_RESULT, task_id=task_id)
            try:
                self._conn.send_message(msg)
                response = self._conn.recv_message()
                
                if not response:
                    raise RuntimeError("Received empty response from Master")

                status = response.payload.get("status")
                result = response.payload.get("result")
                error = response.payload.get("error")

                if status == TaskStatus.COMPLETED:
                    return result
                elif status == TaskStatus.FAILED:
                    raise RuntimeError(f"Task {task_id} failed with error: {error or result}")
                elif status == "ERROR":
                    raise RuntimeError(f"Master reported internal error for task {task_id}: {error}")
                
                if not wait:
                    return None
                
                logger.debug(f"Task {task_id} is still {status}. Retrying in {poll_interval}s...")
                time.sleep(poll_interval)
                
            except Exception as e:
                logger.error(f"Error while fetching result for {task_id}: {e}")
                self.disconnect()
                raise
