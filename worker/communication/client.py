import time
from typing import Optional, Dict, Any
from common import (
    Message, MessageType, Task, get_logger
)
from ..network.client import WorkerNetworkClient

logger = get_logger("worker.communication")

class WorkerCommunicationClient:
    """
    Handles the logical communication between the Worker and the Master.
    Wraps the WorkerNetworkClient for physical delivery.
    """
    def __init__(self, master_host: str, master_port: int):
        self.net_client = WorkerNetworkClient(master_host, master_port)
        logger.info(f"Worker communication initialized for Master at {master_host}:{master_port}")

    def send_heartbeat(self, worker_id: str, load: int, cpu_usage: float = 0.0, memory_usage: float = 0.0) -> bool:
        """Sends a heartbeat message to the Master."""
        payload = {
            "worker_id": worker_id, 
            "load": load,
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage
        }
        msg = Message(type=MessageType.HEARTBEAT, payload=payload)
        return self.net_client.send(msg)

    def send_result(self, task_id: str, result: Any, is_error: bool = False) -> bool:
        """Reports the outcome of a task back to the Master."""
        payload = {"result": result, "status": "COMPLETED" if not is_error else "FAILED"}
        msg = Message(type=MessageType.RESULT, task_id=task_id, payload=payload)
        return self.net_client.send(msg)

    def poll_task(self, worker_id: str) -> Optional[Task]:
        """
        Polls the Master connection for new tasks.
        """
        msg = self.net_client.receive()
        if not msg:
            return None
            
        if msg.type == MessageType.TASK:
            # Send ACK immediately for reliability
            ack = Message(type=MessageType.ACK, task_id=msg.task_id)
            self.net_client.send(ack)
            
            # 2. Reconstruct Task object from dictionary payload
            payload = msg.payload
            if isinstance(payload, dict):
                return Task(**payload)
            return payload
        
        return None

    def register_with_master(self, worker_id: str) -> bool:
        """Sends a REGISTER message to identify this worker to the Master."""
        if not self.net_client.ensure_connected():
            return False
            
        payload = {"worker_id": worker_id}
        msg = Message(type=MessageType.REGISTER, payload=payload)
        return self.net_client.send(msg)
