from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

class MessageType(str, Enum):
    TASK = "TASK"
    RESULT = "RESULT"
    HEARTBEAT = "HEARTBEAT"
    ACK = "ACK"
    REGISTER = "REGISTER"
    SUBMIT_TASK = "SUBMIT_TASK"
    GET_RESULT = "GET_RESULT"
    GET_CLUSTER_STATS = "GET_CLUSTER_STATS"

@dataclass
class Message:
    type: MessageType
    task_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Validate and coerce types
        if isinstance(self.type, str) and not isinstance(self.type, MessageType):
            self.type = MessageType(self.type)
            
        if self.type in (MessageType.TASK, MessageType.RESULT) and not self.task_id:
            raise ValueError(f"task_id is required for message type {self.type}")
            
        import dataclasses
        if not isinstance(self.payload, dict) and not dataclasses.is_dataclass(self.payload):
            raise ValueError(f"payload must be a dictionary or dataclass, got {type(self.payload).__name__}")
