import json
import dataclasses
from enum import Enum
from typing import Any

class SafeJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle types that JSON does not support natively,
    such as Dataclasses and Enums, to keep them safe and serializable.
    """
    def default(self, o: Any) -> Any:
        if dataclasses.is_dataclass(o):
            # Include a hint for easier debugging or custom object hooks if desired
            return dataclasses.asdict(o)
        if isinstance(o, Enum):
            return o.value
        # Tuple becomes a list automatically via JSONEncoder but we handle it cleanly
        if isinstance(o, tuple):
            return list(o)
        if isinstance(o, Exception):
            return str(o)
        return super().default(o)

def serialize(obj: Any) -> str:
    """
    Safely serializes a Python object to JSON string.
    Will convert dataclasses, enums, etc.
    """
    return json.dumps(obj, cls=SafeJSONEncoder)

def deserialize(data: str) -> Any:
    """
    Safely deserializes a JSON string to Python primitives (dict, list, str, number, bool, null).
    Note: To reconstruct into dataclasses (e.g. Task), manual unpacking is necessary down the line
    like Task(**payload).
    """
    return json.loads(data)
