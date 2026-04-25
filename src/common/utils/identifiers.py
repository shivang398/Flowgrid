import uuid
import hashlib
from typing import Any

def generate_uuid() -> str:
    """Generates a standard UUID4 string."""
    return str(uuid.uuid4())

def generate_idempotency_key(*args: Any, **kwargs: Any) -> str:
    """
    Generates a deterministic idempotency key based on function arguments.
    By hashing the arguments, we can identify identical task submissions.
    """
    # Simple deterministic str conversion (for complex types, serialization is typically needed)
    val_str = f"args:{args}-kwargs:{sorted(kwargs.items())}"
    return hashlib.sha256(val_str.encode('utf-8')).hexdigest()
