import hashlib
import os
from typing import Dict, List, Optional
from common import get_logger

logger = get_logger("master.auth")

class AuthManager:
    """
    Handles Role-Based Access Control (RBAC) for the Flowgrid cluster.
    """
    def __init__(self, api_keys: Optional[Dict[str, str]] = None):
        # Format: {hashed_key: role}
        # Roles: 'admin', 'user', 'read_only'
        self._api_keys = api_keys or {}
        
        # If no keys provided, check environment or use a default
        if not self._api_keys:
            admin_key = os.getenv("FLOWGRID_ADMIN_KEY", "flowgrid_admin_123")
            if admin_key == "flowgrid_admin_123":
                logger.warning("Using INSECURE default admin key. Set FLOWGRID_ADMIN_KEY env var!")
            
            hashed = self._hash_key(admin_key)
            self._api_keys[hashed] = "admin"
            logger.info("RBAC Initialized (Admin key loaded from environment/default)")

    def _hash_key(self, key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()

    def verify_key(self, key: str) -> Optional[str]:
        """Returns the role if key is valid, else None."""
        hashed = self._hash_key(key)
        return self._api_keys.get(hashed)

    def has_permission(self, role: str, action: str) -> bool:
        """
        Permissions Matrix:
        - admin: All actions
        - user: SUBMIT_TASK, GET_RESULT
        - read_only: GET_RESULT, GET_CLUSTER_STATS
        """
        if role == "admin":
            return True
            
        permissions = {
            "user": ["SUBMIT_TASK", "GET_RESULT"],
            "read_only": ["GET_RESULT", "GET_CLUSTER_STATS"]
        }
        
        return action in permissions.get(role, [])
