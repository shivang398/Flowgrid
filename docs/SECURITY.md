# Flowgrid Security Overview

Flowgrid Enterprise is designed with multi-tenant security and Role-Based Access Control (RBAC).

## 🛡️ Authentication Architecture

Flowgrid uses a custom challenge-response handshake over TCP. 

1. **Handshake**: Clients must send an `AUTH` message containing a valid API Key.
2. **Persistence**: Once a connection is authenticated, the socket is tagged with a `Role`.
3. **Authorization**: Every subsequent message (e.g., `SUBMIT_TASK`) is checked against the role's permission matrix.

## 👥 Default Roles

| Role | Permissions | Use Case |
| :--- | :--- | :--- |
| `admin` | Full Access | Cluster management & system tasks |
| `user` | `SUBMIT_TASK`, `GET_RESULT` | Standard developer workloads |
| `read_only` | `GET_CLUSTER_STATS` | Monitoring dashboards |

## 🔑 Key Management Best Practices

### Environment Variables
Avoid hardcoding API keys. Use environment variables on both Master and Client:

**Master Node:**
```bash
export FLOWGRID_ADMIN_KEY="your-complex-secret"
python3 -m master.master
```

**Client Side:**
```python
import os
from client.flowgrid_client import FlowgridClient

key = os.getenv("FLOWGRID_API_KEY")
client = FlowgridClient("localhost", 9999)
client.connect()
client.authenticate(key)
```

### Docker Security
Flowgrid runs tasks in Docker containers for isolation. 
- **User Namespaces**: We recommend running the worker with restricted Docker socket permissions.
- **Resource Limits**: The Scheduler enforces CPU/RAM limits to prevent "noisy neighbor" scenarios in a shared cluster.

## 🛡️ Reporting Vulnerabilities
Please report any security issues to security@flowgrid.io.
