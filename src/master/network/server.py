import socket
import threading
from typing import Optional

from common import get_logger, MessageType, Message, Task
from common.network.connection import TcpConnection
from common.utils.identifiers import generate_uuid
from master.worker_manager import WorkerManager
from master.result_manager import ResultManager
from master.queue import TaskQueue
from master.auth.manager import AuthManager
from master.metrics import MasterMetrics

logger = get_logger("master.network")

class MasterServer:
    """
    authoritative TCP server for the Flowgrid cluster.
    Manages incoming worker connections and routes messages to internal managers.
    """
    def __init__(
        self, 
        worker_manager: WorkerManager, 
        result_manager: ResultManager,
        task_queue: TaskQueue,
        auth_manager: AuthManager,
        host: str = "0.0.0.0", 
        port: int = 9999
    ):
        self.worker_manager = worker_manager
        self.result_manager = result_manager
        self.task_queue = task_queue
        self.auth_manager = auth_manager
        self.host = host
        self.port = port
        
        self._running = False
        self._server_sock: Optional[socket.socket] = None
        self._threads = []
        self._auth_roles: Dict[TcpConnection, str] = {} # conn -> role

    def start(self):
        """Starts the physical server listener."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(5)
        
        self._running = True
        logger.info(f"Master Server listening on {self.host}:{self.port}")
        
        # Accept loop in a background thread
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True, name="Accept-Thread")
        accept_thread.start()
        self._threads.append(accept_thread)

    def stop(self):
        """Halts the server and closes all active connections."""
        self._running = False
        if self._server_sock:
            self._server_sock.close()
        logger.info("Master Server stopped.")

    def _accept_loop(self):
        while self._running:
            try:
                client_sock, addr = self._server_sock.accept()
                logger.info(f"New physical connection from {addr}")
                
                conn = TcpConnection(client_sock)
                handler = threading.Thread(
                    target=self._handle_worker_connection, 
                    args=(conn,), 
                    daemon=True
                )
                handler.start()
                self._threads.append(handler)
            except socket.error:
                if self._running:
                    logger.error("Accept loop encountered socket error.")
                break

    def _handle_worker_connection(self, conn: TcpConnection):
        """Authoritative lifecycle handler for a single remote worker's connection."""
        worker_id: Optional[str] = None
        
        try:
            while self._running:
                msg = conn.recv_message()
                if not msg:
                    break
                
                if msg.type == MessageType.REGISTER:
                    # Workers are considered 'trusted' for now, or we could auth them too
                    worker_id = msg.payload.get("worker_id")
                    if worker_id:
                        self.worker_manager.register_worker(worker_id)
                        self.worker_manager.register_connection(worker_id, conn)
                        self._auth_roles[conn] = "worker" # Special role
                        logger.info(f"Worker {worker_id} successfully registered via {conn.peer_name}")

                elif msg.type == MessageType.AUTH:
                    # Client authentication
                    api_key = msg.payload.get("api_key")
                    role = self.auth_manager.verify_key(api_key)
                    if role:
                        self._auth_roles[conn] = role
                        conn.send_message(Message(type=MessageType.ACK, payload={"status": "AUTHENTICATED", "role": role}))
                        logger.info(f"Client authenticated as {role} from {conn.peer_name}")
                    else:
                        conn.send_message(Message(type=MessageType.RESULT, payload={"status": "ERROR", "error": "Invalid API Key"}))
                        logger.warning(f"Failed authentication attempt from {conn.peer_name}")
                
                elif msg.type == MessageType.HEARTBEAT:
                    wid = msg.payload.get("worker_id")
                    load = msg.payload.get("load", 0)
                    cpu = msg.payload.get("cpu_usage", 0.0)
                    mem = msg.payload.get("memory_usage", 0.0)
                    gpu = msg.payload.get("gpu_available", False)
                    if wid:
                        self.worker_manager.update_worker_status(wid, load, cpu, mem, gpu_available=gpu)
                
                elif msg.type == MessageType.RESULT:
                    if msg.task_id:
                        result = msg.payload.get("result")
                        status = msg.payload.get("status")
                        is_partial = (status == "PARTIAL")
                        try:
                            self.result_manager.store_result(msg.task_id, result, is_partial=is_partial)
                        except Exception as e:
                            logger.error(f"Failed to store result for {msg.task_id}: {e}")
                
                elif msg.type == MessageType.ACK:
                    logger.debug(f"Received ACK for task {msg.task_id}")

                elif msg.type == MessageType.SUBMIT_TASK:
                    role = self._auth_roles.get(conn)
                    if not self.auth_manager.has_permission(role, "SUBMIT_TASK"):
                        conn.send_message(Message(type=MessageType.RESULT, payload={"status": "ERROR", "error": "Unauthorized"}))
                        continue

                    # Handle client task submission (Python or Docker)
                    payload = msg.payload
                    func_name = payload.get("function_name")
                    image = payload.get("image")
                    command = payload.get("command")
                    args = payload.get("args", [])
                    kwargs = payload.get("kwargs", {})
                    env = payload.get("env", {})
                    needs_gpu = payload.get("needs_gpu", False)
                    depends_on = payload.get("depends_on", [])
                    
                    if func_name or image:
                        task_id = generate_uuid()
                        task = Task(
                            id=task_id, 
                            function_name=func_name, 
                            image=image,
                            command=command,
                            args=args, 
                            kwargs=kwargs,
                            env=env,
                            needs_gpu=needs_gpu,
                            depends_on=depends_on
                        )
                        
                        self.result_manager.register_task(task)
                        self.task_queue.enqueue(task)
                        MasterMetrics.task_submitted()
                        
                        # Respond with task_id
                        response = Message(
                            type=MessageType.ACK, 
                            task_id=task_id, 
                            payload={"status": "ACCEPTED", "task_id": task_id}
                        )
                        conn.send_message(response)
                        logger.info(f"Client submitted task: {task_id} (Type: {'Docker' if image else 'Python'})")

                elif msg.type == MessageType.GET_RESULT:
                    # Handle client result request
                    tid = msg.task_id
                    if tid:
                        try:
                            status = self.result_manager.get_task_status(tid)
                            result = self.result_manager.get_result(tid)
                            
                            response = Message(
                                type=MessageType.RESULT, 
                                task_id=tid, 
                                payload={"status": status, "result": result}
                            )
                            conn.send_message(response)
                            logger.debug(f"Served result for task {tid} (status: {status})")
                        except Exception as e:
                            logger.error(f"Error serving result for {tid}: {e}")
                            conn.send_message(Message(
                                type=MessageType.RESULT, 
                                task_id=tid, 
                                payload={"status": "ERROR", "error": str(e)}
                            ))

                elif msg.type == MessageType.GET_CLUSTER_STATS:
                    role = self._auth_roles.get(conn)
                    if not self.auth_manager.has_permission(role, "GET_CLUSTER_STATS"):
                        conn.send_message(Message(type=MessageType.RESULT, payload={"status": "ERROR", "error": "Unauthorized"}))
                        continue

                    # Provide an overview of the cluster state
                    workers = []
                    for wid, wobj in self.worker_manager._workers.items():
                        workers.append({
                            "id": wid,
                            "status": wobj.status,
                            "load": wobj.load,
                            "last_heartbeat": wobj.last_heartbeat
                        })
                    
                    stats = {
                        "worker_count": len(workers),
                        "queue_size": self.task_queue.get_queue_size(),
                        "workers": workers
                    }
                    
                    conn.send_message(Message(
                        type=MessageType.ACK,
                        payload=stats
                    ))
                    logger.debug("Served cluster stats.")

        except Exception as e:
            logger.error(f"Connection handler fault for {worker_id or 'unknown'}: {e}")
        finally:
            if worker_id:
                logger.warning(f"Worker {worker_id} disconnected.")
                try:
                    worker = self.worker_manager._workers.get(worker_id)
                    if worker:
                        worker.mark_offline()
                except:
                    pass
            conn.close()
