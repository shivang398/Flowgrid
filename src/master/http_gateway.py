import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from common import get_logger

logger = get_logger("master.http_gateway")

class StatusHandler(BaseHTTPRequestHandler):
    """
    Minimal HTTP handler to serve cluster metrics as JSON.
    """
    def log_message(self, format, *args):
        # Suppress standard logging to keep terminal clean
        return

    def do_GET(self):
        if self.path == '/api/stats':
            self._serve_stats()
        elif self.path == '/api/tasks':
            self._serve_tasks()
        elif self.path == '/' or self.path == '/index.html':
            self._serve_dashboard()
        else:
            self.send_response(404)
            self.end_headers()

    def _serve_stats(self):
        master = self.server.master_node
        
        workers = []
        for wid, wobj in master.worker_manager._workers.items():
            workers.append({
                "id": wid,
                "status": wobj.status.value,
                "load": wobj.load,
                "cpu": wobj.cpu_usage,
                "ram": wobj.memory_usage,
                "last_heartbeat": round(wobj.last_heartbeat, 2) if wobj.last_heartbeat else 0
            })
        
        data = {
            "worker_count": len([w for w in workers if w['status'] != 'OFFLINE']),
            "queue_size": master.task_queue.get_queue_size(),
            "avg_latency": master.result_manager.get_avg_latency(),
            "workers": workers,
            "system_status": "HEALTHY" if len(workers) > 0 else "DEGRADED"
        }
        self._send_json(data)

    def _serve_tasks(self):
        master = self.server.master_node
        tasks = []
        with master.result_manager._lock:
            for tid, t in master.result_manager._tasks.items():
                tasks.append({
                    "id": t.id,
                    "type": "Docker" if t.image else "Python",
                    "status": t.status.value,
                    "image": t.image,
                    "created_at": t.created_at,
                    "depends_on": t.depends_on
                })
        self._send_json(tasks)

    def _send_json(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def _serve_dashboard(self):
        try:
            with open('dashboard/index.html', 'rb') as f:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                self.wfile.write(f.read())
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Dashboard file not found.")

class HttpGateway:
    """
    Wrapper for the HTTP stats server.
    """
    def __init__(self, master_node, port=8080):
        self.master_node = master_node
        self.port = port
        self.server = None
        self._thread = None

    def start(self):
        self.server = HTTPServer(('0.0.0.0', self.port), StatusHandler)
        self.server.master_node = self.master_node
        
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True, name="HTTP-Gateway")
        self._thread.start()
        logger.info(f"Dashboard HTTP Gateway started on port {self.port}")

    def stop(self):
        if self.server:
            self.server.shutdown()
            logger.info("HTTP Gateway stopped.")
