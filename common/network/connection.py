import socket
import struct
from typing import Optional
from common import Message, serialize, deserialize, get_logger

logger = get_logger("common.network")

class TcpConnection:
    """
    Handles robust, length-prefixed message exchange over a TCP socket.
    Protocol: [4 bytes Big-Endian Length] [JSON Payload]
    """
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._sock.settimeout(None) # Blocking by default for reliable IO

    def send_message(self, msg: Message):
        """Serializes and sends a message with a 4-byte length prefix."""
        payload = serialize(msg).encode('utf-8')
        length = len(payload)
        header = struct.pack('>I', length) # 4-byte unsigned int, Big-Endian
        
        try:
            self._sock.sendall(header + payload)
        except socket.error as e:
            logger.error(f"Failed to send message: {e}")
            raise

    def recv_message(self) -> Optional[Message]:
        """Blocks and receives exactly one full Message object."""
        try:
            # 1. Read header (4 bytes)
            header = self._recv_all(4)
            if not header:
                return None
            
            length = struct.unpack('>I', header)[0]
            
            # 2. Read payload of specified length
            payload_data = self._recv_all(length)
            if not payload_data:
                return None
                
            payload_str = payload_data.decode('utf-8')
            msg_data = deserialize(payload_str)
            if isinstance(msg_data, dict):
                return Message(**msg_data)
            return None
        except socket.error as e:
            logger.error(f"Socket error during recv: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during message decoding: {e}")
            return None

    def _recv_all(self, n: int) -> Optional[bytes]:
        """Helper to receive exactly n bytes from the socket."""
        data = bytearray()
        while len(data) < n:
            packet = self._sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def close(self):
        """Safely closes the underlying socket."""
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except:
            pass

    @property
    def peer_name(self) -> str:
        try:
            return str(self._sock.getpeername())
        except:
            return "unknown"
