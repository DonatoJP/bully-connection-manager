import socket
from .conn_errors import PeerDownError
from threading import Condition, Lock
class PeerConnection:
    def __init__(self, addr, port, node_id) -> None:
        self.peer_addr = addr
        self.peer_port = int(port)
        self.node_id = int(node_id)

        self.peer_conn = None
        self.peer_conn_cv = Condition(Lock())

    def _is_ip(self, ip):
        try:
            socket.inet_aton(ip)
        except socket.error:
            return False

        return True

    def is_peer(self, peer_addr):
        if self._is_ip(peer_addr):
            hostname = socket.gethostbyaddr(peer_addr)[0].split('.')[0]
        else:
            hostname = peer_addr.split(":")[0]
        return self.peer_addr == hostname

    def is_higher(self, peer_id: int):
        return self.node_id > peer_id

    def set_connection(self, conn):
        self.peer_conn_cv.acquire()

        # Closing latest connection
        if self.peer_conn is not None:
            self.peer_conn.close()

        self.peer_conn = conn
        print(f'Setting new connection: {conn}')
        self.peer_conn_cv.notify_all()
        self.peer_conn_cv.release()


    def shutdown(self):
        if self.peer_conn:
            self.peer_conn.close()

    def init_connection(self):
        # Already stablished connection
        if self.peer_conn is not None:
            return

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_host = (self.peer_addr, self.peer_port)
        try:
            conn.connect(peer_host)
            self.set_connection(conn)
            print(
                f'[Main Thread] Connection to {peer_host} successfully done!')
        except ConnectionRefusedError as e:
            print(
                f'[Main Thread] Could not connect to {peer_host}. It is not yet active...')

    def recv_message(self):

        # Receive first 4 bytes (len of message)
        try:
            msg_len = self._recv(4)

            # Receive Final Message
            msg = self._recv(int.from_bytes(msg_len, byteorder='big'))
        except OSError as e:
            print('Connection closed?')
            self.peer_conn_cv.acquire()
            self.peer_conn_cv.wait_for(self.perr_conn_is_valid, None)
            self.peer_conn_cv.release()
            return self.recv_message()
        
        return msg.decode('utf-8')
    
    def perr_conn_is_valid(self):
        return self.peer_conn is not None and self.peer_conn.fileno() != -1

    def send_message(self, msg: str):
        if self.peer_conn is None:
            raise Exception("No hay socket")

        msg_bytes = bytes(msg, 'utf-8')
        msg_len = len(msg_bytes)
        to_send = msg_len.to_bytes(4, byteorder='big') + msg_bytes

        self.peer_conn.sendall(to_send)

    def _recv(self, to_receive: int) -> bytes:
        result = b''
        received = b''
        bytes_read = 0
        while True:
            received += self.peer_conn.recv(to_receive - bytes_read)
            bytes_read += len(received)
            result += received
            if bytes_read >= to_receive:
                break

        return result
