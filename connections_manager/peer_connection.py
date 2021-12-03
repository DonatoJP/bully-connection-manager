import socket


class PeerConnection:
    def __init__(self, addr, port) -> None:
        self.peer_addr = addr
        self.peer_port = int(port)
        self.peer_conn = None

    def is_peer(self, peer_addr):
        hostname = socket.gethostbyaddr(peer_addr)[0].split('.')[0]
        return self.peer_addr == hostname

    def is_mayor(self, peer_id: int):
        return self.peer_port > peer_id

    def set_connection(self, conn):
        # Closing latest connection
        if self.peer_conn is not None:
            self.peer_conn.close()

        self.peer_conn = conn

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
            self.peer_conn = conn
            print(
                f'[Main Thread] Connection to {peer_host} successfully done!')
        except ConnectionRefusedError as e:
            print(
                f'[Main Thread] Could not connect to {peer_host}. It is not yet active...')

    def recv_message(self):
        if self.peer_conn is None:
            raise Exception("No hay socket")

        # Receive first 4 bytes (len of message)
        msg_len = self._recv(4)

        # Receive Final Message
        msg = self._recv(int.from_bytes(msg_len, byteorder='big'))

        return msg.decode('utf-8')

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
