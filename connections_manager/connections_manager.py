import socket
from threading import Thread
from .peer_connection import PeerConnection
from typing import Optional


class ConnectionsManager:
    def __init__(self, node_id: str, self_port_n: str, connections_to_create: list):
        self.connections: list[PeerConnection] = []
        self.node_id = int(node_id)
        self.port_n = int(self_port_n)
        self.listener_stream = None
        self.addresses = connections_to_create

        for c in connections_to_create:
            id_addr, port = c.split(':')
            id, addr = id_addr.split('-')
            if id != node_id:
                self.connections.append(PeerConnection(addr, port, id))

        # Open Listening process
        self.t1 = Thread(target=self._init_listening_port)
        self.t1.daemon = True
        self.t1.start()

        # Begin Connections with active peers
        self._init_peer_connections()

    def _join_listen_thread(self):
        self.t1.join()

    def shutdown_connections(self):
        for conn in self.connections:
            conn.shutdown()

        self.listener_stream.close()

    def _init_peer_connections(self):
        for peer_connection in self.connections:
            peer_connection.init_connection()

    def _init_listening_port(self):
        stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stream.bind(('0.0.0.0', self.port_n))
        stream.listen()
        self.listener_stream = stream
        print(f'[Node {self.node_id} Listener Thread] Begin listening in {self.port_n}')
        while True:
            conn, client_addr = stream.accept()
            peer_connection = self._find_peer(client_addr[0])
            if not peer_connection:
                conn.close()
                continue

            print(
                f'[Node {self.node_id} Listener Thread] Incoming connection request from {socket.gethostbyaddr(client_addr[0])[0].split(".")[0]}')
            peer_connection.set_connection(conn)

    def _find_peer(self, peer_addr) -> Optional[PeerConnection]:
        return next((x for x in self.connections if x.is_peer(peer_addr)), None)

    def send_to(self, peer_addr, message):
        peer = self._find_peer(peer_addr)
        if peer is None:
            raise Exception('Invalid peer address')

        peer.send_message(message)

    def send_to_all(self, message):
        for peer in self.connections:
            peer.send_message(message)

    def recv_from(self, peer_addr) -> str:
        peer = self._find_peer(peer_addr)
        if peer is None:
            raise Exception('Invalid peer address')

        return peer.recv_message()

    def send_to_higher(self, message: str):
        # TODO: Change port_n to conn_id
        higher_peers = filter(lambda pc: pc.is_higher(self.node_id) , self.connections)

        for mp in higher_peers:
            mp.send_message(message)