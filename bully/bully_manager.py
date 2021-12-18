from threading import Thread
from bully.events_enum import Event
from connections_manager import ConnectionsManager
from . import Bully
import os

class BullyManager(Thread):
    def __init__(self, node_id, peer_hostnames, port_n):
        Thread.__init__(self)
        self.node_id = node_id
        self.peer_hostnames = peer_hostnames
        self.port_n = port_n
        self.bully = None
    
    def run(self) -> None:
        bully_peers = [addr for addr in os.environ['BULLY_PEERS_INFO'].split(
        ',') if not addr.startswith(f"{self.node_id}-")]
        print(bully_peers)

        bully_port = os.environ['BULLY_LISTEN_PORT']
        bully_cm = ConnectionsManager(self.node_id, bully_port, bully_peers)
        self.bully = Bully(bully_cm, self.peer_hostnames)

        return super().run()
    
    def set_callback(self, event: Event, callback) -> None:
        self.bully.set_callback(event, callback)
    
    def shutdown_connections(self):
        self.bully.conn_manager.shutdown_connections()

    def _join_listen_thread(self):
        self.bully.conn_manager._join_listen_thread()
    
    def begin_election_process(self):
        self.bully.begin_election_process()