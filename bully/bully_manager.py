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
        print(self.peer_hostnames)
        bully_port = self.port_n
        
        bully_cm = ConnectionsManager(self.node_id, bully_port, self.peer_hostnames)
        bully_peers = list(map(lambda x: x.split(':')[0].split('-')[1], self.peer_hostnames))
        self.bully = Bully(bully_cm, bully_peers)

        return super().run()
    
    def set_callback(self, event: Event, callback) -> None:
        self.bully.set_callback(event, callback)
    
    def shutdown_connections(self):
        self.bully.conn_manager.shutdown_connections()

    def _join_listen_thread(self):
        self.bully.conn_manager._join_listen_thread()
    
    def begin_election_process(self):
        self.bully.begin_election_process()