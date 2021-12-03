from connections_manager import ConnectionsManager
from threading import Thread
import time

class Bully:
    def __init__(self, 
        connection_manager: ConnectionsManager,
        peer_hostnames: "list[str]" 
    ) -> None:
        self.conn_manager = connection_manager
        self.peer_hostnames = peer_hostnames
        self.listen_threads = []
        self.is_in_election = False
        self.received_ok = False
        self.leader_addr = None
        self.is_leader = False

        for ph in peer_hostnames:
            th = Thread(target=self.start_receiving_from_peer, args=(ph,))
            th.daemon = True
            th.start()
            self.listen_threads.append(th)
    
    def begin_election_process(self):
        if self.is_in_election: return
        self.is_in_election = True

        self.conn_manager.send_to_mayors('ELECTION')
        time.sleep(5)
        if not self.received_ok:
            self.proclaim_leader()
    
    def proclaim_leader(self):
        print('I am the new LEADER !')
        self.conn_manager.send_to_all('LEADER')
        self.is_leader = True
        self.leader_addr = None

    def process_election_message(self, peer_addr):
        print(f'Received ELECTION message from {peer_addr}')
        
        # Responder con OK y comenzar proceso de eleccion
        self.conn_manager.send_to(peer_addr, 'OK')
        self.begin_election_process()

    def process_ok_message(self, peer_addr):
        print(f'Received OK message from {peer_addr}')
        self.received_ok = True

    def process_leader_message(self, peer_addr):
        print(f'Received LEADER message from {peer_addr}')
        self.leader_addr = peer_addr
        self.is_leader = False

        print(f'My new LEADER is now {self.leader_addr} !!')
        self.reset_election_variables()

    def reset_election_variables(self):
        self.is_in_election = False
        self.received_ok = False

    def start_receiving_from_peer(self, peer_addr):
        print(f'Starting to receive from {peer_addr}')
        while True:
            msg = self.conn_manager.recv_from(peer_addr)
            if msg == 'ELECTION':
                self.process_election_message(peer_addr)
            elif msg == 'OK':
                self.process_ok_message(peer_addr)
            elif msg == 'LEADER':
                self.process_leader_message(peer_addr)


        