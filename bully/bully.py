from connections_manager import ConnectionsManager
from threading import Thread

class Bully:
    def __init__(self, 
        connection_manager: ConnectionsManager,
        peer_hostnames: "list[str]" 
    ) -> None:
        self.conn_manager = connection_manager
        self.peer_hostnames = peer_hostnames
        self.listen_threads = []
        self.is_in_election = False

        for ph in peer_hostnames:
            th = Thread(target=self.start_receiving_from_peer, args=(ph,))
            th.daemon = True
            th.start()
            self.listen_threads.append(th)
    
    def begin_election_process(self):
        self.is_in_election = True
        self.conn_manager.send_to_mayors('ELECTION')
        pass

    def process_election_message(self, peer_addr):
        print(f'Received ELECTION message from {peer_addr}')
        pass

    def process_ok_message(self):
        print('Received OK message')
        pass

    def process_leader_message(self):
        print('Received LEADER message')
        pass

    def start_receiving_from_peer(self, peer_addr):
        while True:
            print(f'Starting to receive from {peer_addr}')
            msg = self.conn_manager.recv_from(peer_addr)
            if msg == 'ELECTION':
                self.process_election_message(peer_addr)
            elif msg == 'OK':
                self.process_ok_message()
            elif msg == 'LEADER':
                self.process_leader_message()


        