from connections_manager import ConnectionsManager
from threading import Thread, Condition, Lock
import time

class Bully:
    def __init__(self, 
        connection_manager: ConnectionsManager,
        peer_hostnames: "list[str]" 
    ) -> None:
        self.conn_manager = connection_manager
        self.peer_hostnames = peer_hostnames
        self.threads = []

        self.is_in_election = False
        self.is_in_election_cv = Condition(Lock())

        self.received_ok = False
        self.received_ok_cv = Condition(Lock())

        self.leader_addr = None

        self.is_leader = False

        for ph in peer_hostnames:
            th = Thread(target=self.start_receiving_from_peer, args=(ph,))
            th.daemon = True
            th.start()
            self.threads.append(th)
        
        # ping_thread = Thread(target=self.poll_leader)
        # ping_thread.daemon = True
        # ping_thread.start()
        # self.threads.append(ping_thread)

    # def poll_leader(self):
    #     while True:
    #         if not self.is_leader and not self.is_in_election:
    #             self.conn_manager.send_to(self.leader_addr, 'PING')

    #         time.sleep(5)

    def get_is_in_election(self):
        self.is_in_election_cv.acquire()
        is_in_election = self.is_in_election
        self.is_in_election_cv.release()
        return is_in_election

    def set_is_in_election(self, value):
        self.is_in_election_cv.acquire()
        self.is_in_election = value
        self.is_in_election_cv.release()

    def get_received_ok(self):
        self.received_ok_cv.acquire()
        received_ok = self.received_ok
        self.received_ok_cv.release()
        return received_ok
    
    def wait_get_received_ok(self, timeout):
        self.received_ok_cv.acquire()
        self.received_ok_cv.wait(timeout)
        received_ok = self.received_ok
        self.received_ok_cv.release()
        return received_ok

    def set_received_ok(self, value):
        self.received_ok_cv.acquire()
        self.received_ok = value
        self.received_ok_cv.release()

    def notify_set_received_ok(self, value):
        self.received_ok_cv.acquire()
        self.received_ok = True
        self.received_ok_cv.notify_all()
        self.received_ok_cv.release()

    def begin_election_process(self):
        if self.get_is_in_election(): return
        self.set_is_in_election(True)

        self.conn_manager.send_to_mayors('ELECTION')
        #time.sleep(5)
        received_ok = self.wait_get_received_ok(5)

        if not received_ok:
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
        self.notify_set_received_ok(True)

    def process_leader_message(self, peer_addr):
        print(f'Received LEADER message from {peer_addr}')
        self.leader_addr = peer_addr
        self.is_leader = False

        print(f'My new LEADER is now {self.leader_addr} !!')
        self.reset_election_variables()

    def reset_election_variables(self):
        self.set_is_in_election(False)
        self.set_received_ok(False)


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


        