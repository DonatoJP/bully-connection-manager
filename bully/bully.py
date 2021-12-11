from typing import Callable
from connections_manager import ConnectionsManager
from .events_enum import Event
from threading import Thread, Condition, Lock
import time
import logging




class Bully:
    def __init__(self, 
        connection_manager: ConnectionsManager,
        peer_hostnames: "list[str]" 
    ) -> None:
        self.conn_manager = connection_manager
        self.peer_hostnames = peer_hostnames
        self.threads = []
        self.callbacks = {}

        self.is_in_election = False
        self.is_in_election_cv = Condition(Lock())

        self.received_ok = False
        self.received_ok_cv = Condition(Lock())
        
        self.received_ping_echo = False
        self.received_ping_echo_cv = Condition(Lock())

        self.leader_addr = None
        self.leader_addr_cv = Condition(Lock())

        self.is_leader = False
        self.is_leader_cv = Condition(Lock())

        for ph in peer_hostnames:
            th = Thread(target=self._start_receiving_from_peer, args=(ph,))
            th.daemon = True
            th.start()
            self.threads.append(th)
        
        ping_thread = Thread(target=self._poll_leader)
        ping_thread.daemon = True
        ping_thread.start()
        self.threads.append(ping_thread)

    def _poll_leader(self):
        """
            Private function. If the node is not the leader, it sends PING messages expecting to receive
            an answer. If not, the node begins with the election process.
        """
        while True:
            self.is_leader_cv.acquire()
            self.is_in_election_cv.acquire()
            self.leader_addr_cv.acquire()

            if not self.is_leader and not self.is_in_election and self.leader_addr is not None:
                self.is_leader_cv.release()
                self.is_in_election_cv.release()
                
                self.conn_manager.send_to(self.leader_addr, 'PING')
                self.leader_addr_cv.release()

                received_ping_echo = self.wait_get_received_ping_echo(3)
                if not received_ping_echo:
                    logging.info(f'I detect that LEADER is down. Beggining with election process...')
                    self.begin_election_process()
            else: 
                self.is_leader_cv.release()
                self.is_in_election_cv.release()
                self.leader_addr_cv.release()
            
            self.set_received_ping_echo(False)
            time.sleep(5)

    def get_is_leader(self):
        """
            Returns boolean indicating if this node is the leader in the cluster.
        """
        self.is_leader_cv.acquire()
        is_leader = self.is_leader
        self.is_leader_cv.release()
        return is_leader

    def set_is_leader(self, value):
        self.is_leader_cv.acquire()
        self.is_leader = value
        self.is_leader_cv.release()

    def get_leader_addr(self):
        """
            Returns the address of the leader.
        """
        self.leader_addr_cv.acquire()
        leader_addr = self.leader_addr
        self.leader_addr_cv.release()
        return leader_addr

    def set_leader_addr(self, value):
        self.leader_addr_cv.acquire()
        self.leader_addr = value
        self.leader_addr_cv.release()

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
        """
            Executes wait on received_ok condvar for 'timeout' seconds. Then, it returns the value of
            received_ok
        """
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
        """
            Set a new value for received_ok and notifies all threads waiting for a change on it.
        """
        self.received_ok_cv.acquire()
        self.received_ok = value
        self.received_ok_cv.notify_all()
        self.received_ok_cv.release()

    def wait_get_received_ping_echo(self, timeout):
        """
            Executes wait on received_ping_echo condvar for 'timeout' seconds. Then, it returns the value of
            received_ping_echo
        """
        self.received_ping_echo_cv.acquire()
        self.received_ping_echo_cv.wait(timeout)
        received_ping_echo = self.received_ping_echo
        self.received_ping_echo_cv.release()
        return received_ping_echo
    
    def notify_set_received_ping_echo(self, value):
        """
            Set a new value for received_ping_echo and notifies all threads waiting for a change on it.
        """
        self.received_ping_echo_cv.acquire()
        self.received_ping_echo = value
        self.received_ping_echo_cv.notify_all()
        self.received_ping_echo_cv.release()
    
    def get_received_ping_echo(self):
        self.received_ping_echo_cv.acquire()
        received_ping_echo = self.received_ping_echo
        self.received_ping_echo_cv.release()
        return received_ping_echo
    
    def set_received_ping_echo(self, value):
        self.received_ping_echo_cv.acquire()
        self.received_ping_echo = value
        self.received_ping_echo_cv.release()

    def begin_election_process(self):
        """
            Begins with the leader election process. It may execute the ELECTION_STARTED callback,
            if it is set
        """
        if self.get_is_in_election(): return
        self.set_is_in_election(True)

        if Event.ELECTION_STARTED in self.callbacks:
            self.callbacks[Event.ELECTION_STARTED]()

        self.conn_manager.send_to_higher('ELECTION')
        received_ok = self.wait_get_received_ok(5) # TODO: Timeout as env var

        if not received_ok:
            self._proclaim_leader()
    
    def _proclaim_leader(self):
        """
            Private function. Proclaims the node as the leader in the cluster.
        """
        logging.info('I am the new LEADER !')
        self.conn_manager.send_to_all('LEADER')
        self.set_is_leader(True)
        self.set_leader_addr(None)

        self._reset_election_variables()

        if Event.NEW_LEADER in self.callbacks:
            self.callbacks[Event.NEW_LEADER]()

    def _process_election_message(self, peer_addr):
        """
            Process message of type ELECTION. It sends "Ok" to the sender and starts new election process
        """
        logging.info(f'Received ELECTION message from {peer_addr}')
        
        # Responder con OK y comenzar proceso de eleccion
        self.conn_manager.send_to(peer_addr, 'OK')
        self.begin_election_process()

    def _process_ok_message(self, peer_addr):
        """
            Process message of type OK. It notify the reception of the OK message to other threads
        """
        logging.info(f'Received OK message from {peer_addr}')
        self.notify_set_received_ok(True)

    def _process_leader_message(self, peer_addr):
        """
            Process message of type LEADER. Configures new leader address. Executes callback NEW_LEADER, if set.
        """
        logging.info(f'Received LEADER message from {peer_addr}')
        self.set_leader_addr(peer_addr)
        self.set_is_leader(False)

        logging.info(f'My new LEADER is now {peer_addr} !!')
        self._reset_election_variables()

        if Event.NEW_LEADER in self.callbacks:
            self.callbacks[Event.NEW_LEADER]()

    def _reset_election_variables(self):
        self.set_is_in_election(False)
        self.set_received_ok(False)
    
    def _echo_ping(self, peer_addr):
        """
            Process message of type PING. Answers with ECHO_PING
        """
        self.conn_manager.send_to(peer_addr, 'ECHO_PING')

    def _start_receiving_from_peer(self, peer_addr):
        logging.info(f'Starting to receive from {peer_addr}')
        while True:
            msg = self.conn_manager.recv_from(peer_addr)
            if msg == 'ELECTION':
                self._process_election_message(peer_addr)
            elif msg == 'OK':
                self._process_ok_message(peer_addr)
            elif msg == 'LEADER':
                self._process_leader_message(peer_addr)
            elif msg == 'PING':
                self._echo_ping(peer_addr)
            elif msg == 'ECHO_PING':
                self.notify_set_received_ping_echo(True)

    def set_callback(self, event: Event, callback: Callable):
        """
            Configures callback on event. Event must be type Event from events_enum:

            Event.NEW_LEADER
            Event.ELECTION_STARTED
            Event.LEADER_DOWN
        """
        self.callbacks[event] = callback


        