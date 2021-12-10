from multiprocessing import process
from bully import Bully, Event
from connections_manager import ConnectionsManager
import os, signal, sys, time, socket
import coordinator.server as server
import coordinator.controller as controller
import logging
import threading
import pickle
import heartbeat.heartbeat as heartbeat

def new_leader_callback():
    # is_leader = state["is_leader"]
    print('CALLBACK: NEW_LEADER')

def election_callback():
    print('CALLBACK: ELECTION_STARTED')

filename = "state.txt"

class State:
    _state = {}
    def get(self,key):
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        return self._state[key]

    def set(self, key, val):
        self.lock.acquire()
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        self._state[key] = val
        with open(filename, "wb") as f:
            pickle.dump(self._state, f)
        self.lock.release()


    def set_k(self, key, key2, val):
        self.lock.acquire()
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        self._state[key][key2] = val
        with open(filename, "wb") as f:
            pickle.dump(self._state, f)
        self.lock.release()
    
    def remove_k(self, key, key2):
        self.lock.acquire()
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        del self._state[key][key2]
        with open(filename, "wb") as f:
            pickle.dump(self._state, f)
        self.lock.release()

    
    def init(self, lock):
        # with open(filename, "wb") as f:
        #     pickle.dump(self._state, f)
        self.lock = lock

state = State()
state.init(threading.Lock())
def main():
    port_n = os.environ['LISTEN_PORT']
    node_id = os.environ['NODE_ID']
    peer_addrs = [addr for addr in os.environ['PEERS_INFO'].split(',') if not f"{node_id}-" in addr]
    print(f'Starting node {node_id} with LISTEN_PORT={port_n} and PEERS_INFO={peer_addrs}')
    cm = ConnectionsManager(node_id, port_n, peer_addrs)

    def __exit_gracefully(*args):
        print("Received SIGTERM signal. Starting graceful exit...")
        cm.shutdown_connections()
        sys.exit(0)

    signal.signal(signal.SIGTERM, __exit_gracefully)

    time.sleep(5)
    peer_hostnames = list(map(lambda x: x.split(':')[0].split('-')[1], peer_addrs))
    bully = Bully(cm, peer_hostnames)

    bully.set_callback(Event.NEW_LEADER, new_leader_callback)
    bully.set_callback(Event.ELECTION_STARTED, election_callback)

    bully.begin_election_process()


    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    logging.info("Main    : before creating thread")
    state.set("coordinator", {})
    udp_server = threading.Thread(target=server.run, args=(state,))
    state_checker = threading.Thread(target=controller.run, args=(state,bully))
    heartbeat_t = threading.Thread(target=heartbeat.run)

    logging.info("Main    : before running thread")
    udp_server.start()
    state_checker.start()
    heartbeat_t.start()
    logging.info("Main    : wait for the thread to finish")
    udp_server.join()
    state_checker.join()
    heartbeat_t.join()
    logging.info("Main    : all done")
    cm._join_listen_thread()


if __name__ == '__main__':
    main()
