from bully import Bully, Event
from connections_manager import ConnectionsManager
from coordinator.state import State
import os, signal, sys, time, logging, threading
import coordinator.server as server
import coordinator.controller as controller
import heartbeat.heartbeat as heartbeat

def new_leader_callback():
    # is_leader = state["is_leader"]
    print('CALLBACK: NEW_LEADER')

def election_callback():
    print('CALLBACK: ELECTION_STARTED')

state = State()
state.init(threading.Lock())
# state.set("coordinator", {})


format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")


def main():

    port_n = os.environ['LISTEN_PORT']
    node_id = os.environ['NODE_ID']
    peer_addrs = [addr for addr in os.environ['PEERS_INFO'].split(',') if not f"{node_id}-" in addr]
    logging.info(f'Starting node {node_id} with LISTEN_PORT={port_n} and PEERS_INFO={peer_addrs}')
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
