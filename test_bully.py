from multiprocessing import process
from bully import Bully, Event
from connections_manager import ConnectionsManager
import os, signal, sys, time, socket

def new_leader_callback():
    print('CALLBACK: NEW_LEADER')

def election_callback():
    print('CALLBACK: ELECTION_STARTED')


def main():
    port_n = os.environ['LISTEN_PORT']
    peer_addrs = os.environ['PEERS_INFO'].split(',')
    node_id = os.environ['NODE_ID']
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

    cm._join_listen_thread()


if __name__ == '__main__':
    main()
