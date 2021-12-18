from multiprocessing import process
from bully import Bully, Event
from bully.bully_manager import BullyManager
from connections_manager import ConnectionsManager
import os, signal, sys, time, socket, logging

def new_leader_callback():
    logging.info('CALLBACK: NEW_LEADER')

def election_callback():
    logging.info('CALLBACK: ELECTION_STARTED')

def configure_logger():
    FORMAT = '%(asctime)s | %(message)s'
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format=FORMAT)


def main():
    port_n = os.environ['LISTEN_PORT']
    peer_addrs = os.environ['PEERS_INFO'].split(',')
    node_id = os.environ['NODE_ID']
    configure_logger()

    logging.info(f'Starting node {node_id} with LISTEN_PORT={port_n} and PEERS_INFO={peer_addrs}')
    bully = BullyManager(node_id, peer_addrs, port_n)
    bully.start()
    def __exit_gracefully(*args):
        print("Received SIGTERM signal. Starting graceful exit...")
        bully.shutdown_connections()
        sys.exit(0)

    signal.signal(signal.SIGTERM, __exit_gracefully)

    bully.set_callback(Event.NEW_LEADER, new_leader_callback)
    bully.set_callback(Event.ELECTION_STARTED, election_callback)

    bully._join_listen_thread()


if __name__ == '__main__':
    main()
