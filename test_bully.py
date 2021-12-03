from multiprocessing import process
from bully import Bully
from connections_manager import ConnectionsManager
import os, signal, sys, time, socket

def main():
    port_n = os.environ['LISTEN_PORT']
    self_hostname = os.environ['HOSTNAME']
    peer_addrs = os.environ['PEER_ADDRESS'].split(',')
    print(f'Starting App with LISTEN_PORT={port_n} and PEER_ADDRESS={peer_addrs}')
    cm = ConnectionsManager(port_n, peer_addrs)

    def __exit_gracefully(*args):
        print("Received SIGTERM signal. Starting graceful exit...")
        cm.shutdown_connections()
        sys.exit(0)

    signal.signal(signal.SIGTERM, __exit_gracefully)

    time.sleep(5)
    peer_hostnames = list(map(lambda x: x.split(':')[0], peer_addrs))
    bully = Bully(cm, peer_hostnames)
    bully.begin_election_process()

    cm._join_listen_thread()


if __name__ == '__main__':
    main()
