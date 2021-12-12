from multiprocessing import process
from connections_manager import ConnectionsManager
import os, signal, sys, time

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
    cm.send_to_all(f'Hola 1 desde {port_n} !!')
    cm.send_to_all(f'Hola 2 desde {port_n} !!')
    for peer in peer_addrs:
        peer_addr = peer.split(':')[0].split('-')[1]
        received = cm.recv_from(peer_addr)
        print(f'Received from {peer_addr}: {received}')
        received = cm.recv_from(peer_addr)
        print(f'Received from {peer_addr}: {received}')
    cm._join_listen_thread()

if __name__ == '__main__':
    main()
