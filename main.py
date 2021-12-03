from multiprocessing import process
from connections_manager import ConnectionsManager
import os
import signal
import sys
import time

from vault import Vault


def main():
    port_n = os.environ['LISTEN_PORT']
    peer_addrs = os.environ['PEER_ADDRESS'].split(',')
    print(
        f'Starting App with LISTEN_PORT={port_n} and PEER_ADDRESS={peer_addrs}')
    cm = ConnectionsManager(port_n, peer_addrs)

    def __exit_gracefully(*args):
        print("Received SIGTERM signal. Starting graceful exit...")
        cm.shutdown_connections()
        sys.exit(0)

    signal.signal(signal.SIGTERM, __exit_gracefully)

    time.sleep(5)

    i_am_leader = os.environ.get('LEADER', False)

    vault = Vault(cm)
    if not i_am_leader:
        leader_address = os.environ['LEADER_ADDRESS']
        vault.set_leader_addr(leader_address)
        vault.slave_listen()
    else:
        for i in range(5):
            time.sleep(5)
            vault.leader_post(f"key_{i}", f"Value {i}")

        print(vault.leader_get(f"key_1"))

    cm._join_listen_thread()


if __name__ == '__main__':
    main()
