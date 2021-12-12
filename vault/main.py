import os
import logging
import signal
import time

from connections_manager import ConnectionsManager

from .server import RabbitMessageProcessor, RabbitConsumerServer
from .vault import Vault


class VaultMessageProcessor(RabbitMessageProcessor):
    def __init__(self, vault: Vault, retry_wait):
        self.vault = vault
        self.retry_wait = retry_wait

    def process(self, message):
        op, params = message.split(" ", 1)

        if op == "GET":
            queue, key = params.split(" ", 1)
            return queue, self._get(key)

        if op == "POST":
            key, value = params.split("=", 1)
            self._post(key, value)
            return None, None

    def _get(self, key):
        retry, value = self.vault.leader_get(key)
        while retry:
            time.sleep(self.retry_wait)
            retry, value = self.vault.leader_get(key)

        return value if value is not None else ""

    def _post(self, key, value):
        retry = self.vault.leader_post(key, value)
        while retry:
            time.sleep(self.retry_wait)
            retry = self.vault.leader_post(key, value)


def leader_start(vault: Vault):
    retry_wait = float(os.environ['RETRY_WAIT'])

    message_processor = VaultMessageProcessor(vault, retry_wait)

    rabbit_adress = os.environ['RABBIT_ADDRESS']
    input_queue_name = os.environ['INPUT_QUEUE_NAME']

    server = RabbitConsumerServer(
        rabbit_adress, input_queue_name, message_processor)

    server.start()


def follower_start(vault: Vault):
    leader_address = os.environ['LEADER_ADDRESS']
    vault.set_leader_addr(leader_address)
    vault.follower_listen()


def main():
    logging.basicConfig(level="INFO")

    port_n = os.environ['LISTEN_PORT']
    peer_addrs = os.environ['PEERS_INFO'].split(',')
    node_id = os.environ['NODE_ID']
    logging.info(
        f'Starting node {node_id} with LISTEN_PORT={port_n} and PEERS_INFO={peer_addrs}')

    cluster = ConnectionsManager(node_id, port_n, peer_addrs)

    def stop_signal_handler(*args):
        cluster.shutdown_connections()

    signal.signal(signal.SIGTERM, stop_signal_handler)
    signal.signal(signal.SIGINT, stop_signal_handler)

    logging.info("Waiting for initialization...")
    time.sleep(5)

    logging.info("Initialization finished!")

    storage_path = os.environ['STORAGE_PATH']
    storage_buckets_number = int(os.environ['STORAGE_BUCKETS_NUMBER'])

    vault = Vault(cluster, storage_path, storage_buckets_number)

    ia_am_leader = os.environ.get('LEADER', False)

    if ia_am_leader:
        leader_start(vault)
    else:
        follower_start(vault)
