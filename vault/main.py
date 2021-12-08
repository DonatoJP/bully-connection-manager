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
            return self._get(params)

        if op == "POST":
            key, value = params.split("=", 1)
            return self._post(key, value)

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
            retry = self.vault.leader_get(key)

        return "OK"


def main():
    port_n = os.environ['LISTEN_PORT']
    peer_addrs = os.environ['PEER_ADDRESS'].split(',')
    logging.info(
        f'Starting App with LISTEN_PORT={port_n} and PEER_ADDRESS={peer_addrs}')
    cluster = ConnectionsManager(port_n, peer_addrs)

    def stop_signal_handler(*args):
        cluster.shutdown_connections()

    signal.signal(signal.SIGTERM, stop_signal_handler)
    signal.signal(signal.SIGINT, stop_signal_handler)

    logging.info("Waiting for initialization...")
    time.sleep(5)

    print("Initialization finished!")

    storage_path = os.environ['STORAGE_PATH']
    storage_buckets_number = os.environ['STORAGE_BUCKETS_NUMBER']

    vault = Vault(cluster, storage_path, storage_buckets_number)

    retry_wait = os.environ['RETRY_WAIT']

    message_processor = VaultMessageProcessor(vault, retry_wait)

    rabbit_adress = os.environ['RABBIT_ADDRES']
    input_queue_name = os.environ['INPUT_QUEUE_NAME']
    output_queue_name = os.environ['OUTPUT_QUEUE_NAME']

    server = RabbitConsumerServer(
        rabbit_adress, input_queue_name, output_queue_name, message_processor)

    server.start()


if __name__ == '__main__':
    main()
