import os
import logging
import signal
import time
from threading import Condition, Lock

from connections_manager import ConnectionsManager
from bully import Bully, Event

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


def leader_start(server: RabbitConsumerServer):
    signal_exited = [False]

    def stop_signal_handler(*args):
        signal_exited[0] = True
        server.stop()

    signal.signal(signal.SIGTERM, stop_signal_handler)
    signal.signal(signal.SIGINT, stop_signal_handler)

    server.start()

    return signal_exited[0]


def follower_start(vault: Vault, leader_addr):
    signal_exited = [False]

    def stop_signal_handler(*args):
        signal_exited[0] = True
        vault.follower_stop()

    signal.signal(signal.SIGTERM, stop_signal_handler)
    signal.signal(signal.SIGINT, stop_signal_handler)

    vault.set_leader_addr(leader_addr)
    vault.follower_listen()

    return signal_exited[0]


def main():
    logging.basicConfig(level="INFO")

    node_id = os.environ['NODE_ID']
    logging.info(f'Starting node {node_id}')

    vault_peers = [addr for addr in os.environ['VAULT_PEERS_INFO'].split(
        ',') if not addr.startswith(f"{node_id}-")]
    vault_port = os.environ['VAULT_LISTEN_PORT']
    vault_timeout = int(os.environ['VAULT_TIMEOUT'])
    vault_cm = ConnectionsManager(
        node_id, vault_port, vault_peers, vault_timeout)

    bully_peers = [addr for addr in os.environ['BULLY_PEERS_INFO'].split(
        ',') if not addr.startswith(f"{node_id}-")]
    print(bully_peers)
    bully_port = os.environ['BULLY_LISTEN_PORT']
    bully_cm = ConnectionsManager(node_id, bully_port, bully_peers)

    def stop_signal_handler(*args):
        vault_cm.shutdown_connections()
        bully_cm.shutdown_connections()

    signal.signal(signal.SIGTERM, stop_signal_handler)
    signal.signal(signal.SIGINT, stop_signal_handler)

    logging.info("Waiting for initialization...")

    time.sleep(5)
    logging.info("Initialization finished!")

    storage_path = os.environ['STORAGE_PATH']
    storage_buckets_number = int(os.environ['STORAGE_BUCKETS_NUMBER'])
    vault = Vault(vault_cm, storage_path, storage_buckets_number)

    peer_hostnames = [addr.split("-")[1].split(":")[0]
                      for addr in bully_peers]
    bully = Bully(bully_cm, peer_hostnames)

    retry_wait = float(os.environ['RETRY_WAIT'])

    message_processor = VaultMessageProcessor(vault, retry_wait)

    rabbit_adress = os.environ['RABBIT_ADDRESS']
    input_queue_name = os.environ['INPUT_QUEUE_NAME']

    server = RabbitConsumerServer(
        rabbit_adress, input_queue_name, message_processor)

    i_am_leader = [False]

    started = [False]
    started_cv = Condition(Lock())

    def new_leader_callback():
        print("CALLBACK")
        if started[0]:
            if i_am_leader[0]:
                server.stop()
            else:
                vault.follower_stop()
        else:
            started_cv.acquire()
            started[0] = True
            started_cv.notify_all()
            started_cv.release()

    bully.set_callback(Event.NEW_LEADER, new_leader_callback)

    bully.begin_election_process()

    started_cv.acquire()
    started_cv.wait_for(lambda: started[0])
    started_cv.release()

    exited = False
    while not exited:
        i_am_leader[0] = bully.get_is_leader()
        if i_am_leader[0]:
            exited = leader_start(server)
        else:
            leader_addr = bully.get_leader_addr()
            exited = follower_start(vault, leader_addr)
            logging.info(f"Follower finished: {exited}")

    for thread in bully.threads:
        thread.join()

    vault_cm._join_listen_thread()
    bully_cm._join_listen_thread()
