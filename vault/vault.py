from math import ceil
from socket import timeout

from connections_manager import ConnectionsManager
from multiprocessing.pool import ThreadPool


CLUSTER_SIZE = 5
CLUSTER_QUORUM = ceil(CLUSTER_SIZE / 2)


class Vault:
    """Distributed, replicated, highly available kay-value store"""

    def __init__(self, cluster: ConnectionsManager, cluster_addresses: list[str]):
        self.cluster = cluster
        self.pool = ThreadPool(len(cluster.connections))
        self.cluster_addresses = cluster_addresses

    def validate_key(self, key):
        ILLEGAL_CHARS = ["="]
        if any(c in key for c in ILLEGAL_CHARS):
            raise ValueError("key contains illegal characters")

    def set_leader_addr(self, leader_addr):
        self.leader_addr = leader_addr

    def slave_listen(self):
        while True:
            # Necesitamos un timeout para que cada tanto salga del recv_from y pueda cambiar de leader
            message = self.cluster.recv_from(self.leader_addr)
            if message is None:
                continue

            # Se podria optimizar esto con una pool de workers
            op, params = message.split(" ", 1)
            if op == "GET":
                self._slave_get(params, self.leader_addr)
            elif op == "POST":
                key, value = message.split(":", 1)
                self._slave_post(params, key, value, self.leader_addr)

    def _slave_get(self, key, leader_addr):
        # value = self.storage.get(key)
        value = "TEST"
        try:
            self.cluster.send_to(leader_addr, value)
        except:
            # Leader down, abort operation
            pass

    def _slave_post(self, key, value, leader_addr):
        # self.storage.post(key)
        try:
            self.cluster.send_to(leader_addr, "ACK")
        except:
            # Leader down, abort operation
            pass

    def leader_get(self, key: str) -> tuple[bool, str]:
        """
        gets from vault a value searching by the key
        returns (error, value)

        if error is false and value is none, means that the key was not found in the store
        """
        self.validate_key(key)

        message = f"GET {key}"
        self.cluster.send_to_all(message)
        responses = self._get_responses()

        if len(responses) < CLUSTER_QUORUM:
            return True, None

        parsed_responses = map(lambda res: res.split(':', 1), responses)
        most_updated_value = max(parsed_responses, key=lambda res: res[0])

        return False, most_updated_value[1]

    def leader_post(self, key: str, value: str) -> bool:
        """
        inserts a value by indexed by key on vault
        key must not contain the "=" character
        """
        self.validate_key(key)

        message = f"POST {key}={value}"
        self.cluster.send_to_all(message)
        responses = self._get_responses()

        return len(filter(lambda res: res == "ACK", responses)) >= CLUSTER_QUORUM

    def _get_responses(self):
        return self.pool.map(self.cluster.recv_from, self.cluster.addresses)
