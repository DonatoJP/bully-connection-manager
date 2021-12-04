from math import ceil
from multiprocessing.pool import ThreadPool
from threading import Lock

from connections_manager import ConnectionsManager
from .storage import Storage


class Vault:
    """Distributed, replicated, highly available kay-value store"""

    def __init__(self, cluster: ConnectionsManager, storage_path="/storage", buckets_number=20):
        self.cluster = cluster
        self.pool = ThreadPool(len(cluster.connections))
        self.storage = Storage(storage_path, buckets_number)
        self.leader_addr_lock = Lock()
        self.cluster_quorum = ceil(len(self.cluster.connections) / 2)

    def validate_key(self, key):
        if not key:
            raise ValueError("key must not be empty")
        ILLEGAL_CHARS = ["=", "\r", "\n"]
        if any(c in key for c in ILLEGAL_CHARS):
            raise ValueError("key contains illegal characters")

    def validate_value(self, value):
        if not value:
            raise ValueError("value must not be empty")
        ILLEGAL_CHARS = ["\r", "\n"]
        if any(c in value for c in ILLEGAL_CHARS):
            raise ValueError("value contains illegal characters")

    def set_leader_addr(self, leader_addr):
        with self.leader_addr_lock:
            self.leader_addr = leader_addr

    def slave_listen(self):
        while True:
            # Solo se puede cambiar el leader cuando no hay una operacion siendo procesada
            with self.leader_addr_lock:

                # Necesitamos un timeout para que cada tanto salga del recv_from y pueda cambiar de leader
                message = self.cluster.recv_from(self.leader_addr)
                if message is None:
                    continue

                # Se podria optimizar esto con una pool de workers?
                op, params = message.split(" ", 1)
                if op == "GET":
                    res = self._slave_get(params)
                elif op == "POST":
                    version, rest = params.split(":", 1)
                    key, value = rest.split("=", 1)
                    res = self._slave_post(version, key, value)
                elif op == "VERSION":
                    res = self._slave_version(params)

                try:
                    self.cluster.send_to(self.leader_addr, res)
                except:
                    # Leader down, abort operation
                    pass

    def _slave_get(self, key):
        res = self.storage.get(key)
        if res is None:
            return "0:"

        version, value = res
        return f"{version}:{value}"

    def _slave_post(self, version, key, value):
        self.storage.post(version, key, value)
        return "ACK"

    def _slave_version(self, key):
        return str(self.storage.version(key))

    def leader_get(self, key: str) -> tuple:
        """
        gets from vault a value searching by the key
        returns (retry, value)

        if error is false and value is none, it means that the key was not found in the store
        """
        key = key.strip()
        self.validate_key(key)

        message = f"GET {key}"
        self.cluster.send_to_all(message)
        responses = self._get_responses()
        responses.append(self._slave_get(key))

        if len(responses) < self.cluster_quorum:
            return True, None

        def parse_respone(res):
            version, value = res.split(':', 1)
            return int(version), value

        parsed_responses = map(parse_respone, responses)
        most_recent_value = max(parsed_responses, key=lambda res: res[0])

        if most_recent_value[0] == 0:
            return False, None

        return False, most_recent_value[1]

    def leader_post(self, key: str, value: str) -> bool:
        """
        inserts a value by indexed by key on vault
        key must not contain "=" or newline characters
        value must not contain the newline character
        key will be striped

        returns True if client must retry
        """
        key = key.strip()
        self.validate_key(key)
        self.validate_value(key)

        print("Getting versions")

        message = f"VERSION {key}"
        self.cluster.send_to_all(message)
        responses = self._get_responses()
        responses.append(self._slave_version(key))

        if len(responses) < self.cluster_quorum:
            return True

        print(f"Got responses: {responses}")

        parsed_responses = map(lambda res: int(res), responses)
        next_version = max(parsed_responses) + 1

        print(f"Next version: {next_version}")

        print(f"Executing posts")

        message = f"POST {next_version}:{key}={value}"
        self.cluster.send_to_all(message)
        responses = self._get_responses()
        responses.append(self._slave_post(next_version, key, value))

        print(f"Got responses: {responses}")

        return responses.count("ACK") < self.cluster_quorum

    def _get_responses(self):
        return self.pool.map(self.cluster.recv_from, self.cluster.addresses)
