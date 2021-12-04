from pathlib import Path
import os
from os import path
import re


class Storage:
    def __init__(self, storage_path, buckets=20):
        Path(storage_path).mkdir(parents=True, exist_ok=True)
        self.storage_path = storage_path
        self.buckets = buckets

    def serialize(self, version, key, value):
        return f"{version}:{key}={value}\n"

    def deserialize(self, line):
        match = re.match(r"^(?P<version>\d+):(?P<key>.+)=(?P<value>.+)$", line)
        if match is not None:
            return (int(match['version']), match['key'], match['value'])

    def get(self, key):
        key_hash = self._get_hash(key)
        try:
            with open(path.join(self.storage_path, key_hash), 'r') as file:
                for line in file:
                    match = self.deserialize(line)
                    if match is not None and match[1] == key:
                        return match[0], match[2]
        except FileNotFoundError:
            pass

    def post(self, version, key, value):
        key_hash = self._get_hash(key)

        file_path = path.join(self.storage_path, key_hash)
        # Create file if it does not exists
        with open(file_path, 'a+'):
            pass

        written = False

        with open(path.join(self.storage_path, "temp"), "w") as temp_file:
            with open(file_path, 'r') as key_file:
                for line in key_file:
                    match = self.deserialize(line)
                    if match is not None and match[1] == key:
                        temp_file.write(self.serialize(version, key, value))
                        written = True
                    else:
                        temp_file.write(line)

            if not written:
                temp_file.write(self.serialize(version, key, value))

        os.rename(path.join(self.storage_path, "temp"), file_path)

    def version(self, key):
        key_hash = self._get_hash(key)
        try:
            with open(path.join(self.storage_path, key_hash), 'r') as file:
                for line in file:
                    match = self.deserialize(line)
                    if match is not None and match[1] == key:
                        return match[0]
        except FileNotFoundError:
            return 0

        return 0

    def _get_hash(self, key):
        return str(hash(key) % self.buckets)
