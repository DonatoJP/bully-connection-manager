import os
import pickle

filename = "state.txt"


class State:
    _state = {}

    def get(self, key):
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        return self._state[key]

    def set(self, key, val):
        self.lock.acquire()
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        self._state[key] = val
        with open(filename, "wb") as f:
            pickle.dump(self._state, f)
        self.lock.release()

    def set_k(self, key, key2, val):
        self.lock.acquire()
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        self._state[key][key2] = val
        with open(filename, "wb") as f:
            pickle.dump(self._state, f)
        self.lock.release()

    def remove_k(self, key, key2):
        self.lock.acquire()
        with open(filename, "rb") as f:
            self._state = pickle.load(f)
        del self._state[key][key2]
        with open(filename, "wb") as f:
            pickle.dump(self._state, f)
        self.lock.release()

    def init(self, lock):
        if os.stat(filename).st_size == 0:
            with open(filename, "wb") as f:
                pickle.dump({"coordinator": {}}, f)
        self.lock = lock
