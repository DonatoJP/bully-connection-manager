import docker
import time
from datetime import datetime
import logging
from threading import Thread, Timer
import time


STATE_CHECK_TIME = 2
CHECK_TIME_DIFF = 10
STATUS_RESTART = "restart"
STATUS_INVALID_KEY = "invalid_key"
state = {}


class Reviver(Thread):
    def __init__(self, state, bully):
        # Call the Thread class's init function
        Thread.__init__(self)
        self.state = state
        self.bully = bully

    def thread_function(name):
        logging.info("Thread %s: starting", name)
        time.sleep(2)
        logging.info("Thread %s: finishing", name)

    def check_state(self):
        def check_key_value(key, value, now):
            diff = (now - value).total_seconds()
            logging.info("Key %s, Diff %s", key, diff)
            if diff > CHECK_TIME_DIFF:
                logging.info("Client %s Down, restarting!", key)
                try:
                    c = self.client.containers.get(key)
                    c.restart()
                    return {"key": key, "status": STATUS_RESTART}
                except Exception:
                    return {"key": key, "status": STATUS_INVALID_KEY}

        Timer(STATE_CHECK_TIME, self.check_state).start()

        if self.bully.get_is_leader():
            now = datetime.now()
            res = [
                check_key_value(key, value, now)
                for key, value in self.state.get("coordinator").items()
            ]
            # logging.debug("RES: %s", res)
            [
                self.state.remove_k("coordinator", r["key"])
                for r in res
                if r is not None and r["status"] == STATUS_INVALID_KEY
            ]
            now = datetime.now()
            [
                self.state.set_k("coordinator", r["key"], now)
                for r in res
                if r is not None and r["status"] == STATUS_RESTART
            ]
        else:
            logging.info("Not Leader, skipping!")

    def run(self):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

        self.client = docker.from_env()
        self.check_state()
