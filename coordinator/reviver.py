import docker
import time 
from datetime import datetime
import logging
import threading
import time


STATE_CHECK_TIME = 2
CHECK_TIME_DIFF = 10
STATUS_RESTART = "restart"
STATUS_INVALID_KEY = "invalid_key"
state = {}

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

def run(state, bully):
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    client = docker.from_env()

    def check_key_value(key, value, now):
        diff = (now - value).total_seconds() 
        logging.info("Key %s, Diff %s",key, diff)
        if diff >CHECK_TIME_DIFF:
            logging.info("Client %s Down, restarting!",key)
            try: 
                c = client.containers.get(key)
                c.restart() 
                return {"key":key, "status": STATUS_RESTART}
            except Exception:
                return {"key":key, "status": STATUS_INVALID_KEY}

            

    def check_state():
        threading.Timer(STATE_CHECK_TIME, check_state).start()

        if bully.get_is_leader():
            now = datetime.now()
            res = [check_key_value(key, value, now) for key, value in state.get("coordinator").items()]
            # logging.debug("RES: %s", res)
            [state.remove_k("coordinator",r["key"]) for r in res if r is not None and r["status"] == STATUS_INVALID_KEY]
            now = datetime.now()
            [state.set_k("coordinator",r["key"], now) for r in res if r is not None and r["status"] == STATUS_RESTART]
        else:
            logging.info("Not Leader, skipping!")



    check_state()
    