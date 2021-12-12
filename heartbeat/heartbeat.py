from threading import Thread
import time
from socket import *
import json
import os
import random
import logging

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

HOSTNAME = os.getenv("HOSTNAME", "tp3_heartbeat")
COORDINATOR_HOSTNAME = os.getenv("COORDINATOR_HOSTNAME", "coordinator")
COORDINATOR_PORT = int(os.getenv("COORDINATOR_PORT", 12000))
COORDINATOR_AMOUNT = int(os.getenv("COORDINATOR_AMOUNT", 1))
HEARTBEAT_TIME = 1


class Heartbeat(Thread):
    def __init__(self):
        # Call the Thread class's init function
        Thread.__init__(self)

    def run(self):
        pings = 0
        logging.info(f"Created {HOSTNAME}")
        while True:
            idx = random.randint(1, COORDINATOR_AMOUNT)
            clientSocket = socket(AF_INET, SOCK_DGRAM)
            coordinator_host = f"{COORDINATOR_HOSTNAME}{idx}"
            message = {"host": HOSTNAME, "message": "ping"}
            addr = (coordinator_host, COORDINATOR_PORT)
            logging.info("Sending status to %s", coordinator_host)
            try:
                res = clientSocket.sendto(
                    bytes(json.dumps(message), encoding="utf-8"), addr
                )
                # logging.info("res %s", res)
                pings += 1
                # logging.info("PINGS COUNT %s", pings)
                time.sleep(HEARTBEAT_TIME)
            except Exception:
                logging.info(f"Coordinator <{idx}> unrechable, next random?")
