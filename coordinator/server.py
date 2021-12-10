from socket import *
import json
import sys
import logging
from datetime import datetime


def run(state):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.info("Started Server",)
    serverSocket = socket(AF_INET, SOCK_DGRAM)

    serverSocket.bind(('', 12000))
    bufsize = 1024
    counter = 0
    while True:
        message, address = serverSocket.recvfrom(bufsize)
        data = message.decode('utf-8')
        message_json = json.loads(data)
        host = message_json["host"]
        message = message_json["message"]
        if message == "ping":
            now = datetime.now()
            state.set_k("coordinator", host, now)
        counter += 1
        logging.info("Current counter %s", counter)