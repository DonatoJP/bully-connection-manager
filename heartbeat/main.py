import heartbeat
import logging
import threading

from heartbeat import Heartbeat

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    logging.info("Main    : before creating thread")
    heartbeat = Heartbeat()
    logging.info("Main    : before running thread")
    heartbeat.start()
    logging.info("Main    : wait for the thread to finish")
    heartbeat.join()
    logging.info("Main    : all done")
