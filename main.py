from bully import Bully, Event
from connections_manager import ConnectionsManager
from coordinator.state import State
import os, signal, sys, time, logging, threading
from coordinator.server import UdpServer
from coordinator.reviver import Reviver
from heartbeat.heartbeat import Heartbeat


def new_leader_callback():
    logging.info("CALLBACK: NEW_LEADER")


def election_callback():
    logging.info("CALLBACK: ELECTION_STARTED")


state = State()
state.init(threading.Lock())

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


def main():
    threads = []

    heartbeat_t = Heartbeat()
    heartbeat_t.start()
    threads.append(heartbeat_t)

    udp_server = UdpServer(state)  # threading.Thread(target=server.run, args=(state,))
    udp_server.start()
    threads.append(udp_server)

    port_n = os.environ["LISTEN_PORT"]
    node_id = os.environ["NODE_ID"]
    peer_addrs = [
        addr
        for addr in os.environ["PEERS_INFO"].split(",")
        if not f"{node_id}-" in addr
    ]
    logging.info(
        f"Starting node {node_id} with LISTEN_PORT={port_n} and PEERS_INFO={peer_addrs}"
    )
    cm = ConnectionsManager(node_id, port_n, peer_addrs)

    def __exit_gracefully(*args):
        print("Received SIGTERM signal. Starting graceful exit...")
        cm.shutdown_connections()
        sys.exit(0)

    signal.signal(signal.SIGTERM, __exit_gracefully)

    time.sleep(5)
    peer_hostnames = list(map(lambda x: x.split(":")[0].split("-")[1], peer_addrs))
    bully = Bully(cm, peer_hostnames)

    bully.set_callback(Event.NEW_LEADER, new_leader_callback)
    bully.set_callback(Event.ELECTION_STARTED, election_callback)

    bully.begin_election_process()

    state_checker = Reviver(state, bully)
    state_checker.start()
    threads.append(state_checker)

    [t.join() for t in threads]

    cm._join_listen_thread()


if __name__ == "__main__":
    main()
