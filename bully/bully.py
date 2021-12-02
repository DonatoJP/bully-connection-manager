from connections_manager import ConnectionsManager

class Bully:
    def __init__(self, connection_manager: ConnectionsManager, peer_hostnames: "list[str]" ) -> None:
        self.conn_manager = connection_manager
        self.peer_hostnames = peer_hostnames
        