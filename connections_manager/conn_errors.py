class PeerDownError(Exception):
    """Raised when trying to use a Socket that belongs to a closed connection"""
    pass