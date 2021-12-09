import enum

class Event(enum.Enum):
    NEW_LEADER = 'NEW_LEADER'
    ELECTION_STARTED = 'ELECTION_STARTED'
    LEADER_DOWN = 'LEADER_DOWN'