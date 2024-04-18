from dataclasses import dataclass


@dataclass
class Connection:
    """
    A base connection object used by jobs to obtain job specific connection configuration attributes
    """
    id: str
