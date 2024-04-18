from dataclasses import dataclass

from connections.models import Connection


@dataclass
class BwfConnection(Connection):
    RECORD_DEFINITION = 'com.bmc.dsom.hgm:Connection_BWF'
    FIELD_USER = 490000280
    user: str | None
