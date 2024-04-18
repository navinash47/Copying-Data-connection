from dataclasses import dataclass

from connections.models import Connection


@dataclass
class HkmConnection(Connection):
    RECORD_DEFINITION = 'com.bmc.dsom.hgm:Connection_HKM'
    FIELD_USER = 490000260
    user: str | None
