from dataclasses import dataclass

from connections.models import Connection


@dataclass
class SharePointConnection(Connection):
    client_id: str
    client_secret: str
    tenant_id: str
    tenant_name: str
    site: str
