from dataclasses import dataclass

from connections.models import Connection


@dataclass
class ConfluenceConnection(Connection):
    url: str
    access_token: str
    page_id: str
