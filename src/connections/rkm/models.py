from dataclasses import dataclass
from typing import Any, Dict

from connections.models import Connection


@dataclass
class RkmConnection(Connection):
    RECORD_DEFINITION = 'com.bmc.dsom.hgm:Connection_RKM'
    FIELD_USER = 490000270
    user: str | None


@dataclass
class KnowledgeArticle:
    form: str | None
    fk_guid: str | None
    display_id: str | None
    title: str | None
    company: str | None
    internal: bool | None
    language: str | None

    @staticmethod
    def from_dict(entry: Dict[str, Any]):
        return KnowledgeArticle(
            form=entry['ArticleForm'],
            fk_guid=entry['FK_GUID'],
            title=entry['ArticleTitle'],
            display_id=entry['DocID'],
            internal=(entry['InternalArticleIndication'] == 'Yes'),
            company=entry['Company'],
            language=entry['Language'],
        )

