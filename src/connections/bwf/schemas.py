from dataclasses import dataclass
from typing import Any, Dict, List

from .constants import BWF_KNOWLEDGE_BUNDLE


@dataclass
class Content:
    """
    Component object of BwfArticle. Represents a content section.
    Different templates have different such sections.
    """
    label: str
    content: str


@dataclass
class BwfArticle:
    FORM_KNOWLEDGE_ARTICLE_TEMPLATE = f'{BWF_KNOWLEDGE_BUNDLE}:Knowledge Article Template'
    FIELD_ARTICLE_STATUS = 302300500  # ArticleStatus
    ARTICLE_STATUS_PUBLISHED = '5000'
    FIELD_CONTENT_ID = 302300507  # Content ID
    FIELD_COMPANY = 1000000001  # Company
    FIELD_ARTICLE_MODIFIED_DATE = 6  # ModifiedDate

    uuid: str
    content_id: str
    template_name: str
    title: str
    contents: [Content]
    external: bool
    locale: str
    company: str = None

    @staticmethod
    def from_json_dict(json_dict: Dict[str, Any]) -> 'BwfArticle':
        """ Deserializes a JSON Dict object into a returned BwfResults."""
        uuid = json_dict['uuid']
        content_id = json_dict['contentId']
        template_name = json_dict['templateName']
        title = json_dict["title"]
        external = bool(json_dict['external'])
        locale = json_dict['locale']
        contents = [Content(label=content['label'], content=content['content']) for content in json_dict['content']]
        return BwfArticle(
            title=title,
            contents=contents,
            uuid=uuid,
            content_id=content_id,
            template_name=template_name,
            external=external,
            locale=locale)


@dataclass
class BwfResults:
    total_count: int
    uuids: List[str]

    @staticmethod
    def from_json_dict(json_dict: Dict[str, Any]) -> 'BwfResults':
        """ Deserializes a JSON Dict object into a returned BwfResults."""
        total_count = json_dict["totalCount"]
        results = json_dict["results"]
        uuids = [result["uuid"] for result in results]
        return BwfResults(total_count=total_count, uuids=uuids)
