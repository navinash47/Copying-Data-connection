from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

from utils.text_utils import clean_text


def convert_confluence_date(input_date: str) -> datetime | None:
    if input_date is None:
        return None
    return datetime.strptime(str(input_date), '%Y-%m-%dT%H:%M:%S.%f%z')


@dataclass
class ConfluencePage:
    id_: str
    title: str
    content: str
    space_name: str
    base_url: str
    web_url: str
    last_modified: datetime

    @staticmethod
    def from_json_dict(json_dict: Dict[str, Any]) -> 'ConfluencePage':
        id_ = json_dict.get('id')
        title = clean_text(json_dict.get('title'))
        json_space = json_dict.get('space')
        space_name = json_space.get('name')
        json_body = json_dict.get('body')
        json_storage = json_body.get('storage')
        content = clean_text(str(json_storage.get('value')))
        json_version = json_dict.get('version')
        str_page_last_modified = json_version.get('when')
        last_modified = convert_confluence_date(str_page_last_modified)
        json_link = json_dict.get('_links')
        base_url = json_link.get('base')
        web_url = base_url + json_link.get('webui')
        return ConfluencePage(
            id_=id_,
            title=title,
            content=content,
            last_modified=last_modified,
            base_url=base_url,
            space_name=space_name,
            web_url=web_url
        )


@dataclass
class AttachmentMetaData:
    id_: str
    title: str
    source: str
    mime_type: str
    status: str
    download_link: str
    web_url: str
    last_modified: datetime

    @staticmethod
    def from_json_dict(json_dict: Dict[str, Any], page: ConfluencePage) -> 'AttachmentMetaData':
        id_ = json_dict.get('id')
        title = json_dict.get('title')
        source = f"{page.space_name}/{page.id_}/{id_}"
        status = json_dict.get('status')
        json_link = json_dict.get('_links')
        download_link = json_link.get('download')
        web_url = page.base_url + json_link.get('webui')
        metadata = json_dict.get('metadata')
        mime_type = metadata.get('mediaType')
        version_link = json_dict.get('version')
        str_page_last_modified = version_link.get('when')
        last_modified = convert_confluence_date(str_page_last_modified)
        return AttachmentMetaData(
            id_=id_,
            title=title,
            source=source,
            mime_type=mime_type,
            status=status,
            download_link=download_link,
            web_url=web_url,
            last_modified=last_modified,
        )
