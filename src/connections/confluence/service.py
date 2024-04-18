from typing import List

from atlassian import Confluence

from connections.confluence.models import ConfluenceConnection
from connections.confluence.schemas import AttachmentMetaData, ConfluencePage
from connections.models import Connection
from connections.service import ConnectionLoader
from helixplatform import ar_core_fields
from helixplatform.models import Record


class ConfluenceConnectionLoader(ConnectionLoader):
    FIELD_CONFLUENCE_URL = 490000240
    FIELD_CONFLUENCE_ACCESS_TOKEN = 490000241
    FIELD_CONFLUENCE_PAGE_ID = 490000242

    @classmethod
    def get_record_definition_name(cls):
        return "com.bmc.dsom.hgm:Connection_Confluence"

    @classmethod
    def from_record(cls, record: Record) -> Connection:
        return ConfluenceConnection(
            id=record[ar_core_fields.FIELD_ID],
            url=record[cls.FIELD_CONFLUENCE_URL],
            access_token=record[cls.FIELD_CONFLUENCE_ACCESS_TOKEN],
            page_id=record[cls.FIELD_CONFLUENCE_PAGE_ID]
        )


class ConfluenceService:

    def __init__(self, connection: ConfluenceConnection):
        self.confluence = Confluence(url=connection.url, token=connection.access_token)

    def get_page_with_all_child_ids(self, page_ids: [str]):
        for page_id in page_ids:
            yield page_id
            child_ids = self.confluence.get_child_id_list(page_id)
            if child_ids:
                yield from self.get_page_with_all_child_ids(child_ids)

    def get_page(self, page_id: str) -> ConfluencePage:
        confluence_data_dict = self.confluence.get_page_by_id(page_id, expand='space,body.storage,version')
        return ConfluencePage.from_json_dict(confluence_data_dict)

    def get_page_attachments_metadata(self, page: ConfluencePage) -> List[AttachmentMetaData]:
        page_attachments_data = self.confluence.get_attachments_from_content(
            page.id_, expand='version'
        )
        return [AttachmentMetaData.from_json_dict(attachment, page)
                for attachment in page_attachments_data["results"]]

    def download_attachment(self, attachment_meta_data: AttachmentMetaData, file_path: str):
        download_url = self.confluence.url + attachment_meta_data.download_link
        response = self.confluence._session.get(f"{download_url}")
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(response.content)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.confluence.session.close()
