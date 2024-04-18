from typing import List

from requests import HTTPError

from config import Settings
from connections.hkm.constants import PAGE_SIZE
from connections.hkm.models import HkmConnection
from connections.hkm.schemas import HkmArticle, HkmResults, decode_hkm_article_from_json_dict
from connections.service import ConnectionLoader
from helixplatform import ar_core_fields
from helixplatform.models import Record
from helixplatform.service import ArRestClient
from utils.file_types_utils import ContentType
from utils.http_utils import is_content_type_of_mime_type


class HkmConnectionLoader(ConnectionLoader):

    @classmethod
    def get_record_definition_name(cls):
        return HkmConnection.RECORD_DEFINITION

    @classmethod
    def from_record(cls, record: Record) -> HkmConnection:
        return HkmConnection(id=record[ar_core_fields.FIELD_ID], user=record[HkmConnection.FIELD_USER])


class Hkm(ArRestClient):

    def __init__(self, connection: HkmConnection | None):
        super().__init__(
            Settings.HKM_URL,
            Settings.HKM_USER,
            Settings.HKM_PASSWORD,
            impersonated_user=connection.user if connection else None)

    def _get_list_of_content_ids(self, page, page_size) -> HkmResults:
        headers = {'Content-Type': ContentType.APPLICATION_JSON}
        url = "/api/rx/application/knowledge/search?knowledgeStates=Published" \
              f"&pageSize={page_size}&enablePagination=true&pageNumber={page}"
        response = self.session.get(self.build_url(url), headers=headers)
        response.raise_for_status()
        json_response = response.json()
        pages = json_response["totalPages"]
        content_ids = [result["contentId"] for result in json_response["result"]]
        hkm_results = HkmResults(pages=pages, content_ids=content_ids)
        return hkm_results

    def get_article(self, content_id: int) -> HkmArticle | None:
        """
        Returns the specified HKM article or ``None`` if not found.

        :param content_id: content ID of the article
        """
        try:
            headers = {'Content-Type': 'application/json'}
            url = f"/api/rx/application/knowledge/article/{content_id}"
            response = self.session.get(self.build_url(url), headers=headers)
            response.raise_for_status()
            json_response = response.json()
            return decode_hkm_article_from_json_dict(json_response)
        except HTTPError as http_error:
            # The platform API doesn't handle the ComAround 404 correctly and will return a 500 instead.
            # We have to parse the error message to figure out that this is indeed a ComAround 404 error.
            content_type = http_error.response.headers['Content-Type']
            if is_content_type_of_mime_type(content_type, ContentType.APPLICATION_JSON):
                errors_json = http_error.response.json()
                # We try to see if it looks like an IS error
                if isinstance(errors_json, List) and len(errors_json) > 0:
                    first_error = errors_json[0]
                    if (first_error.get('messageType') == 'ERROR' and
                            first_error.get('messageNumber') == 234010 and
                            'appendedText' in first_error and
                            'Failed to get the knowledge article' in first_error['appendedText'] and
                            '404 Not Found' in first_error['appendedText']):
                        return None
            raise http_error

    def get_article_ids(self, content_id: int | None = None) -> set[int]:
        """
        Returns the IDs of the published HKM articles corresponding to the specified criteria.

        :param content_id: if not ``None``, specifies *the* article to return (if it is published).
        """
        if content_id is None:
            return self.__get_article_ids()
        else:
            # Verify the specific article is published:
            article = self.get_article(content_id)
            if article is not None and article.is_published():
                return {content_id}
            return set()

    def __get_article_ids(self) -> set[int]:
        """ Returns the set of *all* HKM article IDs (not just one page) """
        content_ids = set()  # We use a set to ensure there are no duplicates returned.
        results = self._get_list_of_content_ids(1, PAGE_SIZE)
        content_ids.update(results.content_ids)
        if results.pages > 1:
            for page in range(2, results.pages + 1):
                contents = self._get_list_of_content_ids(page, PAGE_SIZE)
                content_ids.update(contents.content_ids)
        return content_ids
