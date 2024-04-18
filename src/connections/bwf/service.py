from datetime import datetime
from typing import List, Callable, Dict, Any

from config import Settings
from connections.bwf.constants import BWF_KNOWLEDGE_BUNDLE
from connections.bwf.models import BwfConnection
from connections.bwf.schemas import BwfArticle
from connections.service import ConnectionLoader
from helixplatform import ar_core_fields
from helixplatform.models import Record
from helixplatform.service import InnovationSuite


class BwfConnectionLoader(ConnectionLoader):

    @classmethod
    def get_record_definition_name(cls):
        return BwfConnection.RECORD_DEFINITION

    @classmethod
    def from_record(cls, record: Record) -> BwfConnection:
        return BwfConnection(id=record[ar_core_fields.FIELD_ID], user=record[BwfConnection.FIELD_USER])


class Bwf(InnovationSuite):

    def __init__(self, connection: BwfConnection | None):
        super().__init__(
            Settings.BWF_URL,
            Settings.BWF_USER,
            Settings.BWF_PASSWORD,
            impersonated_user=connection.user if connection else None)

    def get_article_ids(self, display_id: str | None = None, modified_since: datetime | None = None) -> [str]:
        """
        Returns the list of the IDs of the knowledge articles.

        :param display_id: if not falsy, the result will only include the ID of the article having that display ID
                           (at best).
        :param modified_since: only articles modified since this date will be returned
        """
        return self.__query_articles(
            property_selection=[ar_core_fields.FIELD_ID],
            record_reader=lambda record: record[str(ar_core_fields.FIELD_ID)],
            display_id=display_id,
            modified_since=modified_since
        )

    def get_article_display_ids(self, display_id: str | None = None) -> [str]:
        """
        Returns the list of the display IDs of all the knowledge articles.

        :param display_id: if not falsy, the result will only include the ID of the article having that display ID
                           (at best).

        FUTURE: We can evaluate the possibility to use a trie collection in order to save memory. This is relevant
                because there could be a lot of articles and their display IDs very often have common prefixes e.g.,
                'KA-000000000022' and 'KA-000000000279', where only '022' and '279' are different.
                The same opportunity _may_ exist for internal IDs, actually, since they are kinda prefixed too (even
                though they are often called "GUIDs") e.g.
                 'AGGADG31EB17YARIFXXURIFXXU2INJ' and 'AGGADGG8ECDC0AQK1IDIQ92SZON28H'.
                 The gain would have to be validated for these, though.
        """
        return self.__query_articles(
            property_selection=[BwfArticle.FIELD_CONTENT_ID],
            record_reader=lambda record: record[str(BwfArticle.FIELD_CONTENT_ID)],
            display_id=display_id
        )

    def __query_articles(self,
                         property_selection: List[int] | None = None,
                         record_reader: Callable[[Dict[str, Any]], Any] = None,
                         id_: str = None,
                         display_id: str | None = None,
                         modified_since: datetime | None = None) -> []:
        record_reader = record_reader or (lambda record: record)  # return the whole record by default
        query_expression = Bwf._build_query_expression(id_, display_id, modified_since)

        if id_ or display_id:
            page = self.get_records(
                BwfArticle.FORM_KNOWLEDGE_ARTICLE_TEMPLATE,
                property_selection=property_selection,
                query_expression=query_expression,
                default_bundle_scope=BWF_KNOWLEDGE_BUNDLE
            )
            records = page.data
        else:
            records = self.get_all_records(
                BwfArticle.FORM_KNOWLEDGE_ARTICLE_TEMPLATE,
                property_selection=property_selection,
                query_expression=query_expression,
                default_bundle_scope=BWF_KNOWLEDGE_BUNDLE
            )

        return [record_reader(record) for record in records]

    @staticmethod
    def _build_query_expression(id_: str = None,
                                display_id : str | None = None,
                                modified_since: datetime | None = None):
        query_expression = f"'{BwfArticle.FIELD_ARTICLE_STATUS}' = \"{BwfArticle.ARTICLE_STATUS_PUBLISHED}\""

        extra_filters = []
        if modified_since:
            extra_filters.append(f"'{BwfArticle.FIELD_ARTICLE_MODIFIED_DATE}' >= \"{int(modified_since.timestamp())}\"")
        if id_:
            extra_filters.append(f"'{ar_core_fields.FIELD_ID}' = \"{id_}\"")
        if display_id:
            extra_filters.append(f"'{BwfArticle.FIELD_CONTENT_ID}' = \"{display_id}\"")

        if extra_filters:
            query_expression += ' AND ' + ' AND '.join(extra_filters)

        return query_expression

    def get_article(self, uuid: str) -> BwfArticle:
        url = f"/api/com.bmc.dsm.knowledge/rx/application/knowledge/{uuid}"
        headers = InnovationSuite.build_rx_headers()
        response = self.session.get(self.build_url(url), headers=headers)
        response.raise_for_status()
        json = response.json()
        bwf_article = BwfArticle.from_json_dict(json)
        return bwf_article

    def get_article_company(self, uuid: str) -> str | None:
        """
        Returns the company (e.g., "Petramco") of the specified article or ``None`` if blank or if the article wasn't
        found.
        """
        companies = self.__query_articles(
            property_selection=[BwfArticle.FIELD_COMPANY],
            record_reader=lambda record: record[str(BwfArticle.FIELD_COMPANY)],
            id_=uuid
        )
        return companies[0] if companies else None
