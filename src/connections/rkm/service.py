from datetime import datetime
from typing import Dict, List, Collection

#import marisa_trie

from config import Settings
from connections.rkm.models import KnowledgeArticle, RkmConnection
from connections.service import ConnectionLoader
from helixplatform import ar_core_fields
from helixplatform.models import Record
from helixplatform.service import ArRestClient


class RkmConnectionLoader(ConnectionLoader):

    @classmethod
    def get_record_definition_name(cls):
        return RkmConnection.RECORD_DEFINITION

    @classmethod
    def from_record(cls, record: Record) -> RkmConnection:
        return RkmConnection(id=record[ar_core_fields.FIELD_ID], user=record[RkmConnection.FIELD_USER])


class Rkm(ArRestClient):
    FORM_KNOWLEDGE_ARTICLE_MANAGER = 'RKM:KnowledgeArticleManager'
    FIELD_KAM_INSTANCE_ID = 179  # InstanceId
    FIELD_KAM_ARTICLE_DISPLAY_ID = 302300507  # DocID: called "display ID" outside of AR queries to avoid confusion
    FIELD_KAM_ARTICLE_MODIFIED_DATE = 302300535  # ArticleLastModifiedDate
    FIELD_KAM_ARTICLE_TITLE = 302300502  # ArticleTitle
    FIELD_KAM_ARTICLE_FORM = 302300503  # ArticleForm
    FIELD_KAM_ARTICLE_GUID = 302301021  # Article GUID
    FIELD_KAM_FK_GUID = 302301020  # FK_GUID: foreign key to the more specific form
    FIELD_KAM_ARTICLE_STATUS = 302312185  # StatusSelectionField - the "Status"/7 field seems to be unused.
    FIELD_KAM_INTERNAL_ARTICLE_INDICATION = 302312186  # InternalArticleIndication - 0 (No), 1 (Yes)
    FIELD_KAM_COMPANY = 1000000001  # Company
    FIELD_LANGUAGE = 302300539  # Language ('English', 'Polish', etc)

    ARTICLE_STATUS_PUBLISHED = 700

    FIELD_GUID = 179  # GUID

    FORM_HOW_TO_TEMPLATE = 'RKM:HowToTemplate'
    FIELD_HTT_QUESTION = 302311200  # RKMTemplateQuestion
    FIELD_HTT_ANSWER = 302311201  # RKMTemplateAnswer
    FIELD_HTT_TECHNICAL_NOTES = 302311202  # RKMTemplateTechnicianNotes

    FORM_PROBLEM_SOLUTION_TEMPLATE = 'RKM:ProblemSolutionTemplate'
    FIELD_PST_PROBLEM = 302311205  # RKMTemplateProblem
    FIELD_PST_SOLUTION = 302311206  # RKMTemplateSolution
    FIELD_PST_TECHNICAL_NOTES = 302311202  # RKMTemplateTechnicianNotes

    FORM_KNOWN_ERROR_TEMPLATE = 'RKM:KnownErrorTemplate'
    FIELD_KET_TECHNICAL_NOTES = 302311202  # RKMTemplateTechnicianNotes
    FIELD_KET_ERROR = 302311207  # RKMTemplateError
    FIELD_KET_ROOT_CAUSE = 302311208  # RKMTemplateRootCause
    FIELD_KET_FIX = 302311209  # RKMTemplateFix

    FORM_REFERENCE_TEMPLATE = 'RKM:ReferenceTemplate'
    FIELD_RT_REFERENCE = 302311210  # Reference

    FORM_KCS = 'RKM:KCS:Template'
    FIELD_KCS_PROBLEM = 302308621  # RKMTemplateKCSProblem
    FIELD_KCS_ENVIRONMENT = 302308631  # RKMTemplateEnvironment
    FIELD_KCS_RESOLUTION = 302308641  # RKMTemplateResolution
    FIELD_KCS_CAUSE = 302308651  # RKMTemplateCause

    def __init__(self, connection: RkmConnection | None):
        super().__init__(
            Settings.RKM_URL, Settings.RKM_USER, Settings.RKM_PASSWORD, connection.user if connection else None)

    @staticmethod
    def _build_field_eq(field: int, values: List[str]) -> str | None:
        """
            Builds and returns an AR qualification filter, which matches the specified field against any of
            specified _text_ values. `None` values are ignored.
            --> `(field = "value1" OR field = "value2")`
        """
        if values:
            return '(' + ' OR '.join([f"'{field}' = \"{value}\"" for value in values if value]) + ')'
        else:
            return None

    def list_published_knowledge_articles(
            self,
            instance_ids: List[str] | None = None,
            display_ids: List[str] | None = None,
            modified_since: datetime | None = None):
        """
         Enumerates the entries of the published knowledge articles.

        :param modified_since: only articles modified since this date will be returned
        :param instance_ids: if specified, only these article instance IDs will be taken into account.
                             Combined with ``display_ids`` with an ``OR``.
        :param display_ids: if specified, only these display IDs will be taken into account.
                            Combined with ``instance_ids`` with an ``OR``.
        """
        qualification = Rkm._build_published_article_qualification(
            instance_ids=instance_ids, display_ids=display_ids, modified_since=modified_since)

        fields = [Rkm.FIELD_KAM_INSTANCE_ID, Rkm.FIELD_KAM_FK_GUID]
        for entry in self.enumerate_all_entries(Rkm.FORM_KNOWLEDGE_ARTICLE_MANAGER, qualification, fields):
            yield entry

    @staticmethod
    def _build_published_article_qualification(
            instance_ids: List[str] | None = None,
            display_ids: List[str] | None = None,
            modified_since: datetime | None = None) -> str:
        # only published articles
        qualification = Rkm.get_kam_published_articles_qualification()

        # consider specified IDs
        instance_ids_filter = Rkm._build_field_eq(Rkm.FIELD_KAM_INSTANCE_ID, instance_ids)
        display_ids_filter = Rkm._build_field_eq(Rkm.FIELD_KAM_ARTICLE_DISPLAY_ID, display_ids)
        ids_filter = ' OR '.join([f for f in [instance_ids_filter, display_ids_filter] if f])
        if ids_filter:
            qualification += ' AND (' + ids_filter + ')'

        if modified_since:
            qualification += f" AND '{Rkm.FIELD_KAM_ARTICLE_MODIFIED_DATE}' >= \"{int(modified_since.timestamp())}\""

        return qualification

    @staticmethod
    def get_kam_published_articles_qualification():
        return f"'{Rkm.FIELD_KAM_ARTICLE_STATUS}' = {Rkm.ARTICLE_STATUS_PUBLISHED}"

    def list_published_knowledge_article_display_ids(
            self,
            instance_ids: List[str] | None = None,
            display_ids: List[str] | None = None) -> Collection[str]:
        """
        Returns the collection of the display IDs of all the published articles.

        :param instance_ids: if specified, only these article instance IDs will be taken into account.
                             Combined with ``display_ids`` with an ``OR``.
        :param display_ids: if specified, only these display IDs will be taken into account.
                            Combined with ``instance_ids`` with an ``OR``.
        """
        qualification = Rkm._build_published_article_qualification(instance_ids=instance_ids, display_ids=display_ids)

        fields = [Rkm.FIELD_KAM_ARTICLE_DISPLAY_ID]
        entries = self.enumerate_all_entries(Rkm.FORM_KNOWLEDGE_ARTICLE_MANAGER, qualification, fields)
        # Using a trie collection to store the keys is much more memory efficient than having them in a set.
        # The Trie initializer will iterate on the entry generator returned by enumerate_all_entries().
        # This implies that only one page of entries will be in non-reclaimable memory at any time for one execution.
        #return marisa_trie.Trie(entry['DocID'] for entry in entries)

    def get_knowledge_article(self, instance_id: str) -> KnowledgeArticle:
        """ Fetches and returns the specified knowledge article trunk data () """
        qualification = f"""('{Rkm.FIELD_KAM_INSTANCE_ID}' = "{instance_id}")"""
        fields = [
            Rkm.FIELD_KAM_ARTICLE_DISPLAY_ID,
            Rkm.FIELD_KAM_ARTICLE_TITLE,
            Rkm.FIELD_KAM_ARTICLE_FORM,
            Rkm.FIELD_KAM_FK_GUID,
            Rkm.FIELD_KAM_INTERNAL_ARTICLE_INDICATION,
            Rkm.FIELD_KAM_COMPANY,
            Rkm.FIELD_LANGUAGE,
        ]
        response = self.get_entries(Rkm.FORM_KNOWLEDGE_ARTICLE_MANAGER, qualification, fields)
        entry = self.__first_entry_values_or_none(response)
        return KnowledgeArticle.from_dict(entry) if entry else None

    def __get_knowledge_article_details(self, form: str, fields: List[int], guid: str):
        qualification = f"""('{Rkm.FIELD_GUID}' = "{guid}")"""
        response = self.get_entries(form, qualification, fields)
        return self.__first_entry_values_or_none(response)

    def get_how_to(self, guid: str):
        fields = [
            Rkm.FIELD_HTT_QUESTION,
            Rkm.FIELD_HTT_ANSWER,
            Rkm.FIELD_HTT_TECHNICAL_NOTES,
        ]
        return self.__get_knowledge_article_details(Rkm.FORM_HOW_TO_TEMPLATE, fields, guid)

    def get_problem_solution(self, guid: str):
        fields = [
            Rkm.FIELD_PST_PROBLEM,
            Rkm.FIELD_PST_SOLUTION,
            Rkm.FIELD_PST_TECHNICAL_NOTES,
        ]
        return self.__get_knowledge_article_details(Rkm.FORM_PROBLEM_SOLUTION_TEMPLATE, fields, guid)

    def get_known_error(self, guid: str):
        fields = [
            Rkm.FIELD_KET_ERROR,
            Rkm.FIELD_KET_ROOT_CAUSE,
            Rkm.FIELD_KET_FIX,
            Rkm.FIELD_KET_TECHNICAL_NOTES,
        ]
        return self.__get_knowledge_article_details(Rkm.FORM_KNOWN_ERROR_TEMPLATE, fields, guid)

    def get_reference(self, guid: str) -> dict | None:
        fields = [Rkm.FIELD_RT_REFERENCE]
        return self.__get_knowledge_article_details(Rkm.FORM_REFERENCE_TEMPLATE, fields, guid)

    def get_kcs(self, guid: str) -> dict | None:
        fields = [
            Rkm.FIELD_KCS_PROBLEM,
            Rkm.FIELD_KCS_ENVIRONMENT,
            Rkm.FIELD_KCS_RESOLUTION,
            Rkm.FIELD_KCS_CAUSE
        ]
        return self.__get_knowledge_article_details(Rkm.FORM_KCS, fields, guid)

    @staticmethod
    def __first_entry_values_or_none(response: Dict) -> Dict | None:
        entries = response['entries']
        return entries[0]['values'] if entries and len(entries) > 0 else None
