import os
from threading import Lock
from typing import List, Dict, Any
from urllib.parse import quote

from loguru import logger
from opensearchpy import OpenSearch
from opensearchpy.exceptions import OpenSearchException, NotFoundError, ConnectionError, RequestError

from config import Settings
from health.constants import HealthStatus
from health.models import Health, HealthIndicator
from utils.io_utils import read_json_dict

ERROR_INDEX_NOT_FOUND_EXCEPTION = 'index_not_found_exception'
RESOURCE_ALREADY_EXISTS_EXCEPTION = 'resource_already_exists_exception'
INDEX_CREATION_LOCK = Lock()
INDEX_CREATION_LOCK_TIMEOUT = 30.0  # secs


def _load_index_definition() -> dict:
    return read_json_dict(os.path.dirname(__file__), 'opensearch_index_schema.json')


class OpenSearchClient(OpenSearch):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index_exists = False

    def check_index_exists(self, index_name: str):
        """ Indicates whether the specified OpenSearch index exists. Errors are rethrown. """
        try:
            self.indices.get(index_name)
            return True
        except NotFoundError as e:
            if e.error == ERROR_INDEX_NOT_FOUND_EXCEPTION:
                return False
            else:
                raise e

    def ensure_application_index_created_no_rethrow(self):
        """
        A version of `ensure_application_index_created()` where all errors are caught and logged rather than rethrown.
        """
        try:
            self.ensure_application_index_created()
        except ConnectionError as e:
            logger.warning(f'unable to ensure OpenSearch application index existence because of connection issue: {e}')
        except Exception:
            logger.exception("error while making sure the application OpenSearch index is created")

    def ensure_application_index_created(self):
        """
        Checks whether the application OpenSearch index exists and tries to create it if it doesn't.
        Once the existence has been established (through check or creation), new calls of this method will just return
        without re-checking.
        """
        application_index_name = Settings.OPENSEARCH_INDEX

        acquired = INDEX_CREATION_LOCK.acquire(blocking=True, timeout=INDEX_CREATION_LOCK_TIMEOUT)
        if acquired:
            try:
                return self._ensure_application_index_created_no_lock(application_index_name)
            finally:
                INDEX_CREATION_LOCK.release()
        else:
            logger.info('lock for application index detection timed out: skipping')

    def _ensure_application_index_created_no_lock(self, application_index_name):
        if self.index_exists:
            logger.trace('skipping index existence detection')
            return
        self.index_exists = self.check_index_exists(application_index_name)
        logger.info('application index exists: {exists}', exists=self.index_exists)

        if not self.index_exists:
            try:
                self.create_application_index(application_index_name)
            except RequestError as e:
                if e.error == RESOURCE_ALREADY_EXISTS_EXCEPTION:
                    # It seems that another node or thread did the work (though that shouldn't be possible anymore)
                    logger.warning(
                        "creation failure because application index already exists:"
                        " did another node or thread create it?")
                else:
                    raise e
            self.index_exists = True
        else:
            self._ensure_application_mappings(application_index_name)

    def _ensure_application_mappings(self, application_index_name):
        """
        Ensures that the latest metadata field mapping definitions have been applied to the given index
        :param application_index_name: name of the index to update the field mappings for
        """
        logger.info('ensuring application index {index_name} has all field mapping updates', index_name=application_index_name)
        index_definition = _load_index_definition()
        mappings = index_definition.get("mappings")
        self.indices.put_mapping(body=mappings, index=application_index_name)
        logger.info('updated application index {index_name} field mappings successfully', index_name=application_index_name)

    def create_application_index(self, application_index_name):
        """
        Creates a Data Connection application index with the official builtin definition and the specified name.
        """
        logger.debug('creating application index {index_name}', index_name=application_index_name)
        index_definition = _load_index_definition()
        self.indices.create(application_index_name, body=index_definition)
        self.index_exists = True
        logger.info('created application index {index_name} successfully',
                    index_name=application_index_name)

    def search_scroll_documents(
            self,
            datasource: str,
            size: int | None = 10_000,
            scroll: str | None = '5m',
            search_by_key_field: str | None = None,
            search_by_key_value: str | None = None):
        """
        Yields the results of searching the documents matched by the given criteria.
        The yielded objects are `Dict` objects corresponding with the `doc_id` and `doc_display_id` properties.

            {'doc_id': '123', 'doc_display_id': 'ABC'}

        :param search_by_key_field:
            if not None (and if `search_by_key_value` is not None), indicates a field to filter by.
        :param search_by_key_value: value corresponding to `search_by_key_field`

        :return: an iterator on the above metadata objects.
        """
        response = None
        while True:
            if response:
                # if there was a previous response, use `scroll()` to get the next one
                # or exit the loop if it was the last one
                if response['hits']['hits'] and (scroll_id := response.get('_scroll_id')):
                    response = self.scroll(scroll=scroll, scroll_id=scroll_id)
                else:
                    break
            else:  # initial search call
                response = self.search_documents(
                    datasource,
                    size=size,
                    scroll=scroll,
                    search_by_key_field=search_by_key_field,
                    search_by_key_value=search_by_key_value)

            if response:
                for metadata in self.__convert_search_response_to_metadata_array(response):
                    yield metadata

    def search_documents(
            self,
            datasource: str,
            size: int | None = None,
            scroll: str | None = None,
            search_by_key_field: str | None = None,
            search_by_key_value: str | None = None) -> Any:
        """
        Searches the documents matching the given criteria and returns the response.

        :param datasource: specifies the datasource of the searched documents.
        :param size: the page size.
        :param scroll: if not None, specifies how long OpenSearch should cache the search, e.g. '10m' for 10 minutes.
        :param search_by_key_field:
            if not None (and if `search_by_key_value` is not None), indicates a field to filter by.
        :param search_by_key_value: value corresponding to `search_by_key_field`
        """
        filters = [{'term': {'metadata.datasource': {'value': datasource}}}]
        if search_by_key_field and search_by_key_value:
            filters.append({'term': {search_by_key_field: {'value': search_by_key_value}}})
        return self.search(
            index=Settings.OPENSEARCH_INDEX,
            size=size,
            scroll=scroll,
            _source_includes='metadata.doc_id,metadata.doc_display_id',
            body={
                'query': {
                    'bool': {
                        'must': filters
                    }
                }
            }
        )

    @staticmethod
    def __convert_search_response_to_metadata_array(response) -> List[Dict]:
        """
        Captures and returns the `hits.hits[].metadata` elements gathered in a flat array.
        """
        if (hits1 := response.get('hits')) and (hits2 := hits1.get('hits')):
            return [result.get('_source').get('metadata') for result in hits2 if '_source' in result]
        return []


def get_open_search_client() -> OpenSearchClient:
    return OpenSearchClient(
        get_open_search_url(),
        verify_certs=Settings.OPENSEARCH_VERIFY_CERTIFICATES
    )


def get_open_search_url() -> str:
    host = Settings.OPENSEARCH_HOST
    if Settings.OPENSEARCH_PORT is not None:
        host = f'{host}:{Settings.OPENSEARCH_PORT}'

    scheme = 'https' if Settings.OPENSEARCH_SECURE else 'http'
    credentials = ''
    if Settings.OPENSEARCH_USER and Settings.OPENSEARCH_USER_PASSWORD:
        credentials = f'{quote(Settings.OPENSEARCH_USER)}:{quote(Settings.OPENSEARCH_USER_PASSWORD)}@'

    return f'{scheme}://{credentials}{host}'


class IndexHealthIndicator(HealthIndicator):
    def get_health(self) -> Health:
        client = get_open_search_client()
        try:
            available = client.ping()
            if available:
                return Health(name='opensearch', status=HealthStatus.UP)

        except OpenSearchException as e:
            logger.error('opensearch service is unavailable: {cause}', cause=e)

        return Health(name='opensearch', status=HealthStatus.DOWN)
