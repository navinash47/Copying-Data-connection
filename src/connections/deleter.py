import contextlib
from abc import abstractmethod, ABC
from contextlib import AbstractContextManager
from loguru import logger
import re
from typing import Collection

from connections.models import Connection
from indexing.service import IndexingJobChain
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import Feature, JobChain, DeleteDocBy
from opensearch.client import get_open_search_client


class BaseDeleter(ABC):
    """
    Base class for synchronizing deletions from source to the index in OpenSearch.

    Note that the current logic assumes it is reasonable to get the keys of all the source documents.
    This may not be true for every case, in the future.
    """
    def __init__(self, feature: Feature, source_document_label: str = 'source document'):
        self.feature = feature
        self.source_document_label = source_document_label

    def open_source_client(self, connection: Connection) -> AbstractContextManager:
        """
        Opens and returns the client object, which will be used to get the keys of the published source documents
        later.

        Only override this method if your source client doesn't implement the context manager interface.
        If it doesn't implement the context manager, just create it in `get_source_published_keys()`.
        The point of this method is just to allow opening the client at the right points in `sync_deletions()`.

        :param connection: connection config to the source
        """
        return contextlib.nullcontext

    @abstractmethod
    def get_source_published_keys(self, source_client, job: Job, job_step: JobStep, connection: Connection) \
            -> Collection[str]:
        """
        Returns a collection of all the keys of the published/indexable documents currently available in the source.
        This method should return the kind of keys appropriate for this integration (display ID or plain ID).

        :param source_client: the client previously gotten using `open_source_client()`.
                              Can be ignored if this implementation doesn't override `open_source_client()`.
        :param job: defines the scope of the deletion sync.
        :param job_step: the ``SYNC_DELETIONS`` job step
        :param connection: configuration details of the integration
        """
        ...

    def sync_deletions(self, job: Job, job_step: JobStep, chain: JobChain, connection: Connection) -> None:
        """
        Enqueues deletions of OpenSearch documents, which have been unpublished or were deleted in the source system.

        :param job: specifies the scope of the OpenSearch documents to check for deletion (datasource, IDs, etc)
        :param job_step: this job step specification.
        :param chain: allows to chain deletion job steps.
        :param connection: configuration details of the integration
        """
        with get_open_search_client() as open_search_client, self.open_source_client(connection) as source:
            source_published_keys = self.get_source_published_keys(source, job, job_step, connection)

            delete_doc_by, job_key_value, os_key_field = self.__pick_key_for_delete(job, job_step)
            os_metadata_key_field = re.sub(r'^metadata\.', '', os_key_field)  # 'metadata.xyz' -> 'xyz'

            # retrieve the OpenSearch entries adequate for the scope of the job
            os_entry_metadatas = open_search_client.search_scroll_documents(
                job.datasource, search_by_key_field=os_key_field, search_by_key_value=job_key_value)

            # for each OpenSearch entry, check whether it is still present in the published sources
            already_scheduled_for_deletion = set()
            for os_entry_metadata in os_entry_metadatas:
                if not BaseDeleter._matches_job_doc_ids(os_entry_metadata, job):
                    continue

                os_key = os_entry_metadata[os_metadata_key_field]
                if os_key not in already_scheduled_for_deletion and os_key not in source_published_keys:
                    # if we detect a now missing source document that we were unaware of, we spawn a new DELETE job step
                    logger.info(
                        'scheduling DELETE job for {document} "{key}"',
                        document=self.source_document_label, key=job_key_value)
                    delete_job_step = JobStep(
                        JobType.DELETE,
                        job.datasource,
                        job_id=job.id,
                        doc_id=os_key if delete_doc_by == DeleteDocBy.BY_DOC_ID else None,
                        doc_display_id=os_key if delete_doc_by == DeleteDocBy.BY_DOC_DISPLAY_ID else None)
                    chain.queue_job_step(job, delete_job_step, connection)
                    already_scheduled_for_deletion.add(os_key)

        chain.execute_job_steps(job)

    @staticmethod
    def _matches_job_doc_ids(os_entry_metadata: dict, job: Job) -> bool:
        if job.doc_id:
            os_entry_doc_id = os_entry_metadata.get('doc_id')
            return os_entry_doc_id == job.doc_id
        if job.doc_display_id:
            os_entry_doc_display_id = os_entry_metadata.get('doc_display_id')
            return os_entry_doc_display_id == job.doc_display_id
        return True

    def delete_open_search_document(
            self, job: Job, job_step: JobStep, chain: IndexingJobChain, connection: Connection) -> None:
        """
        Deletes a document according to the specification of the passed ``DELETE`` job step.

        :param job: parent of the job step
        :param job_step: ``DELETE`` specification of what document to delete.
        :param chain: used to perform the deletion
        :param connection: configuration of the connection to the source
        """
        delete_doc_by, job_key_value, os_key_field = self.__pick_key_for_delete(job, job_step)

        logger.info('deleting {document} {field}={value}',
                    document=self.source_document_label, field=os_key_field, value=job_key_value)

        if job_key_value is None:
            raise ValueError(
                f"unable to determine the OpenSearch property value, by which to delete documents ({os_key_field})")

        with get_open_search_client() as open_search_client:
            connection_id = connection.id if connection else None
            chain.delete_document(open_search_client, job.datasource, os_key_field, job_key_value, connection_id)

    def __pick_key_for_delete(self, job, job_step) -> (DeleteDocBy, str, str):
        """ Returns the ``DeleteDocBy`` mode used by this deleter, the name of the OpenSearch property to delete by,
            and the value of that property. """
        delete_doc_by = self.feature.get_delete_doc_by(job, job_step)
        os_key_field, job_key_value = delete_doc_by.pick_key_for_delete(job_step.doc_id, job_step.doc_display_id)
        return delete_doc_by, job_key_value, os_key_field
