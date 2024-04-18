from contextlib import AbstractContextManager
from typing import Collection

from connections.deleter import BaseDeleter
from connections.models import Connection
from indexing.service import IndexingJobChain
from jobs.models import JobStep, Job
from jobs.service import JobChain
from utils.object_utils import int_defaulted_to_none
from .feature import HkmFeature
from .models import HkmConnection
from .service import Hkm


class HkmDeleter(BaseDeleter):
    def __init__(self):
        super().__init__(HkmFeature(), source_document_label='HKM article')

    def open_source_client(self, connection: HkmConnection) -> AbstractContextManager:
        return Hkm(connection)

    def get_source_published_keys(self, hkm: Hkm, job: Job, job_step: JobStep, connection: HkmConnection) \
            -> Collection[str]:
        """ Returns the list of all HKM articles as strings. """
        article_ids: set[int] = hkm.get_article_ids(int_defaulted_to_none(job.doc_id))
        return [str(article_int_id) for article_int_id in article_ids]


def sync_hkm_deletions(job: Job, job_step: JobStep, chain: JobChain, connection: HkmConnection) -> None:
    """
    Enqueues deletions of HKM articles, which have been unpublished or were deleted, from OpenSearch.

    :param job: specifies the scope of the articles to check for deletion
    :param job_step: this job step specification.
    :param chain: allows to chain deletion job steps.
    :param connection: configuration details of the integration
    """
    deleter_impl = HkmDeleter()
    deleter_impl.sync_deletions(job, job_step, chain, connection)


def delete_hkm_article(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: HkmConnection) -> None:
    """
    Deletes a HKM article specified by ``job_step`` from OpenSearch.

    :param job: parent job of `job_step`
    :param job_step: specifies the article to delete
    :param chain: leveraged to delete the OpenSearch documents
    :param connection: configuration details of the integration
    """
    deleter_impl = HkmDeleter()
    deleter_impl.delete_open_search_document(job, job_step, chain, connection)




