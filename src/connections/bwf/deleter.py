from contextlib import AbstractContextManager
from typing import Collection

from connections.bwf.feature import BwfFeature
from connections.bwf.service import Bwf
from connections.models import Connection
from .models import BwfConnection
from ..deleter import BaseDeleter
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from jobs.service import JobChain


class BwfDeleter(BaseDeleter):
    def __init__(self):
        super().__init__(BwfFeature(), source_document_label='BWF KA')

    def open_source_client(self, connection: BwfConnection) -> AbstractContextManager:
        return Bwf(connection)

    def get_source_published_keys(self, bwf: Bwf, job: Job, job_step: JobStep, connection: Connection) \
            -> Collection[str]:
        return bwf.get_article_display_ids(job.doc_display_id)


def sync_bwf_deletions(job: Job, job_step: JobStep, chain: JobChain, connection: Connection) -> None:
    """
    Enqueues deletions of BWF knowledge articles, which have been unpublished or were deleted, from OpenSearch.

    :param job: specifies the scope of the articles to check for deletion
    :param job_step: this job step specification.
    :param chain: allows to chain deletion job steps.
    :param connection: configuration details of the integration
    """
    deleter_impl = BwfDeleter()
    deleter_impl.sync_deletions(job, job_step, chain, connection)


def delete_bwf_knowledge_article(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: Connection) -> None:
    """
    Deletes a BWF knowledge article specified by ``job_step`` from OpenSearch.

    :param job: parent job of `job_step`
    :param job_step: specifies the article to delete
    :param chain: leveraged to delete the OpenSearch documents
    :param connection: configuration details of the integration
    """
    deleter_impl = BwfDeleter()
    deleter_impl.delete_open_search_document(job, job_step, chain, connection)
