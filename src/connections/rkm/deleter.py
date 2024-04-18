from contextlib import AbstractContextManager
from typing import Collection


from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from jobs.service import JobChain
from .feature import RkmFeature
from .models import RkmConnection
from ..models import Connection
from ..rkm.service import Rkm
from ..deleter import BaseDeleter


class RkmDeleter(BaseDeleter):
    def __init__(self):
        super().__init__(RkmFeature(), source_document_label='RKM KA')

    def open_source_client(self, connection: RkmConnection) -> AbstractContextManager:
        return Rkm(connection)

    def get_source_published_keys(self, rkm: Rkm, job: Job, job_step: JobStep, connection: Connection) \
            -> Collection[str]:
        # For now, only scoping down to the display ID is supported because the technical ID would have to first be
        # translated to the display ID anyway (RKM deletes by display ID).
        # The rest of the algorithm should be OK, though, regardless of the job properties.
        return rkm.list_published_knowledge_article_display_ids(
            display_ids=[job.doc_display_id] if job.doc_display_id else None,
        )


def sync_rkm_deletions(job: Job, job_step: JobStep, chain: JobChain, connection: Connection) -> None:
    """
    Enqueues deletions of RKM knowledge articles, which have been unpublished or were deleted, from OpenSearch.

    :param job: specifies the scope of the articles to check for deletion
    :param job_step: this job step specification.
    :param chain: allows to chain deletion job steps.
    :param connection: configuration details of the integration
    """
    deleter_impl = RkmDeleter()
    deleter_impl.sync_deletions(job, job_step, chain, connection)


def delete_rkm_knowledge_article(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: Connection) -> None:
    """
    Deletes a RKM knowledge article specified by ``job_step`` from OpenSearch.

    :param job: parent job of `job_step`
    :param job_step: specifies the article to delete
    :param chain: leveraged to delete the OpenSearch documents
    :param connection: configuration details of the integration
    """
    deleter_impl = RkmDeleter()
    deleter_impl.delete_open_search_document(job, job_step, chain, connection)
