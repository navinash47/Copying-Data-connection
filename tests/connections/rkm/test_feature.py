from pytest_mock import MockerFixture

from connections.rkm.crawler import crawl_rkm
from connections.rkm.deleter import sync_rkm_deletions, delete_rkm_knowledge_article
from connections.rkm.feature import RkmFeature
from connections.rkm.loader import load_rkm_knowledge_article
from connections.rkm.service import RkmConnectionLoader
from connections.service import ConnectionRepository
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import DeleteDocBy


def test_get_connection_loader(mocker: MockerFixture):
    connection_repository = mocker.Mock(ConnectionRepository)
    connection_loader = RkmFeature().get_connection_loader('CONNECTION_ID', connection_repository)
    assert type(connection_loader) is RkmConnectionLoader


def test_delete_doc_by():
    job = Job('RKM')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert RkmFeature().get_delete_doc_by(job, job_step) == DeleteDocBy.BY_DOC_DISPLAY_ID


def test_get_handler_with_crawl():
    job = Job('RKM')
    job_step = JobStep(JobType.CRAWL, job.datasource)
    assert RkmFeature().get_handler(job, job_step) == crawl_rkm


def test_get_handler_with_load():
    job = Job('RKM')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert RkmFeature().get_handler(job, job_step) == load_rkm_knowledge_article


def test_get_handler_with_sync_deletions():
    job = Job('RKM')
    job_step = JobStep(JobType.SYNC_DELETIONS, job.datasource)
    assert RkmFeature().get_handler(job, job_step) == sync_rkm_deletions


def test_get_handler_with_delete():
    job = Job('RKM')
    job_step = JobStep(JobType.DELETE, job.datasource, doc_id='DOC ID')
    assert RkmFeature().get_handler(job, job_step) == delete_rkm_knowledge_article
