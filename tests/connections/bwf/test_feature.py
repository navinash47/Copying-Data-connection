from pytest_mock import MockerFixture

from connections.bwf.crawler import crawl_bwf
from connections.bwf.deleter import sync_bwf_deletions, delete_bwf_knowledge_article
from connections.bwf.feature import BwfFeature
from connections.bwf.loader import load_bwf_article
from connections.bwf.service import BwfConnectionLoader
from connections.service import ConnectionRepository
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import DeleteDocBy


def test_get_connection_loader(mocker: MockerFixture):
    connection_repository = mocker.Mock(ConnectionRepository)
    connection_loader = BwfFeature().get_connection_loader('CONNECTION_ID', connection_repository)
    assert type(connection_loader) is BwfConnectionLoader


def test_delete_doc_by():
    job = Job('BWF')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert BwfFeature().get_delete_doc_by(job, job_step) == DeleteDocBy.BY_DOC_DISPLAY_ID


def test_get_handler_with_crawl():
    job = Job('BWF')
    job_step = JobStep(JobType.CRAWL, job.datasource)
    assert BwfFeature().get_handler(job, job_step) == crawl_bwf


def test_get_handler_with_load():
    job = Job('BWF')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert BwfFeature().get_handler(job, job_step) == load_bwf_article


def test_get_handler_with_sync_deletions():
    job = Job('BWF')
    job_step = JobStep(JobType.SYNC_DELETIONS, job.datasource)
    assert BwfFeature().get_handler(job, job_step) == sync_bwf_deletions


def test_get_handler_with_delete():
    job = Job('BWF')
    job_step = JobStep(JobType.DELETE, job.datasource, doc_id='DOC ID')
    assert BwfFeature().get_handler(job, job_step) == delete_bwf_knowledge_article
