import pytest
from pytest_mock import MockerFixture

from connections.hkm.crawler import crawl_hkm
from connections.hkm.models import HkmConnection
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


@pytest.fixture
def connection() -> HkmConnection:
    return HkmConnection(id='CONNECTION_ID', user=None)


def test_crawl_hkm_specific_article(mocker: MockerFixture, connection):
    job = Job(Datasource.HKM, id='JOB_ID', doc_id='123')
    job_step = JobStep(JobType.LOAD, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_article_ids_mock = mocker.patch('connections.hkm.crawler.Hkm.get_article_ids', return_value={123})

    crawl_hkm(job, job_step, job_chain, connection)

    get_article_ids_mock.assert_called_once_with(123)

    # check queued job steps
    job_chain.queue_job_step.assert_called_once()
    assert job_chain.queue_job_step.mock_calls[0].args[0] == job

    load_job_step: JobStep = job_chain.queue_job_step.mock_calls[0].args[1]
    assert load_job_step.job_id == job.id
    assert load_job_step.datasource == job.datasource
    assert load_job_step.doc_id == job.doc_id
    assert load_job_step.doc_display_id is None

    assert not job_chain.queue_job_step.mock_calls[0].kwargs['execute_now']

    queue_sync_deletions_if_configured_call = job_chain.queue_sync_deletions_if_configured.mock_calls[0]
    assert queue_sync_deletions_if_configured_call.args[0] == job
    assert queue_sync_deletions_if_configured_call.args[0].doc_id == '123'
    assert queue_sync_deletions_if_configured_call.args[1] == connection


def test_crawl_hkm_no_article_listed(mocker: MockerFixture, connection):
    job = Job(Datasource.HKM, id='JOB_ID')
    job_step = JobStep(JobType.LOAD, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_article_ids_mock = mocker.patch('connections.hkm.crawler.Hkm.get_article_ids', return_value=set())

    crawl_hkm(job, job_step, job_chain, connection)

    get_article_ids_mock.assert_called_once()
    job_chain.queue_job_step.assert_not_called()

    queue_sync_deletions_if_configured_call = job_chain.queue_sync_deletions_if_configured.mock_calls[0]
    assert queue_sync_deletions_if_configured_call.args[0] == job
    assert queue_sync_deletions_if_configured_call.args[1] == connection


def test_crawl_hkm(mocker: MockerFixture, connection):
    job = Job(Datasource.HKM, id='JOB_ID')
    job_step = JobStep(JobType.LOAD, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_article_ids_mock = mocker.patch('connections.hkm.crawler.Hkm.get_article_ids', return_value={123, 456})

    crawl_hkm(job, job_step, job_chain, connection)

    get_article_ids_mock.assert_called_once()
    assert len(job_chain.queue_job_step.mock_calls) == 2
    # check the `doc_id`s of the queued job steps
    assert set([mock_call.args[1].doc_id for mock_call in job_chain.queue_job_step.mock_calls]) == {'123', '456'}
    for mock_call in job_chain.queue_job_step.mock_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource

    queue_sync_deletions_if_configured_call = job_chain.queue_sync_deletions_if_configured.mock_calls[0]
    assert queue_sync_deletions_if_configured_call.args[0] == job
    assert queue_sync_deletions_if_configured_call.args[1] == connection

