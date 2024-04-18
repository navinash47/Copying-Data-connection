import pytest
from pytest_mock import MockerFixture

from connections.bwf.crawler import crawl_bwf
from connections.bwf.models import BwfConnection
from indexing.service import IndexingJobChain
from jobs.constants import JobType, Datasource
from jobs.models import JobStep, Job


@pytest.fixture
def connection():
    return BwfConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


def test_crawl_bwf(mocker: MockerFixture, connection: BwfConnection):
    job = Job(Datasource.BWF, id='JOB_ID')
    job.sync_deletions = False
    job_step = JobStep(JobType.CRAWL, datasource=job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_article_ids_mock = mocker.patch(
        'connections.bwf.crawler.Bwf.get_article_ids', return_value=['KA_ID_1', 'KA_ID_2'])

    crawl_bwf(job, job_step, job_chain, connection)

    get_article_ids_mock.assert_called_once()
    assert len(job_chain.queue_job_step.mock_calls) == 2
    # check the `doc_id`s of the queued job steps
    assert (set([mock_call.args[1].doc_id for mock_call in job_chain.queue_job_step.mock_calls]) ==
            {'KA_ID_1', 'KA_ID_2'})
    for mock_call in job_chain.queue_job_step.mock_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource

    queue_sync_deletions_if_configured_call = job_chain.queue_sync_deletions_if_configured.mock_calls[0]
    assert queue_sync_deletions_if_configured_call.args[0] == job
    assert queue_sync_deletions_if_configured_call.args[1] == connection


def test_crawl_bwf_with_no_articles_and_sync_delete(mocker: MockerFixture, connection: BwfConnection):
    job = Job(Datasource.BWF, id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_article_ids_mock = mocker.patch(
        'connections.bwf.crawler.Bwf.get_article_ids', return_value=[])

    crawl_bwf(job, job_step, job_chain, connection)

    get_article_ids_mock.assert_called_once()
    job_chain.queue_job_step.assert_not_called()

    job_chain.queue_sync_deletions_if_configured.assert_called_once()
    queue_sync_deletions_if_configured_call = job_chain.queue_sync_deletions_if_configured.mock_calls[0]
    assert queue_sync_deletions_if_configured_call.args[0] == job
    assert queue_sync_deletions_if_configured_call.args[1] == connection
