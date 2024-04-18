import pytest
from pytest_mock import MockerFixture

from connections.rkm.crawler import crawl_rkm
from connections.rkm.models import RkmConnection
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


@pytest.fixture
def connection():
    return RkmConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


def test_crawl_rkm(mocker: MockerFixture, connection: RkmConnection):
    job = Job(Datasource.RKM, id='JOB_ID')
    job.sync_deletions = False
    job_step = JobStep(JobType.CRAWL, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    list_published_knowledge_articles = mocker.patch(
        'connections.rkm.crawler.Rkm.list_published_knowledge_articles',
        return_value=[{'InstanceId': 'KA_ID_1'}, {'InstanceId': 'KA_ID_2'}])

    crawl_rkm(job, job_step, job_chain, connection)

    list_published_knowledge_articles.assert_called_once()
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


def test_crawl_rkm_with_no_articles_and_sync_delete(mocker: MockerFixture, connection: RkmConnection):
    job = Job(Datasource.RKM, id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    list_published_knowledge_articles = mocker.patch(
        'connections.rkm.crawler.Rkm.list_published_knowledge_articles', return_value=[])

    crawl_rkm(job, job_step, job_chain, connection)

    list_published_knowledge_articles.assert_called_once()
    job_chain.queue_job_step.assert_not_called()

    queue_sync_deletions_if_configured_call = job_chain.queue_sync_deletions_if_configured.mock_calls[0]
    assert queue_sync_deletions_if_configured_call.args[0] == job
    assert queue_sync_deletions_if_configured_call.args[1] == connection
