from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from connections.bwf.deleter import sync_bwf_deletions, delete_bwf_knowledge_article
from connections.bwf.models import BwfConnection
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from opensearch.client import OpenSearchClient


@pytest.fixture
def open_search_client(mocker: MockerFixture) -> Mock:
    open_search_client = mocker.Mock(OpenSearchClient)
    open_search_client.__enter__ = mocker.Mock()
    open_search_client.__enter__.return_value = open_search_client
    open_search_client.__exit__ = mocker.Mock()
    open_search_client.__exit__.return_value = None
    return open_search_client


@pytest.fixture
def connection():
    return BwfConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


@pytest.fixture
def job() -> Job:
    return Job(Datasource.BWF, id='JOB_ID')


def test_sync_bwf_deletions(mocker: MockerFixture, open_search_client, connection: BwfConnection, job):
    job_step = JobStep(JobType.SYNC_DELETIONS, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)

    get_open_search_client = mocker.patch(
        'connections.deleter.get_open_search_client', return_value=open_search_client)
    open_search_client.search_scroll_documents.return_value = [
        {'doc_display_id': 'KA_ID_1'}, {'doc_display_id': 'KA_ID_3'}, {'doc_display_id': 'KA_ID_4'}]
    get_article_display_ids = mocker.patch(
        'connections.bwf.deleter.Bwf.get_article_display_ids',
        return_value={'KA_ID_2', 'KA_ID_3'})
    # published but not indexed -> no deletion expected: KA_ID_2
    # published and indexed     -> no deletion expected: KA_ID_3
    # not published and indexed -> deletion expected:    KA_ID_1, KA_ID_4

    sync_bwf_deletions(job, job_step, job_chain, connection)

    get_open_search_client.assert_called_once()
    get_article_display_ids.assert_called_once()
    queue_job_step_calls = job_chain.queue_job_step.mock_calls
    assert len(queue_job_step_calls) == 2
    # check the `doc_display_id`s of the queued job steps
    assert set([mock_call.args[1].doc_display_id for mock_call in queue_job_step_calls]) == {'KA_ID_1', 'KA_ID_4'}
    for mock_call in queue_job_step_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource
        assert mock_call.args[2] == connection

    job_chain.execute_job_steps.assert_called_once()


def test_delete_bwf_knowledge_article(mocker: MockerFixture, open_search_client, connection: BwfConnection, job):
    job_step = JobStep(JobType.DELETE, job.datasource, doc_display_id='KA_DISPLAY_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    get_open_search_client = mocker.patch(
        'connections.deleter.get_open_search_client', return_value=open_search_client)

    delete_bwf_knowledge_article(job, job_step, job_chain, connection)

    get_open_search_client.assert_called_once()
    job_chain.delete_document.assert_called_once_with(
        open_search_client, Datasource.BWF, 'metadata.doc_display_id', 'KA_DISPLAY_ID', 'CONNECTION_ID')


def test_delete_bwf_knowledge_article_no_doc_display_id(mocker: MockerFixture, connection: BwfConnection, job):
    job_step = JobStep(JobType.DELETE, job.datasource, doc_display_id=None)
    job_chain = mocker.Mock(IndexingJobChain)
    with pytest.raises(ValueError):
        delete_bwf_knowledge_article(job, job_step, job_chain, connection)
