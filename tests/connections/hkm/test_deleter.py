from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from connections.hkm.deleter import sync_hkm_deletions, delete_hkm_article
from connections.hkm.models import HkmConnection
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
def job() -> Job:
    return Job(Datasource.HKM, id='JOB_ID')


@pytest.fixture
def connection() -> HkmConnection:
    return HkmConnection(id='CONNECTION_ID', user='USER')


def test_sync_hkm_deletions(mocker: MockerFixture, open_search_client, job, connection):
    job_step = JobStep(JobType.SYNC_DELETIONS, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)

    get_open_search_client = mocker.patch(
        'connections.deleter.get_open_search_client', return_value=open_search_client)
    open_search_client.search_scroll_documents.return_value = [
        {'doc_id': 'KA_ID_1'}, {'doc_id': 'KA_ID_3'}, {'doc_id': 'KA_ID_4'}]
    get_article_display_ids = mocker.patch(
        'connections.hkm.deleter.Hkm.get_article_ids', return_value={'KA_ID_2', 'KA_ID_3'})
    # published but not indexed -> no deletion expected: KA_ID_2
    # published and indexed     -> no deletion expected: KA_ID_3
    # not published and indexed -> deletion expected:    KA_ID_1, KA_ID_4

    sync_hkm_deletions(job, job_step, job_chain, connection)

    get_open_search_client.assert_called_once()
    get_article_display_ids.assert_called_once()
    queue_job_step_calls = job_chain.queue_job_step.mock_calls
    assert len(queue_job_step_calls) == 2
    # check the `doc_display_id`s of the queued job steps
    assert set([mock_call.args[1].doc_id for mock_call in queue_job_step_calls]) == {'KA_ID_1', 'KA_ID_4'}
    for mock_call in queue_job_step_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource
        assert mock_call.args[2] == connection

    job_chain.execute_job_steps.assert_called_once()


def test_delete_hkm_article(mocker: MockerFixture, open_search_client, job, connection):
    job_step = JobStep(JobType.DELETE, job.datasource, doc_id='ARTICLE_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    get_open_search_client = mocker.patch(
        'connections.deleter.get_open_search_client', return_value=open_search_client)

    delete_hkm_article(job, job_step, job_chain, connection)

    get_open_search_client.assert_called_once()
    job_chain.delete_document.assert_called_once_with(
        open_search_client, Datasource.HKM, 'metadata.doc_id', 'ARTICLE_ID','CONNECTION_ID')


def test_delete_hkm_article_no_doc_display_id(mocker: MockerFixture, job, connection):
    job_step = JobStep(JobType.DELETE, job.datasource, doc_id=None, doc_display_id=None)
    job_chain = mocker.Mock(IndexingJobChain)
    with pytest.raises(ValueError):
        delete_hkm_article(job, job_step, job_chain, connection)
