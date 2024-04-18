import pytest
from pytest_mock import MockerFixture

from connections.confluence.crawler import crawl_confluence
from connections.confluence.service import ConfluenceConnection
from indexing.service import IndexingJobChain
from jobs.constants import JobType, Datasource
from jobs.models import JobStep, Job


def test_crawl_confluence(mocker: MockerFixture):
    confluence_connection = ConfluenceConnection(id="test_id", page_id="page_id", url="test_url",
                                                 access_token="")

    job = Job(Datasource.CONFLUENCE, id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, datasource=job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_page_with_all_child_ids_mock = mocker.patch(
        'connections.confluence.crawler.ConfluenceService.get_page_with_all_child_ids', return_value=['page_id','child_page_id'])
    crawl_confluence(job, job_step, job_chain, confluence_connection)

    get_page_with_all_child_ids_mock.assert_called_once()
    assert len(job_chain.queue_job_step.mock_calls) == 2
    assert (set([mock_call.args[1].doc_id for mock_call in job_chain.queue_job_step.mock_calls]) ==
            {'child_page_id', 'page_id'})
    for mock_call in job_chain.queue_job_step.mock_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource


def test_crawl_confluence_with_empty_child_pages(mocker: MockerFixture):
    confluence_connection = ConfluenceConnection(id="test_id", page_id="page_id", url="test_url",
                                                 access_token="")

    job = Job(Datasource.CONFLUENCE, id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, datasource=job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_page_with_all_child_ids_mock = mocker.patch(
        'connections.confluence.crawler.ConfluenceService.get_page_with_all_child_ids', return_value=[])
    crawl_confluence(job, job_step, job_chain, confluence_connection)

    get_page_with_all_child_ids_mock.assert_called_once()
    assert len(job_chain.queue_job_step.mock_calls) == 0
    job_chain.assert_not_called()
    for mock_call in job_chain.queue_job_step.mock_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource


def test_crawl_confluence_with_no_page_and_sync_delete(mocker: MockerFixture):
    confluence_connection = ConfluenceConnection(id="test_id", page_id='', url="test_url",
                                                 access_token="")

    job = Job(Datasource.CONFLUENCE, id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, datasource=job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    get_page_with_all_child_ids_mock = mocker.patch(
        'connections.confluence.crawler.ConfluenceService.get_page_with_all_child_ids', return_value=[])
    crawl_confluence(job, job_step, job_chain, confluence_connection)

    get_page_with_all_child_ids_mock.assert_not_called()

    assert job_step.datasource == job.datasource
    assert job_step.doc_id == job.doc_id
    assert job_step.doc_display_id == job.doc_display_id


def test_crawl_confluence_api_throw_exception(mocker: MockerFixture):
    confluence_connection = ConfluenceConnection(id="test_id", page_id="page_id", url="test_url",
                                                 access_token="")

    job = Job(Datasource.CONFLUENCE, id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, datasource=job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    mocker.patch('connections.confluence.crawler.ConfluenceService.get_page_with_all_child_ids',
                 side_effect=Exception("Test exception"))

    with pytest.raises(Exception) as exc_info:
        crawl_confluence(job, job_step, job_chain, confluence_connection)

    assert "Test exception" in str(exc_info.value)
