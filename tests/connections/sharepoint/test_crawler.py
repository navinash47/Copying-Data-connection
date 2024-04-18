from O365.drive import DriveItem
from pytest_mock import MockerFixture

from connections.models import Connection
from connections.sharepoint.crawler import crawl_sharepoint
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


def test_crawl_sharepoint(mocker: MockerFixture):
    job = Job(Datasource.SHAREPOINT, id='JOB_ID')
    job_step = JobStep(JobType.LOAD, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    connection = mocker.Mock(Connection)
    file = mocker.Mock(DriveItem)
    file.object_id = "test_file_id"
    file.drive_id = "test_library_id"
    get_files_mock = mocker.patch('connections.sharepoint.crawler.SharePoint.get_files', return_value=[file])
    crawl_sharepoint(job, job_step, job_chain, connection)
    get_files_mock.assert_called_once()
    assert len(job_chain.queue_job_step.mock_calls) == 1
    assert set([mock_call.args[1].doc_id for mock_call in job_chain.queue_job_step.mock_calls]) == {
        "test_library_id/test_file_id"}
    for mock_call in job_chain.queue_job_step.mock_calls:
        assert mock_call.args[1].job_id == job.id
        assert mock_call.args[1].datasource == job.datasource


def test_crawl_sharepoint_when_no_files(mocker: MockerFixture):
    job = Job(Datasource.SHAREPOINT, id='JOB_ID')
    job_step = JobStep(JobType.LOAD, job.datasource)
    job_chain = mocker.Mock(IndexingJobChain)
    connection = mocker.Mock(Connection)
    get_files_mock = mocker.patch('connections.sharepoint.crawler.SharePoint.get_files', return_value=[])
    crawl_sharepoint(job, job_step, job_chain, connection)
    get_files_mock.assert_called_once()
    assert len(job_chain.queue_job_step.mock_calls) == 0
