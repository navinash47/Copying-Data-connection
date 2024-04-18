from O365 import Connection
from O365.drive import DriveItem
from langchain.schema import Document
from pytest_mock import MockerFixture

from connections.sharepoint.loader import load_sharepoint_article
from connections.sharepoint.models import SharePointConnection
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


def test_load_sharepoint(mocker: MockerFixture):
    job = Job(Datasource.SHAREPOINT)
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id="test_library_id/test_file_id")
    job_chain = mocker.Mock(IndexingJobChain)
    connection = SharePointConnection(
        id='connection-id', client_id='cid', client_secret='shh', site='a', tenant_id='tid', tenant_name='tname')
    documents = [Document(page_content='test_content', metadata={'source': 'test_source'})]
    mock_file = mocker.Mock(DriveItem)
    mock_file.object_id = 'unique-object-id'
    mock_file.name = 'test.pdf'
    mock_file.web_url = 'test_url'
    mocker.patch('connections.sharepoint.service.SharePoint.get_file', return_value=mock_file)
    mocker.patch('connections.sharepoint.loader.get_documents_from_file', return_value=documents)
    load_sharepoint_article(job, job_step, job_chain, connection)
    job_chain.index_documents.assert_called()
    assert len(job_chain.index_documents.mock_calls) == 1
    assert job_chain.index_documents.mock_calls[0].args[0] == job
    assert job_chain.index_documents.mock_calls[0].args[1] == job_step
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert documents_arg[0].metadata['doc_id'] == 'unique-object-id'
    assert documents_arg[0].metadata['title'] == 'test.pdf'
    assert documents_arg[0].metadata['connection_id'] == 'connection-id'
    assert documents_arg[0].metadata['source'] == 'SPT/test_library_id/test_file_id'
    assert documents_arg[0].metadata['web_url'] == 'test_url'


def test_load_sharepoint_no_file(mocker: MockerFixture):
    job = Job(Datasource.SHAREPOINT)
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id="test_library_id/test_file_id")
    job_chain = mocker.Mock(IndexingJobChain)
    connection = mocker.Mock(Connection)
    mocker.patch('connections.sharepoint.service.SharePoint.get_file', return_value=None)
    load_sharepoint_article(job, job_step, job_chain, connection)
    job_chain.index_documents.assert_not_called()
