from pytest_mock import MockerFixture

from connections.files.file_loader import load_upload_file
from connections.models import Connection
from helixplatform import data_connection_job
from helixplatform.models import Attachment
from indexing.service import IndexingJobChain
from jobs.constants import JobType
from jobs.models import Job, JobStep


def test_load_upload_file(mocker: MockerFixture):
    job = Job('file_datasource')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)
    connection = mocker.Mock(Connection)

    attachment = Attachment(b'test file content', 'test_filename.txt', 'text/plain')
    get_attachment_mock = mocker.patch('helixplatform.service.InnovationSuite.get_attachment', return_value=attachment)

    load_upload_file(job, job_step, job_chain, connection)

    get_attachment_mock.assert_called_once_with(data_connection_job.FORM, job.id, data_connection_job.FIELD_FILE)
    job_chain.index_documents.assert_called_once()
    job_arg = job_chain.index_documents.mock_calls[0].args[0]
    assert job_arg == job
    job_step_arg = job_chain.index_documents.mock_calls[0].args[1]
    assert job_step_arg == job_step
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert documents_arg[0].page_content == 'test file content'
    assert documents_arg[0].metadata['doc_id'] == 'TEST_DOC_ID'
    assert documents_arg[0].metadata['source'] == 'file_datasource/TEST_DOC_ID'
    assert documents_arg[0].metadata['title'] == 'test_filename.txt'


def test_load_upload_file_but_no_filename_with_attachment(mocker: MockerFixture):
    job = Job('file_datasource')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)
    connection = mocker.Mock(Connection)

    attachment = Attachment(b'test file content', filename=None, content_type='text/plain', )
    logger_warning_mock = mocker.patch('loguru.logger.warning')
    get_attachment_mock = mocker.patch('helixplatform.service.InnovationSuite.get_attachment', return_value=attachment)

    load_upload_file(job, job_step, job_chain, connection)

    get_attachment_mock.assert_called_once()
    logger_warning_mock.assert_called_once()
    assert 'skipping' in logger_warning_mock.mock_calls[0].args[0]
