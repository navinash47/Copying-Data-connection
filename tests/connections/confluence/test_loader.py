from datetime import datetime
from io import StringIO

import pytest
from langchain.document_loaders.base import BaseLoader
from langchain.schema import Document
from loguru import logger
from pytest_mock import MockerFixture

from connections.confluence.loader import load_confluence_page, load_attachment
from connections.confluence.loader import load_page_attachments
from connections.confluence.schemas import AttachmentMetaData, ConfluencePage
from connections.confluence.service import ConfluenceConnection, ConfluenceService
from indexing.service import IndexingJobChain
from jobs.constants import JobType, Datasource
from jobs.models import Job, JobStep


def setup_loguru_memory_sink():
    buffer = StringIO()
    handler_id = logger.add(buffer, format="{message}")
    return buffer, handler_id


@pytest.fixture
def setup_test_environment(mocker):
    job = Job(Datasource.CONFLUENCE)
    job_step = JobStep(JobType.CRAWL, datasource=job.datasource, doc_id='77')
    job_chain = mocker.Mock(IndexingJobChain)
    confluence_connection = ConfluenceConnection(id="connection-1", page_id="", url="", access_token="")
    confluence_page = ConfluencePage(
        id_='124',
        title='title',
        content='content',
        space_name='ns',
        base_url='https://base_url_test',
        web_url='https://base_url_test/display/NameSpace/ConfluencePage',
        last_modified=datetime.strptime('2024-01-26T17:31:36.287-06:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )

    # Proper way to mock a context manager
    confluence_service_mock = mocker.MagicMock(ConfluenceService)
    confluence_service_mock.__enter__.return_value = confluence_service_mock
    confluence_service_mock.__exit__.return_value = None

    attachment_meta_data = AttachmentMetaData(id_='456', title='title.pdf', source="/source", mime_type='application/pdf',
                                              status='current', download_link='', web_url='https://example.com/ns/title.pdf',
                                              last_modified=datetime.strptime('2024-01-25T13:21:36.287-06:00',
                                                                              '%Y-%m-%dT%H:%M:%S.%f%z'))

    return (job, job_step, job_chain, confluence_connection, confluence_page,
            attachment_meta_data, confluence_service_mock)


def test_load_confluence_page_successful(mocker: MockerFixture, setup_test_environment):
    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment

    load_page_attachments_mock = mocker.patch('connections.confluence.loader.load_page_attachments')
    get_page_content_mock = mocker.patch('connections.confluence.service.ConfluenceService.get_page',
                                         return_value=confluence_page)

    load_confluence_page(job, job_step, job_chain, confluence_connection)

    assert len(load_page_attachments_mock.mock_calls) == 1
    assert len(get_page_content_mock.mock_calls) == 1
    assert len(job_chain.index_documents.mock_calls) == 1
    assert job_chain.index_documents.call_args[0][0] == job
    assert job_chain.index_documents.call_args[0][1] == job_step
    indexed_document = job_chain.index_documents.call_args[0][2][0]
    assert indexed_document.metadata.get('doc_id') == '124'
    assert indexed_document.metadata.get('title') == 'title'
    assert indexed_document.metadata.get('web_url') == 'https://base_url_test/display/NameSpace/ConfluencePage'
    assert indexed_document.metadata.get('source') == 'CNF/ns/124'
    assert indexed_document.metadata.get('connection_id') == 'connection-1'


def test_load_confluence_page_modified_before_modified_since(mocker: MockerFixture, setup_test_environment):
    log_buffer, handler_id = setup_loguru_memory_sink()

    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment
    modified_since = datetime.strptime("2024-01-30T13:20:00.000Z", '%Y-%m-%dT%H:%M:%S.%f%z')
    job.modified_since = modified_since

    load_page_attachments_mock = mocker.patch('connections.confluence.loader.load_page_attachments')
    get_page_content_mock = mocker.patch('connections.confluence.service.ConfluenceService.get_page',
                                         return_value=confluence_page)

    load_confluence_page(job, job_step, job_chain, confluence_connection)

    assert load_page_attachments_mock.call_count == 0
    assert len(get_page_content_mock.mock_calls) == 1
    assert len(job_chain.index_documents.mock_calls) == 0

    log_buffer.seek(0)
    log_contents = log_buffer.read()
    assert 'Skip Confluence page_id:124, it is not updated after:2024-01-30 13:20:00+00:00' in log_contents
    logger.remove(handler_id)




def test_load_confluence_page_title_absent(mocker: MockerFixture, setup_test_environment):
    log_buffer, handler_id = setup_loguru_memory_sink()

    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment
    confluence_page.title = ''

    mock_load_page_attachments = mocker.patch('connections.confluence.loader.load_page_attachments')
    mock_get_page_content = mocker.patch('connections.confluence.service.ConfluenceService.get_page',
                                         return_value=confluence_page)

    load_confluence_page(job, job_step, job_chain, confluence_connection)

    assert len(mock_get_page_content.mock_calls) == 1
    job_chain.index_documents.assert_not_called()
    mock_load_page_attachments.assert_not_called()
    log_buffer.seek(0)
    log_contents = log_buffer.read()
    assert 'Skip Confluence page_id:124 title is empty' in log_contents
    logger.remove(handler_id)


def test_load_confluence_page_content_empty(mocker: MockerFixture, setup_test_environment):
    log_buffer, handler_id = setup_loguru_memory_sink()

    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment
    confluence_page.content = ''

    mock_load_page_attachments = mocker.patch('connections.confluence.loader.load_page_attachments')
    mock_get_page_content = mocker.patch('connections.confluence.service.ConfluenceService.get_page',
                                         return_value=confluence_page)

    load_confluence_page(job, job_step, job_chain, confluence_connection)

    assert len(mock_get_page_content.mock_calls) == 1
    job_chain.index_documents.assert_not_called()
    mock_load_page_attachments.assert_not_called()
    log_buffer.seek(0)
    log_contents = log_buffer.read()
    assert 'Skip Confluence page_id:124 content is empty' in log_contents
    logger.remove(handler_id)


def test_load_confluence_page_get_page_content_error(mocker: MockerFixture, setup_test_environment):
    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment

    mocker.patch('connections.confluence.crawler.ConfluenceService.get_page', side_effect=Exception("Exception"))

    with pytest.raises(Exception) as execution_info:
        load_confluence_page(job, job_step, job_chain, confluence_connection)

    assert "Exception" in str(execution_info.value)


def test_load_page_attachments(mocker: MockerFixture, setup_test_environment):
    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment

    attachment_meta_data_2 = attachment_meta_data
    attachment_meta_data_2.id_ = 'attachment_meta_data_id_2'

    load_attachment_mock = mocker.patch('connections.confluence.loader.load_attachment')

    mocker.patch('connections.confluence.service.ConfluenceService', return_value=confluence_service_mock)
    confluence_service_mock.get_page_attachments_metadata.return_value = [attachment_meta_data, attachment_meta_data_2]

    load_page_attachments(job, job_step, job_chain, confluence_page, confluence_service_mock, confluence_connection.id)

    assert load_attachment_mock.call_count == 2
    assert load_attachment_mock.call_args[0][0] == job
    assert load_attachment_mock.call_args[0][1] == job_step
    assert load_attachment_mock.call_args[0][2] == job_chain
    assert load_attachment_mock.call_args[0][3] == attachment_meta_data_2
    confluence_service_mock.get_page_attachments_metadata.assert_called_once_with(confluence_page)


def test_load_page_attachments_incorrect_mime_type(mocker: MockerFixture, setup_test_environment):
    log_buffer, handler_id = setup_loguru_memory_sink()

    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment
    attachment_meta_data.mime_type = 'unsupported_mime_type'
    load_attachment_mock = mocker.patch('connections.confluence.loader.load_attachment')

    mocker.patch('connections.confluence.service.ConfluenceService', return_value=confluence_service_mock)

    confluence_service_mock.get_page_attachments_metadata.return_value = [attachment_meta_data]

    load_page_attachments(job, job_step, job_chain, confluence_page, confluence_service_mock, confluence_connection.id)

    confluence_service_mock.get_page_attachments_metadata.assert_called_once_with(confluence_page)
    assert load_attachment_mock.call_count == 0
    assert job_chain.index_documents.call_count == 0

    log_buffer.seek(0)
    log_contents = log_buffer.read()
    assert 'Skip Confluence attachment_id:456 page_id:124 has unsupported file type' in log_contents
    logger.remove(handler_id)


def test_load_page_attachments_incorrect_status(mocker: MockerFixture, setup_test_environment):
    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment
    log_buffer, handler_id = setup_loguru_memory_sink()

    attachment_meta_data.status = 'deprecated_status'
    load_attachment_mock = mocker.patch('connections.confluence.loader.load_attachment')

    mocker.patch('connections.confluence.service.ConfluenceService', return_value=confluence_service_mock)

    confluence_service_mock.get_page_attachments_metadata.return_value = [attachment_meta_data]

    load_page_attachments(job, job_step, job_chain, confluence_page, confluence_service_mock, confluence_connection.id)

    confluence_service_mock.get_page_attachments_metadata.assert_called_once_with(confluence_page)
    assert load_attachment_mock.call_count == 0
    assert job_chain.index_documents.call_count == 0

    log_buffer.seek(0)
    log_contents = log_buffer.read()
    assert 'Skip Confluence attachment_id:456 page_id:124 has incorrect status' in log_contents
    logger.remove(handler_id)


def test_load_attachment(mocker: MockerFixture, setup_test_environment):
    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment
    mock_temp_dir = '/mock/temp/dir'
    base_loader = mocker.Mock(BaseLoader)
    document = Document
    base_loader.load.return_value = [document]

    get_langchain_loader_mock = mocker.patch('connections.confluence.loader.get_langchain_loader',
                                             return_value=base_loader)

    load_attachment(job, job_step, job_chain, attachment_meta_data, confluence_service_mock, mock_temp_dir,
                    confluence_connection.id)

    assert get_langchain_loader_mock.call_count == 1
    confluence_service_mock.download_attachment.assert_called_once_with(attachment_meta_data,
                                                                        mock_temp_dir + '/' + attachment_meta_data.id_)
    assert len(job_chain.index_documents.mock_calls) == 1

    assert job_chain.index_documents.call_args[0][0] == job
    assert job_chain.index_documents.call_args[0][1] == job_step
    indexed_document = job_chain.index_documents.call_args[0][2][0]
    assert indexed_document.metadata.get('doc_id') == '456'
    assert indexed_document.metadata.get('title') == 'title.pdf'
    assert indexed_document.metadata.get('web_url') == 'https://example.com/ns/title.pdf'
    assert indexed_document.metadata.get('source') == 'CNF/source'
    assert indexed_document.metadata.get('connection_id') == 'connection-1'


def test_load_confluence_attachment_modified_before_modified_since(mocker: MockerFixture, setup_test_environment):
    job, job_step, job_chain, confluence_connection, confluence_page, \
        attachment_meta_data, confluence_service_mock, = setup_test_environment

    mocker.patch('connections.confluence.service.ConfluenceService', return_value=confluence_service_mock)

    confluence_service_mock.get_page_attachments_metadata.return_value = [attachment_meta_data]

    log_buffer, handler_id = setup_loguru_memory_sink()

    modified_since = datetime.strptime("2024-01-30T13:20:00.000Z", '%Y-%m-%dT%H:%M:%S.%f%z')
    job.modified_since = modified_since

    load_page_attachments(job, job_step, job_chain, confluence_page, confluence_service_mock, confluence_connection.id)
    assert len(job_chain.index_documents.mock_calls) == 0

    log_buffer.seek(0)
    log_contents = log_buffer.read()
    assert 'Skip Confluence attachment_id:456, it is not updated after:2024-01-30 13:20:00+00:00' in log_contents
    logger.remove(handler_id)

