import pytest
from pytest_mock import MockerFixture

from connections.bwf.loader import load_bwf_article
from connections.bwf.models import BwfConnection
from connections.bwf.schemas import BwfArticle
from connections.models import Connection
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


@pytest.fixture
def connection():
    return BwfConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


def test_load_bwf_article_with_connection(mocker: MockerFixture, connection: BwfConnection):
    job = Job(Datasource.BWF)
    job_step = JobStep(JobType.LOAD, Datasource.BWF, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    article = BwfArticle(
        uuid="TEST_UUiD",
        content_id="TEST_CONTENT_ID",
        template_name="TEST_TEMPLATE_NAME",
        title="TEST_TITLE",
        contents=[],
        external=False,
        locale='en'
    )
    mocker.patch('connections.bwf.loader.Bwf.get_article', return_value=article)
    get_article_company_mock = \
        mocker.patch('connections.bwf.loader.Bwf.get_article_company', return_value='Petramco')

    load_bwf_article(job, job_step, job_chain, connection)

    get_article_company_mock.assert_called_once_with('TEST_UUiD')
    job_chain.index_documents.assert_called_once()
    job_arg = job_chain.index_documents.mock_calls[0].args[0]
    assert job_arg == job
    job_step_arg = job_chain.index_documents.mock_calls[0].args[1]
    assert job_step_arg == job_step
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert documents_arg[0].metadata['doc_id'] == 'TEST_UUiD'
    assert documents_arg[0].metadata['doc_display_id'] == 'TEST_CONTENT_ID'
    assert documents_arg[0].metadata['title'] == 'TEST_TITLE'
    assert documents_arg[0].metadata['internal']
    assert documents_arg[0].metadata['language'] == 'en'
    assert documents_arg[0].metadata['source'] == 'BWF/TEST_TEMPLATE_NAME/TEST_CONTENT_ID'
    assert documents_arg[0].metadata['connection_id'] == 'CONNECTION_ID'
    assert documents_arg[0].metadata['company'] == 'Petramco'


def test_load_bwf_article_without_connection(mocker: MockerFixture):
    job = Job(Datasource.BWF)
    job_step = JobStep(JobType.LOAD, Datasource.BWF, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    article = BwfArticle(
        uuid="TEST_UUiD",
        content_id="TEST_CONTENT_ID",
        template_name="TEST_TEMPLATE_NAME",
        title="TEST_TITLE",
        contents=[],
        external=False,
        locale='en'
    )
    mocker.patch('connections.bwf.loader.Bwf.get_article', return_value=article)
    get_article_company_mock = mocker.patch('connections.bwf.loader.Bwf.get_article_company', return_value='Petramco')

    load_bwf_article(job, job_step, job_chain, None)

    get_article_company_mock.assert_called_once_with('TEST_UUiD')
    job_chain.index_documents.assert_called_once()
    job_arg = job_chain.index_documents.mock_calls[0].args[0]
    assert job_arg == job
    job_step_arg = job_chain.index_documents.mock_calls[0].args[1]
    assert job_step_arg == job_step
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert documents_arg[0].metadata['doc_id'] == 'TEST_UUiD'
    assert documents_arg[0].metadata['doc_display_id'] == 'TEST_CONTENT_ID'
    assert documents_arg[0].metadata['title'] == 'TEST_TITLE'
    assert documents_arg[0].metadata['internal']
    assert documents_arg[0].metadata['language'] == 'en'
    assert documents_arg[0].metadata['source'] == 'BWF/TEST_TEMPLATE_NAME/TEST_CONTENT_ID'
    assert 'connection_id' not in documents_arg[0].metadata
    assert documents_arg[0].metadata['company'] == 'Petramco'
