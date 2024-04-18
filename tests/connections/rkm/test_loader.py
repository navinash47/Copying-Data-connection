import pytest
from pytest_mock import MockerFixture

from connections.rkm.loader import load_rkm_knowledge_article
from connections.rkm.models import KnowledgeArticle, RkmConnection
from connections.rkm.service import Rkm
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


@pytest.fixture
def connection():
    return RkmConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


def test_load_rkm_knowledge_article_with_reference(mocker: MockerFixture, connection: RkmConnection):
    job = Job(Datasource.RKM)
    job_step = JobStep(JobType.LOAD, Datasource.RKM, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.rkm.loader.Rkm.jwt_login', return_value=jwt_token)

    article = KnowledgeArticle(
        Rkm.FORM_REFERENCE_TEMPLATE,
        fk_guid='TEST_FK_GUID',
        display_id='TEST_DOC_DISPLAY_ID',
        title='TEST_TITLE',
        company='TEST_COMPANY',
        internal=True,
        language='English')
    mocker.patch('connections.rkm.loader.Rkm.get_knowledge_article', return_value=article)

    reference_details = {'Reference': 'TEST_REFERENCE_CONTENT'}
    mocker.patch('connections.rkm.loader.Rkm.get_reference', return_value=reference_details)

    load_rkm_knowledge_article(job, job_step, job_chain, connection)

    job_chain.index_documents.assert_called_once()
    job_arg = job_chain.index_documents.mock_calls[0].args[0]
    assert job_arg == job
    job_step_arg = job_chain.index_documents.mock_calls[0].args[1]
    assert job_step_arg == job_step
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert reference_details['Reference'] in documents_arg[0].page_content
    assert documents_arg[0].metadata['doc_id'] == 'TEST_DOC_ID'
    assert documents_arg[0].metadata['doc_display_id'] == 'TEST_DOC_DISPLAY_ID'
    assert documents_arg[0].metadata['title'] == 'TEST_TITLE'
    assert documents_arg[0].metadata['company'] == 'TEST_COMPANY'
    assert documents_arg[0].metadata['internal']
    assert documents_arg[0].metadata['language'] == 'en'
    assert documents_arg[0].metadata['source'] == 'RKM/RKM:ReferenceTemplate/TEST_DOC_ID'
    assert documents_arg[0].metadata['connection_id'] == 'CONNECTION_ID'


def test_load_rkm_knowledge_article_with_reference_no_reference_content(
        mocker: MockerFixture, connection: RkmConnection):
    job = Job(Datasource.RKM)
    job_step = JobStep(JobType.LOAD, Datasource.RKM, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.rkm.loader.Rkm.jwt_login', return_value=jwt_token)

    article = KnowledgeArticle(
        Rkm.FORM_REFERENCE_TEMPLATE,
        fk_guid='TEST_FK_GUID',
        display_id='TEST_DOC_DISPLAY_ID',
        title='TEST_TITLE',
        company='TEST_COMPANY',
        internal=True,
        language='Japanese')
    mocker.patch('connections.rkm.loader.Rkm.get_knowledge_article', return_value=article)

    reference_details = {'Reference': ''}  # empty reference content
    mocker.patch('connections.rkm.loader.Rkm.get_reference', return_value=reference_details)

    load_rkm_knowledge_article(job, job_step, job_chain, connection)

    job_chain.index_documents.assert_not_called()


def test_load_rkm_knowledge_article_with_reference_no_title(mocker: MockerFixture, connection: RkmConnection):
    job = Job(Datasource.RKM)
    job_step = JobStep(JobType.LOAD, Datasource.RKM, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.rkm.loader.Rkm.jwt_login', return_value=jwt_token)

    article = KnowledgeArticle(
        Rkm.FORM_REFERENCE_TEMPLATE,
        fk_guid='TEST_FK_GUID',
        display_id='TEST_DOC_DISPLAY_ID',
        title='',  # no title
        company='TEST_COMPANY',
        internal=True,
        language='Chinese Traditional')
    mocker.patch('connections.rkm.loader.Rkm.get_knowledge_article', return_value=article)

    get_reference_mock = mocker.patch('connections.rkm.loader.Rkm.get_reference', return_value=None)

    load_rkm_knowledge_article(job, job_step, job_chain, connection)

    get_reference_mock.assert_not_called()
    job_chain.index_documents.assert_not_called()


def test_load_rkm_knowledge_article_with_no_connection(mocker: MockerFixture):
    job = Job(Datasource.RKM)
    job_step = JobStep(JobType.LOAD, Datasource.RKM, doc_id='TEST_DOC_ID')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.rkm.loader.Rkm.jwt_login', return_value=jwt_token)

    article = KnowledgeArticle(
        Rkm.FORM_REFERENCE_TEMPLATE,
        fk_guid='TEST_FK_GUID',
        display_id='TEST_DOC_DISPLAY_ID',
        title='TEST_TITLE',
        company='TEST_COMPANY',
        internal=True,
        language='English')
    mocker.patch('connections.rkm.loader.Rkm.get_knowledge_article', return_value=article)

    reference_details = {'Reference': 'TEST_REFERENCE_CONTENT'}
    mocker.patch('connections.rkm.loader.Rkm.get_reference', return_value=reference_details)

    load_rkm_knowledge_article(job, job_step, job_chain, None)

    job_chain.index_documents.assert_called_once()
    job_arg = job_chain.index_documents.mock_calls[0].args[0]
    assert job_arg == job
    job_step_arg = job_chain.index_documents.mock_calls[0].args[1]
    assert job_step_arg == job_step
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert reference_details['Reference'] in documents_arg[0].page_content
    assert documents_arg[0].metadata['doc_id'] == 'TEST_DOC_ID'
    assert documents_arg[0].metadata['doc_display_id'] == 'TEST_DOC_DISPLAY_ID'
    assert documents_arg[0].metadata['title'] == 'TEST_TITLE'
    assert documents_arg[0].metadata['company'] == 'TEST_COMPANY'
    assert documents_arg[0].metadata['internal']
    assert documents_arg[0].metadata['language'] == 'en'
    assert documents_arg[0].metadata['source'] == 'RKM/RKM:ReferenceTemplate/TEST_DOC_ID'
    assert 'connection_id' not in documents_arg[0].metadata
