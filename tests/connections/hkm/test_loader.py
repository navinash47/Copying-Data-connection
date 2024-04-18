import pytest
from pytest_mock import MockerFixture

from connections.hkm.loader import load_hkm_article
from connections.hkm.models import HkmConnection
from connections.hkm.schemas import HkmArticle, HkmArticleTranslation
from indexing.service import IndexingJobChain
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep


@pytest.fixture
def connection() -> HkmConnection:
    return HkmConnection(id='CONNECTION_ID', user='USER')


def test_load_hkm_article(mocker: MockerFixture, connection):
    job = Job(Datasource.HKM)
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='123')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.hkm.loader.Hkm.jwt_login', return_value=jwt_token)

    article = HkmArticle(
        123,
        [
            HkmArticleTranslation(
                'published',
                'en_US',
                'TITLE_en_US',
                'ISSUE_en_US',
                'ENVIRONMENT_en_US',
                'RESOLUTION_en_US',
                'CAUSE_en_US',
                ['TAG1_en_US', 'TAG2_en_US']),
            HkmArticleTranslation(
                'published',
                'zh_CN',
                'TITLE_zh_CN',
                'ISSUE_zh_CN',
                'ENVIRONMENT_zh_CN',
                'RESOLUTION_zh_CN',
                'CAUSE_zh_CN',
                ['TAG1_zh_CN', 'TAG2_zh_CN']),
        ])
    mocker.patch('connections.hkm.loader.Hkm.get_article', return_value=article)

    load_hkm_article(job, job_step, job_chain, connection)

    job_chain.index_documents.assert_called()
    assert len(job_chain.index_documents.mock_calls) == 2
    for i in range(1, 2):
        assert job_chain.index_documents.mock_calls[i].args[0] == job
        assert job_chain.index_documents.mock_calls[i].args[1] == job_step

    # 1st document
    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert article.translations[0].title in documents_arg[0].page_content
    assert article.translations[0].issue in documents_arg[0].page_content
    assert article.translations[0].environment in documents_arg[0].page_content
    assert article.translations[0].resolution in documents_arg[0].page_content
    assert article.translations[0].cause in documents_arg[0].page_content
    assert documents_arg[0].metadata['doc_id'] == '123'
    assert documents_arg[0].metadata['title'] == 'TITLE_en_US'
    assert documents_arg[0].metadata['source'] == 'HKM/123'
    assert documents_arg[0].metadata['connection_id'] == 'CONNECTION_ID'
    assert documents_arg[0].metadata['language'] == 'en-US'
    assert documents_arg[0].metadata['tags'] == ['TAG1_en_US', 'TAG2_en_US']

    # 2nd document
    documents_arg = job_chain.index_documents.mock_calls[1].args[2]
    assert len(documents_arg) == 1
    assert article.translations[1].title in documents_arg[0].page_content
    assert article.translations[1].issue in documents_arg[0].page_content
    assert article.translations[1].environment in documents_arg[0].page_content
    assert article.translations[1].resolution in documents_arg[0].page_content
    assert article.translations[1].cause in documents_arg[0].page_content
    assert documents_arg[0].metadata['doc_id'] == '123'
    assert documents_arg[0].metadata['title'] == 'TITLE_zh_CN'
    assert documents_arg[0].metadata['source'] == 'HKM/123'
    assert documents_arg[0].metadata['connection_id'] == 'CONNECTION_ID'
    assert documents_arg[0].metadata['language'] == 'zh-CN'
    assert documents_arg[0].metadata['tags'] == ['TAG1_zh_CN', 'TAG2_zh_CN']


def test_load_hkm_article_which_doesnt_exist(mocker: MockerFixture, connection):
    job = Job(Datasource.HKM)
    job_step = JobStep(JobType.LOAD, Datasource.RKM, doc_id='123')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.hkm.loader.Hkm.jwt_login', return_value=jwt_token)
    mocker.patch('connections.hkm.loader.Hkm.get_article', return_value=None)

    load_hkm_article(job, job_step, job_chain, connection)

    job_chain.index_documents.assert_not_called()


def test_load_hkm_article_without_connection(mocker: MockerFixture):
    job = Job(Datasource.HKM)
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='123')
    job_chain = mocker.Mock(IndexingJobChain)

    jwt_token = 'TEST_JWT_TOKEN'
    mocker.patch('connections.hkm.loader.Hkm.jwt_login', return_value=jwt_token)

    article = HkmArticle(
        123,
        [
            HkmArticleTranslation(
                'published',
                'en_US',
                'TITLE_en_US',
                'ISSUE_en_US',
                'ENVIRONMENT_en_US',
                'RESOLUTION_en_US',
                'CAUSE_en_US',
                ['TAG1_en_US', 'TAG2_en_US'])
        ])
    mocker.patch('connections.hkm.loader.Hkm.get_article', return_value=article)

    load_hkm_article(job, job_step, job_chain, None)

    job_chain.index_documents.assert_called()

    documents_arg = job_chain.index_documents.mock_calls[0].args[2]
    assert len(documents_arg) == 1
    assert article.translations[0].title in documents_arg[0].page_content
    assert article.translations[0].issue in documents_arg[0].page_content
    assert article.translations[0].environment in documents_arg[0].page_content
    assert article.translations[0].resolution in documents_arg[0].page_content
    assert article.translations[0].cause in documents_arg[0].page_content
    assert documents_arg[0].metadata['doc_id'] == '123'
    assert documents_arg[0].metadata['title'] == 'TITLE_en_US'
    assert documents_arg[0].metadata['source'] == 'HKM/123'
    assert 'connection_id' not in documents_arg[0].metadata
    assert documents_arg[0].metadata['language'] == 'en-US'
    assert documents_arg[0].metadata['tags'] == ['TAG1_en_US', 'TAG2_en_US']
