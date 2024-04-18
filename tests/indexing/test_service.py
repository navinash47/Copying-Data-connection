from langchain.embeddings import SentenceTransformerEmbeddings
from langchain.schema import Document
from opensearchpy.exceptions import NotFoundError
import pytest
from pytest_mock.plugin import MockerFixture

from embeddings.service import embeddings_function
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from jobs.service import JobQueue, DeleteDocBy, FeatureService
from indexing.service import IndexingJobChain
from opensearch.client import OpenSearchClient, get_open_search_url


def test_amend_chunks_metadata(mocker):
    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(Datasource.RKM)
    chunks = [
        Document(page_content='page content 1', metadata={'attr': 'value'}),
        Document(page_content='page content 2', metadata={})
    ]

    chain.amend_chunks_metadata(job, chunks)

    assert chunks[0].page_content == 'passage: page content 1'
    assert chunks[0].metadata['chunk_id'] == 0
    assert chunks[0].metadata['datasource'] == job.datasource

    assert chunks[1].page_content == 'passage: page content 2'
    assert chunks[1].metadata['chunk_id'] == 1
    assert chunks[1].metadata['datasource'] == job.datasource


def test_amend_chunks_metadata_no_chunk(mocker):
    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(Datasource.RKM)
    chunks = []

    chain.amend_chunks_metadata(job, chunks)
    # just checking that no error is raised


def test_amend_chunks_metadata_no_prefix(mocker):
    mocker.patch('config.Settings.CHUNK_PREFIX', None)

    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(Datasource.RKM)
    chunks = [Document(page_content='page content', metadata={'attr': 'value'})]

    chain.amend_chunks_metadata(job, chunks)

    assert chunks[0].page_content == 'page content'
    assert chunks[0].metadata['chunk_id'] == 0
    assert chunks[0].metadata['datasource'] == job.datasource


def test_delete_document(mocker):
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'TEST_OPENSEARCH_INDEX')

    open_search = mocker.Mock(OpenSearchClient)
    mocker.patch('indexing.service.get_open_search_client', return_value=open_search)
    response = {'deleted': 1}
    open_search.delete_by_query.return_value = response

    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)

    chain.delete_document(open_search, 'TEST_DATASOURCE', 'metadata.doc_display_id', 'TEST_DOC_DISPLAY_ID','TEST_CONNECTION_ID' )

    open_search.delete_by_query.assert_called_once()
    index_arg = open_search.delete_by_query.mock_calls[0].kwargs['index']
    assert index_arg == 'TEST_OPENSEARCH_INDEX'
    body_arg = open_search.delete_by_query.mock_calls[0].kwargs['body']
    assert body_arg['query']['bool']['must'][0]['term']['metadata.datasource']['value'] == 'TEST_DATASOURCE'
    assert body_arg['query']['bool']['must'][1]['term']['metadata.doc_display_id']['value'] == 'TEST_DOC_DISPLAY_ID'
    assert body_arg['query']['bool']['must'][2]['terms']['metadata.connection_id'] == ['NONE', 'TEST_CONNECTION_ID']


def test_delete_document_with_no_connection_id(mocker):
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'TEST_OPENSEARCH_INDEX')

    open_search = mocker.Mock(OpenSearchClient)
    mocker.patch('indexing.service.get_open_search_client', return_value=open_search)
    response = {'deleted': 1}
    open_search.delete_by_query.return_value = response

    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)

    chain.delete_document(open_search, 'TEST_DATASOURCE', 'metadata.doc_display_id', 'TEST_DOC_DISPLAY_ID',None )

    open_search.delete_by_query.assert_called_once()
    index_arg = open_search.delete_by_query.mock_calls[0].kwargs['index']
    assert index_arg == 'TEST_OPENSEARCH_INDEX'
    body_arg = open_search.delete_by_query.mock_calls[0].kwargs['body']
    assert body_arg['query']['bool']['must'][0]['term']['metadata.datasource']['value'] == 'TEST_DATASOURCE'
    assert body_arg['query']['bool']['must'][1]['term']['metadata.doc_display_id']['value'] == 'TEST_DOC_DISPLAY_ID'
    assert body_arg['query']['bool']['must'][2]['terms']['metadata.connection_id'] == ['NONE']


def test_delete_document_with_missing_index(mocker: MockerFixture):
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'TEST_OPENSEARCH_INDEX')

    open_search = mocker.Mock(OpenSearchClient)
    mocker.patch('indexing.service.get_open_search_client', return_value=open_search)
    open_search.delete_by_query.side_effect = NotFoundError(404,
                                                            'index_not_found_exception',
                                                            'no such index [TEST_OPENSEARCH_INDEX]',
                                                            'TEST_OPENSEARCH_INDEX',
                                                            'index_or_alias')

    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)

    # there should be no exception
    chain.delete_document(open_search, 'TEST_DATASOURCE', 'metadata.doc_id', 'TEST_DOC_ID', 'TEST_CONNECTION_ID')

    open_search.delete_by_query.assert_called_once()


def test_delete_with_404_other_than_missing_index(mocker: MockerFixture):
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'TEST_OPENSEARCH_INDEX')

    open_search = mocker.Mock(OpenSearchClient)
    mocker.patch('indexing.service.get_open_search_client', return_value=open_search)
    not_found_error = NotFoundError(404,
                                    'unknown_endpoint',
                                    'endpoint nof found: /make_coffee',
                                    '/make_coffee',
                                    'endpoint_not_found')
    open_search.delete_by_query.side_effect = not_found_error

    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)

    try:
        chain.delete_document(open_search, 'TEST_DATASOURCE', 'metadata.doc_id', 'TEST_DOC_ID', 'TEST_CONNECTION_ID')
        assert False  # should not reach this line
    except NotFoundError as error:
        assert error == not_found_error

    open_search.delete_by_query.assert_called_once()


def test_delete_chunks_documents_no_chunk(mocker):
    delete_document_mock = mocker.patch('indexing.service.IndexingJobChain.delete_document', return_value=None)
    open_search = mocker.Mock(OpenSearchClient)

    job_queue = mocker.Mock(JobQueue)
    mocker.patch('jobs.service.FeatureService.get_delete_doc_by', return_value=DeleteDocBy.BY_DOC_ID)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(Datasource.RKM)
    job_step = JobStep(JobType.LOAD, job.datasource)
    chunks = []

    chain.delete_chunks_documents(open_search, job, job_step, chunks)

    delete_document_mock.assert_not_called()


@pytest.mark.parametrize(
    'delete_doc_by,delete_doc_by_key_field,delete_doc_by_key_values',
    [
        (DeleteDocBy.BY_DOC_ID, 'metadata.doc_id', ['TEST_DOC_A', 'TEST_DOC_B', 'TEST_DOC_D']),
        (DeleteDocBy.BY_DOC_DISPLAY_ID,
         'metadata.doc_display_id',
         ['TEST_DOC_DISPLAY_A', 'TEST_DOC_DISPLAY_B', 'TEST_DOC_DISPLAY_E']),
    ]
)
def test_delete_chunks_documents(
        mocker: MockerFixture,
        delete_doc_by: DeleteDocBy,
        delete_doc_by_key_field: str,
        delete_doc_by_key_values: [str]):
    datasource = 'TEST_DATASOURCE'

    delete_document_mock = mocker.patch('indexing.service.IndexingJobChain.delete_document', return_value=None)
    open_search = mocker.Mock(OpenSearchClient)
    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    feature_service.get_delete_doc_by.return_value = delete_doc_by

    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(datasource)
    job_step = JobStep(JobType.LOAD, job.datasource)
    # Chunk #1 and chunk #2 have the same doc_id. Chunk #3 has a different doc_id.
    # ==> 2 deletions should occur (1 for chunk #1 and #2, 1 for chunk #3).
    chunks = [
        Document(
            page_content='chunk content 1',
            metadata={'datasource': datasource, 'doc_id': 'TEST_DOC_A', 'doc_display_id': 'TEST_DOC_DISPLAY_A'}),
        Document(
            page_content='chunk content 2',
            metadata={'datasource': datasource, 'doc_id': 'TEST_DOC_A', 'doc_display_id': 'TEST_DOC_DISPLAY_A'}),
        Document(
            page_content='chunk content 3',
            metadata={'datasource': datasource, 'doc_id': 'TEST_DOC_B', 'doc_display_id': 'TEST_DOC_DISPLAY_B'}),
        # Chunk 4 should always be skipped because it doesn't have a doc ID or doc display ID.
        Document(page_content='chunk content 4', metadata={'datasource': datasource}),
        # Chunk 5 should only be skipped when the doc display ID is required.
        Document(page_content='chunk content 5', metadata={'datasource': datasource, 'doc_id': 'TEST_DOC_D'}),
        # Chunk 6 should only be skipped when the doc ID is required.
        Document(
            page_content='chunk content 6',
            metadata={'datasource': datasource, 'doc_display_id': 'TEST_DOC_DISPLAY_E'}),
    ]

    chain.delete_chunks_documents(open_search, job, job_step, chunks)

    feature_service.get_delete_doc_by.assert_called_once_with(job, job_step)
    assert len(delete_document_mock.mock_calls) == len(delete_doc_by_key_values)
    delete_document_mock.assert_has_calls([
        mocker.call(open_search, datasource, delete_doc_by_key_field, delete_doc_by_key_value, None)
        for delete_doc_by_key_value in delete_doc_by_key_values
    ])


def test_delete_chunks_documents_without_doc_id(mocker: MockerFixture):
    datasource = 'TEST_DATASOURCE'

    delete_document_mock = mocker.patch('indexing.service.IndexingJobChain.delete_document', return_value=None)
    open_search = mocker.Mock(OpenSearchClient)
    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    feature_service.get_delete_doc_by.return_value = DeleteDocBy.BY_DOC_ID

    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(datasource)
    job_step = JobStep(JobType.LOAD, job.datasource)
    chunks = [
        Document(page_content='chunk content', metadata={'datasource': datasource, 'doc_id': None}),
    ]

    chain.delete_chunks_documents(open_search, job, job_step, chunks)

    delete_document_mock.assert_not_called()


def test_store_chunks(mocker: MockerFixture):
    embeddings = [[-0.010464120656251907], [1212121212]]
    datasource = 'TEST_DATASOURCE'
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'TEST_OPENSEARCH_INDEX')
    from_embeddings_mock = mocker.patch('langchain.vectorstores.OpenSearchVectorSearch.from_embeddings',
                                        return_value=None)
    mocker.patch.object(SentenceTransformerEmbeddings, "embed_documents", return_value=embeddings)
    job_queue = mocker.Mock(JobQueue)
    feature_service = mocker.Mock(FeatureService)
    chain = IndexingJobChain(job_queue, feature_service)
    job = Job(datasource)
    chunks = [
        Document(
            page_content='chunk content 1',
            metadata={'datasource': datasource, 'doc_id': 'TEST_DOC_A', 'doc_display_id': 'TEST_DOC_DISPLAY_A'})
    ]
    chain.store_chunks(job, chunks)
    from_embeddings_mock.assert_called_once_with(embeddings,
                                                 ['chunk content 1'],
                                                 embeddings_function,
                                                 metadatas=[{'datasource': 'TEST_DATASOURCE', 'doc_id': 'TEST_DOC_A',
                                                             'doc_display_id': 'TEST_DOC_DISPLAY_A'}],
                                                 bulk_size=3,
                                                 opensearch_url=get_open_search_url(),
                                                 http_compress=True,
                                                 index_name="TEST_OPENSEARCH_INDEX",
                                                 verify_certs=False
                                                 )
