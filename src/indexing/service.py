import time
from typing import List

from langchain.docstore.document import Document
from langchain.vectorstores import OpenSearchVectorSearch
from loguru import logger
from opensearchpy.exceptions import OpenSearchException, NotFoundError
from prometheus_client import Summary

from chunking.service import generate_chunks
from config import Settings
from embeddings.service import embeddings_function
from jobs.models import Job, JobStep
from jobs.service import JobChain, JobQueue, FeatureService
from opensearch.client \
    import OpenSearchClient, get_open_search_client, get_open_search_url, ERROR_INDEX_NOT_FOUND_EXCEPTION

indexed_documents = Summary('indexed_documents', 'Summary of indexed documents')


class IndexingJobChain(JobChain):

    def __init__(self, job_queue: JobQueue, feature_service: FeatureService):
        JobChain.__init__(self, job_queue)
        self.__feature_service = feature_service

    @indexed_documents.time()
    def index_documents(self, job: Job, job_step: JobStep, documents: List[Document]) -> None:
        chunks = generate_chunks(documents, 500, 100)
        self.amend_chunks_metadata(job, chunks)
        with get_open_search_client() as open_search_client:
            open_search_client.ensure_application_index_created()
            self.delete_chunks_documents(open_search_client, job, job_step, chunks)
        self.store_chunks(job, chunks)

    def amend_chunks_metadata(self, job: Job, chunks):
        for chunk_id, chunk in enumerate(chunks):
            # Some models require prefixing the indexed documents/chunks in a certain manner (related to the way the
            # model was trained).
            if Settings.CHUNK_PREFIX:
                chunk.page_content = Settings.CHUNK_PREFIX + chunk.page_content

            chunk.metadata['datasource'] = job.datasource
            chunk.metadata['chunk_id'] = chunk_id

    def store_chunks(self, job: Job, chunks: List[Document]):
        logger.debug("Storing chunks for datasource: '{datasource}'", datasource=job.datasource)
        texts = [chunk.page_content for chunk in chunks]
        metadatas = [chunk.metadata for chunk in chunks]
        embeddings = embeddings_function.embed_documents(texts)
        OpenSearchVectorSearch.from_embeddings(
            embeddings,
            texts,
            embeddings_function,
            metadatas=metadatas,
            bulk_size=len(embeddings)+1,  # it ensures that embeddings are greater thank bulk_size otherwise it'll fail
            opensearch_url=get_open_search_url(),
            http_compress=True,  # enables gzip compression for request bodies
            index_name=Settings.OPENSEARCH_INDEX,
            verify_certs=Settings.OPENSEARCH_VERIFY_CERTIFICATES
        )

    def delete_chunks_documents(
            self, open_search_client: OpenSearchClient,  job: Job, job_step: JobStep, chunks: List[Document]):
        already_deleted_keys = set()
        delete_doc_by = self.__feature_service.get_delete_doc_by(job, job_step)

        for chunk_id, chunk in enumerate(chunks):
            doc_id = chunk.metadata.get('doc_id')
            doc_display_id = chunk.metadata.get('doc_display_id')
            key_field, key_value = delete_doc_by.pick_key_for_delete(doc_id, doc_display_id)

            if key_value:
                if (key_field, key_value) in already_deleted_keys:
                    continue
                else:
                    self.delete_document(open_search_client, job.datasource, key_field, key_value, job.connection_id)
                    already_deleted_keys.add((key_field, key_value))
            else:
                # This shouldn't happen but let's log something.
                logger.warning(
                    "couldn't determine the key to use in order to delete the OpenSearch document"
                    " for chunk {datasource} {chunk_id} ({metadata})",
                    datasource=job_step.datasource, chunk_id=chunk_id, metadata=chunk.metadata)

    def delete_document(
            self,
            open_search_client: OpenSearchClient,
            datasource: str,
            delete_by_key_field: str,
            delete_by_key_value: str,
            delete_by_connection_id: str):
        connection_ids = ['NONE', delete_by_connection_id] if delete_by_connection_id else ['NONE']
        logger.trace(
            "deleting OpenSearch documents for datasource {datasource} and {key_field}={key_value} and connection="
            "{connection}",
            datasource=datasource,
            key_field=delete_by_key_field,
            key_value=delete_by_key_value,
            connection=connection_ids)
        try:
            start_time = time.time()

            response = open_search_client.delete_by_query(
                index=Settings.OPENSEARCH_INDEX,
                body={
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'metadata.datasource': {'value': datasource}}},
                                {'term': {delete_by_key_field: {'value': delete_by_key_value}}},
                                {'terms': {'metadata.connection_id': connection_ids}},
                            ]
                        }
                    }
                }
            )
            logger.debug(
                "deleted {deleted} OpenSearch documents"
                " for datasource '{datasource}' and {key_field} '{key_value}' and connection {connection} ({time}s)",
                deleted=response['deleted'],
                datasource=datasource,
                key_field=delete_by_key_field,
                key_value=delete_by_key_value,
                connection=connection_ids,
                time=time.time() - start_time)
        except NotFoundError as e:
            # If the index doesn't exist then there is nothing to delete in it.
            if e.error != ERROR_INDEX_NOT_FOUND_EXCEPTION:
                raise e
        except OpenSearchException as e:
            logger.error(
                "failed deleting OpenSearch documents"
                " for datasource '{datasource}' and {key_field} '{key_value}': {cause}",
                datasource=datasource, key_field=delete_by_key_field, key_value=delete_by_key_value, cause=e)
            raise e
