import tempfile
from datetime import datetime
from pathlib import Path

from langchain.schema import Document
from loguru import logger

from connections.confluence.models import ConfluenceConnection
from connections.confluence.schemas import AttachmentMetaData, ConfluencePage
from connections.confluence.service import ConfluenceService
from indexing.models import DocumentMetadata
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from utils.file_types_utils import SUPPORTED_CONTENT_TYPES
from utils.langchain_utils import get_langchain_loader


def load_confluence_page(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: ConfluenceConnection):
    logger.info(f"Loading Confluence page {job_step.doc_id}")

    with ConfluenceService(connection) as confluence_service:
        page = confluence_service.get_page(job_step.doc_id)
        if is_page_indexable(page, job.modified_since):
            index_page(job, job_step, chain, page, connection.id)
            load_page_attachments(job, job_step, chain, page, confluence_service, connection.id)


def index_page(job: Job, job_step: JobStep, chain: IndexingJobChain, page: ConfluencePage, connection_id: str):
    document = Document(
        page_content=f"Title={page.title} {page.content}",
        metadata=DocumentMetadata(
            doc_id=page.id_,
            title=page.title,
            web_url=page.web_url,
            source=f"{job_step.datasource}/{page.space_name}/{page.id_}",
            connection_id=connection_id
        ).to_dict()
    )
    chain.index_documents(job, job_step, [document])


def load_page_attachments(job: Job,
                          job_step: JobStep,
                          chain: IndexingJobChain,
                          page: ConfluencePage,
                          confluence_service: ConfluenceService,
                          connection_id: str):
    logger.info(f"Loading attachments for Confluence page_id:{job_step.doc_id}")
    with confluence_service:
        attachments_metadata = confluence_service.get_page_attachments_metadata(page)
        with tempfile.TemporaryDirectory() as temp_dir_name:
            for attachment in attachments_metadata:
                if is_attachment_indexable(page.id_, attachment, job.modified_since):
                    load_attachment(job, job_step, chain, attachment, confluence_service, temp_dir_name, connection_id)


def load_attachment(job: Job,
                    job_step: JobStep,
                    chain: IndexingJobChain,
                    attachment: AttachmentMetaData,
                    confluence_service: ConfluenceService,
                    temp_dir_name: str,
                    connection_id: str):
    logger.info(f"Loading attachment_id {attachment.id_} with page_id:{job_step.doc_id}")
    temp_file_path = Path(temp_dir_name) / attachment.id_
    confluence_service.download_attachment(attachment, str(temp_file_path))
    loader = get_langchain_loader(attachment.mime_type, str(temp_file_path))
    langchain_document = loader.load()
    for document in langchain_document:
        DocumentMetadata(
            doc_id=attachment.id_,
            title=attachment.title,
            source=f"{job_step.datasource}{attachment.source}",
            connection_id=connection_id,
            web_url=f"{attachment.web_url}"
        ).apply_to(document)
    chain.index_documents(job, job_step, langchain_document)


def is_page_indexable(page: ConfluencePage, modified_since: datetime) -> bool:
    if not page.title:
        logger.info(f"Skip Confluence page_id:{page.id_} title is empty")
        return False
    if not page.content:
        logger.info(f"Skip Confluence page_id:{page.id_} content is empty")
        return False
    if modified_since and modified_since >= page.last_modified:
        logger.info(f"Skip Confluence page_id:{page.id_}, it is not updated after:{modified_since}")
        return False
    return True


def is_attachment_indexable(page_id: str, attachment: AttachmentMetaData, modified_since: datetime) -> bool:
    if attachment.mime_type not in SUPPORTED_CONTENT_TYPES:
        logger.error(f"Skip Confluence attachment_id:{attachment.id_} page_id:{page_id} has unsupported file type")
        return False
    if attachment.status != "current":
        logger.info(f"Skip Confluence attachment_id:{attachment.id_} page_id:{page_id} has incorrect status")
        return False
    if modified_since and modified_since >= attachment.last_modified:
        logger.info(f"Skip Confluence attachment_id:{attachment.id_}, it is not updated after:{modified_since}")
        return False
    return True
