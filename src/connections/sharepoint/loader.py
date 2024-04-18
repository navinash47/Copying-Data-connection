import tempfile
from pathlib import Path
from typing import List

from O365.drive import DriveItem
from langchain.schema import Document
from loguru import logger

from connections.sharepoint.constants import CHUNK_SIZE
from connections.sharepoint.models import SharePointConnection
from connections.sharepoint.service import SharePoint
from indexing.models import DocumentMetadata
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from utils.langchain_utils import get_langchain_loader
from utils.text_utils import clean_text


def _parse_library_id_and_file_id_from_doc_id(doc_id: str) -> (str, str):
    details = doc_id.split('/')
    if len(details) < 2:
        raise RuntimeError(f"Library id and file id not found, doc_id {doc_id} has a wrong format")
    library_id = details[0]
    file_id = details[1]
    return library_id, file_id


def get_documents_from_file(file: DriveItem) -> List[Document]:
    temporary_directory = tempfile.TemporaryDirectory()
    temporary_file = Path(temporary_directory.name) / file.name
    try:
        with open(temporary_file, mode="wb") as output:
            file.download(output=output, chunk_size=CHUNK_SIZE)
        loader = get_langchain_loader(file.mime_type, str(temporary_file))
        documents = loader.load()
        return documents
    finally:
        temporary_file.unlink(missing_ok=True)


def load_sharepoint_article(job: Job, job_step: JobStep, chain: IndexingJobChain,
                            connection: SharePointConnection) -> None:
    logger.info("loading Sharepoint file with {id}", id=job_step.doc_id)
    sharepoint = SharePoint()
    library_id, file_id = _parse_library_id_and_file_id_from_doc_id(job_step.doc_id)
    file = sharepoint.get_file(connection, library_id, file_id)
    if not file:
        logger.info(f"skipping loading Sharepoint file with id '{job_step.doc_id}', file not found.")
        return
    documents = get_documents_from_file(file)
    for document in documents:
        DocumentMetadata(
            doc_id=file.object_id,
            source=f"{job_step.datasource}/{job_step.doc_id}",
            connection_id=connection.id,
            title=file.name,
            web_url=file.web_url
        ).apply_to(document)
        document.page_content = f"Title={clean_text(file.name)} Content={clean_text(document.page_content)}"
    chain.index_documents(job, job_step, documents)
