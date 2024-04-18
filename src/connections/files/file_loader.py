import tempfile
from os.path import relpath
from pathlib import Path

from langchain.document_loaders import PyPDFLoader
from loguru import logger

from config import Settings
from connections.models import Connection
from helixplatform import data_connection_job
from helixplatform.service import InnovationSuite
from indexing.models import DocumentMetadata
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from utils.langchain_utils import get_langchain_loader


def load_file(job: Job, job_step: JobStep, chain: IndexingJobChain) -> None:
    logger.info(f"file loading: {job}")
    loader = PyPDFLoader(job.file)
    documents = loader.load()

    for document in documents:
        relative_path = relpath(document.metadata['source'], Settings.FS_DATA_SOURCE_DIR)
        DocumentMetadata(source=relative_path, doc_id=relative_path).apply_to(document)

    chain.index_documents(job, job_step, documents)


# Currently, both methods could be doing the same but
# at some point if we use s3 bucket or anything else there will be more logic
def load_uri(job: Job, job_step: JobStep, chain: IndexingJobChain) -> None:
    if not job.doc_id:
        raise ValueError(f"`doc_id` must be specified when loading from URI ({job.uri})")

    logger.info(f"URI loading: {job}")
    loader = PyPDFLoader(job.uri)
    documents = loader.load()

    for document in documents:
        DocumentMetadata(doc_id=job.doc_id).apply_to(document)

    chain.index_documents(job, job_step, documents)


def load_upload_file(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: Connection) -> None:
    logger.info(f"file upload loading {job_step}")

    with InnovationSuite() as is_client:
        attachment_file = is_client.get_attachment(data_connection_job.FORM, job.id, data_connection_job.FIELD_FILE)

    if not attachment_file.filename:
        logger.warning('skipping loading upload file: no filename specified (job step: {job_step})', job_step=job_step)
        return

    temporary_directory = tempfile.TemporaryDirectory()
    temporary_file = Path(temporary_directory.name) / attachment_file.filename

    with open(temporary_file, mode="wb") as file:
        file.write(attachment_file.content)

    loader = get_langchain_loader(attachment_file.content_type, str(temporary_file))
    documents = loader.load()
    for document in documents:
        doc_id = job_step.doc_id or attachment_file.filename
        DocumentMetadata(
            source=f"{job_step.datasource}/{doc_id}",
            doc_id=doc_id,
            doc_display_id=job_step.doc_display_id if job_step.doc_display_id else None,
            title=attachment_file.filename
        ).apply_to(document)
    chain.index_documents(job, job_step, documents)

    temporary_file.unlink(missing_ok=True)
