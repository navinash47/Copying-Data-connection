from langchain.schema import Document
from loguru import logger

from connections.bwf.models import BwfConnection
from connections.bwf.schemas import BwfArticle
from connections.bwf.service import Bwf
from indexing.models import DocumentMetadata
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from utils.i18n_utils import standardize_language_tag
from utils.text_utils import clean_text


def create_bwf_document(datasource: str, article: BwfArticle, content: str, connection_id: str, company: str | None):
    document = Document(
        page_content=content,
        metadata=DocumentMetadata(
            doc_id=article.uuid,
            doc_display_id=article.content_id,
            source=f"{datasource}/{article.template_name}/{article.content_id}",
            connection_id=connection_id,
            title=article.title,
            internal=not article.external,
            language=standardize_language_tag(article.locale, default_language_tag=None),
            company=company
        ).to_dict()
    )
    return document


def load_bwf_article(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: BwfConnection) -> None:
    logger.info("loading BWF article {id}", id=job_step.doc_id)
    with Bwf(connection) as bwf:
        article = bwf.get_article(job_step.doc_id)
        if article:
            company = bwf.get_article_company(article.uuid)

    if not article:
        logger.info(f"skipping loading BWF article '{job_step.doc_id}'")
        return

    title = clean_text(article.title)
    content_concatenation =\
        ' '.join([f"{clean_text(content.label)}={clean_text(content.content)}" for content in article.contents])
    content = f"Title={title} doc_display_id={article.content_id} {content_concatenation}"
    connection_id = connection.id if connection else None
    document = create_bwf_document(job_step.datasource, article, content, connection_id, company)
    chain.index_documents(job, job_step, [document])
