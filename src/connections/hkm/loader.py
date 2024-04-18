from typing import List

from langchain.schema import Document
from loguru import logger

from connections.hkm.models import HkmConnection
from connections.hkm.service import Hkm
from indexing.models import DocumentMetadata
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from utils import i18n_utils
from utils.text_utils import clean_text


def create_hkm_document(datasource: str,
                        content_id: int,
                        title: str,
                        content: str,
                        language: str,
                        connection_id: str,
                        tags: List[str] | None = None):
    document = Document(
        page_content=content,
        metadata=DocumentMetadata(
            doc_id=str(content_id),
            source=f"{datasource}/{content_id}",
            connection_id=connection_id,
            title=title,
            language=language,
            tags=tags if tags else None
            # 'internal':  TODO:// look around if there is a way to know if the article is private or public regarding
            #  the permissions or role of the user who is querying the api
            # 'company': TODO:// look for tags attribute in the api and investigate if null.. means open to everyone
            #  since sometimes we can get the name of the company in that attribute
        ).to_dict()
    )
    return document


def load_hkm_article(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: HkmConnection) -> None:
    logger.info("loading HKM article {id}", id=job_step.doc_id)
    with Hkm(connection) as hkm:
        article = hkm.get_article(int(job_step.doc_id))

    if not article:
        logger.info(f"skipping loading HKM article '{job_step.doc_id}'")
        return

    if not article.translations:
        logger.info('skipping loading HKM article {content_id} because it contains no translation',
                    content_id=job_step.doc_id)

    for translation in article.translations:
        title = clean_text(translation.title)
        issue = clean_text(translation.issue)
        environment = clean_text(translation.environment)
        resolution = clean_text(translation.resolution)
        cause = clean_text(translation.cause)
        language = i18n_utils.standardize_language_tag(translation.culture, default_language_tag=None)
        content = f"Title={title} Issue={issue} Environment={environment} Resolution={resolution} Cause={cause}"
        connection_id = connection.id if connection else None
        # details about the root cause
        document = create_hkm_document(
            job_step.datasource, article.content_id, title, content, language, connection_id, translation.tags)
        chain.index_documents(job, job_step, [document])
