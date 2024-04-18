from langchain.schema import Document
from loguru import logger

from connections.rkm.constants import from_rkm_language_to_ietf_language_tag
from connections.rkm.models import KnowledgeArticle, RkmConnection
from connections.rkm.service import Rkm
from indexing.models import DocumentMetadata
from indexing.service import IndexingJobChain
from jobs.models import Job, JobStep
from utils.text_utils import clean_text


def load_rkm_how_to(job: Job,
                    job_step: JobStep,
                    chain: IndexingJobChain,
                    rkm: Rkm,
                    article: KnowledgeArticle,
                    connection_id: str) -> None:
    if not article.title:  # however improbable…
        logger.info(f"skipping loading RKM how-to '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty title")
        return

    how_to_details = rkm.get_how_to(article.fk_guid)

    question = how_to_details['RKMTemplateQuestion']
    question = clean_text(question)
    answer = how_to_details['RKMTemplateAnswer']
    answer = clean_text(answer)

    if not answer:
        logger.info(f"skipping loading RKM how-to '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty answer after cleanup")
        return

    technical_notes = how_to_details['RKMTemplateTechnicianNotes']
    technical_notes = clean_text(technical_notes)

    content = f'Title={article.title} Question={question} doc_display_id={article.display_id} {answer}' \
              f' Technical Notes={technical_notes}'

    document = create_rkm_document(job_step, article, content, connection_id)
    chain.index_documents(job, job_step, [document])


def load_rkm_problem_solution(job: Job,
                              job_step: JobStep,
                              chain: IndexingJobChain,
                              rkm: Rkm,
                              article: KnowledgeArticle,
                              connection_id: str) -> None:
    if not article.title:  # however improbable…
        logger.info(f"skipping loading RKM problem-solution '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty title")
        return

    problem_solution_details = rkm.get_problem_solution(article.fk_guid)

    problem = problem_solution_details['RKMTemplateProblem']
    problem = clean_text(problem)
    solution = problem_solution_details['RKMTemplateSolution']
    solution = clean_text(solution)

    if not solution:
        logger.info(f"skipping loading RKM problem-solution '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty solution after cleanup")
        return

    technical_notes = problem_solution_details['RKMTemplateTechnicianNotes']
    technical_notes = clean_text(technical_notes)

    content = f'Title={article.title} Question={problem} doc_display_id={article.display_id} {solution}' \
              f' Technical Notes={technical_notes}'

    document = create_rkm_document(job_step, article, content, connection_id)
    chain.index_documents(job, job_step, [document])


def load_rkm_known_error(job: Job,
                         job_step: JobStep,
                         chain: IndexingJobChain,
                         rkm: Rkm,
                         article: KnowledgeArticle,
                         connection_id: str) -> None:
    if not article.title:  # however improbable…
        logger.info(f"skipping loading RKM known error '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty title")
        return

    known_error_details = rkm.get_known_error(article.fk_guid)

    fix = known_error_details['RKMTemplateFix']
    fix = clean_text(fix)

    if not fix:
        logger.info(f"skipping loading RKM known error '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty fix after cleanup")
        return

    error = known_error_details['RKMTemplateError']
    error = clean_text(error)
    root_cause = known_error_details['RKMTemplateRootCause']
    root_cause = clean_text(root_cause)
    technical_notes = known_error_details['RKMTemplateTechnicianNotes']
    technical_notes = clean_text(technical_notes)

    content = f'Title={article.title} Error={error} doc_display_id={article.display_id} Root Cause={root_cause}' \
              f' Fix={fix} Technical Notes={technical_notes}'

    document = create_rkm_document(job_step, article, content, connection_id)
    chain.index_documents(job, job_step, [document])


def load_rkm_reference(job: Job,
                       job_step: JobStep,
                       chain: IndexingJobChain,
                       rkm: Rkm,
                       article: KnowledgeArticle,
                       connection_id: str) -> None:
    if not article.title:  # however improbable…
        logger.info(f"skipping loading RKM reference '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty title")
        return

    reference_details = rkm.get_reference(article.fk_guid)

    reference = reference_details['Reference']
    reference = clean_text(reference)

    if not reference:
        logger.info(f"skipping loading RKM reference '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty reference after cleanup")
        return

    content = f'Title={article.title} doc_display_id={article.display_id} Reference={reference}'

    document = create_rkm_document(job_step, article, content, connection_id)
    chain.index_documents(job, job_step, [document])


def load_rkm_kcs(job: Job,
                 job_step: JobStep,
                 chain: IndexingJobChain,
                 rkm: Rkm,
                 article: KnowledgeArticle,
                 connection_id: str) -> None:

    if not article.title:  # however improbable…
        logger.info(f"skipping loading RKM KCS '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty title")
        return

    kcs_details = rkm.get_kcs(article.fk_guid)

    problem = kcs_details['RKMTemplateKCSProblem']
    problem = clean_text(problem)
    environment = kcs_details['RKMTemplateEnvironment']
    environment = clean_text(environment)
    resolution = kcs_details['RKMTemplateResolution']
    resolution = clean_text(resolution)
    cause = kcs_details['RKMTemplateCause']
    cause = clean_text(cause)

    if not problem:
        logger.info(f"skipping loading RKM KCS '{job_step.doc_id}' ({article.form}, {article.display_id}):"
                    f" empty problem after cleanup")
        return

    content = f'Title={article.title} doc_display_id={article.display_id}' \
              f' Problem={problem} Environment={environment} Resolution={resolution} Cause={cause}'

    document = create_rkm_document(job_step, article, content, connection_id)
    chain.index_documents(job, job_step, [document])


def create_rkm_document(job_step: JobStep, article: KnowledgeArticle, content: str, connection_id: str):
    document_language = from_rkm_language_to_ietf_language_tag(article.language, default_language_tag=None)
    document = Document(
        page_content=content,
        metadata=DocumentMetadata(
            doc_id=job_step.doc_id,
            doc_display_id=article.display_id,
            source=f"RKM/{article.form}/{job_step.doc_id}",
            connection_id=connection_id,
            title=article.title,
            internal=article.internal,
            company=article.company,
            language=document_language,
        ).to_dict()
    )
    return document


def load_rkm_knowledge_article(job: Job, job_step: JobStep, chain: IndexingJobChain, connection: RkmConnection) -> None:
    logger.info("loading RKM article {id}", id=job_step.doc_id)
    with Rkm(connection) as rkm:
        article = rkm.get_knowledge_article(job_step.doc_id)

        if not article.form:
            logger.info(f"skipping loading RKM article '{job_step.doc_id}': it doesn't have an 'ArticleForm'")
            return

        if not article.fk_guid:
            logger.info(f"skipping loading RKM article '{job_step.doc_id}' ({article.form}): it doesn't have a 'FK_GUID'")
            return

        connection_id = connection.id if connection else None
        if article.form == Rkm.FORM_HOW_TO_TEMPLATE:
            load_rkm_how_to(job, job_step, chain, rkm, article, connection_id)
        elif article.form == Rkm.FORM_PROBLEM_SOLUTION_TEMPLATE:
            load_rkm_problem_solution(job, job_step, chain, rkm, article, connection_id)
        elif article.form == Rkm.FORM_KNOWN_ERROR_TEMPLATE:
            load_rkm_known_error(job, job_step, chain, rkm, article, connection_id)
        elif article.form == Rkm.FORM_REFERENCE_TEMPLATE:
            load_rkm_reference(job, job_step, chain, rkm, article, connection_id)
        elif article.form == Rkm.FORM_KCS:
            load_rkm_kcs(job, job_step, chain, rkm, article, connection_id)
        else:
            logger.info(f"skipping loading RKM article '{job_step.doc_id}': unsupported article form: {article.form}")
