from loguru import logger

from connections.bwf.models import BwfConnection
from connections.bwf.service import Bwf
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import JobChain


def crawl_bwf(job: Job, job_step: JobStep, chain: JobChain, connection: BwfConnection):
    logger.info("Crawling BWF articles")

    with Bwf(connection) as bwf:
        article_ids = [job.doc_id] if job.doc_id else bwf.get_article_ids(display_id=job_step.doc_display_id,
                                                                          modified_since=job.modified_since)

    if article_ids:
        for article_id in article_ids:
            logger.info(f"scheduling a LOAD job for BWF article with article_id {article_id}")
            load_job_step = JobStep(JobType.LOAD, job.datasource, job_id=job.id, doc_id=article_id)
            chain.queue_job_step(job, load_job_step, connection, execute_now=False)
    else:
        logger.info("no BWF published articles found")

    chain.queue_sync_deletions_if_configured(job, connection)
    chain.execute_job_steps(job)
