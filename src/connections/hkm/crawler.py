from loguru import logger

from connections.hkm.models import HkmConnection
from connections.hkm.service import Hkm
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import JobChain
from utils.object_utils import int_defaulted_to_none


def crawl_hkm(job: Job, job_step: JobStep, chain: JobChain, connection: HkmConnection):
    logger.info("Crawling HKM articles")

    with Hkm(connection) as hkm:
        article_ids = hkm.get_article_ids(int_defaulted_to_none(job.doc_id))

    if article_ids:
        for article_id in article_ids:
            logger.info(f"scheduling a LOAD job for HKM article with content id {article_id}")
            load_job_step = JobStep(JobType.LOAD, job.datasource, job_id=job.id, doc_id=str(article_id))
            chain.queue_job_step(job, load_job_step, connection, execute_now=False)
    else:
        logger.info("found no HKM published articles to load")

    chain.queue_sync_deletions_if_configured(job, connection)
    chain.execute_job_steps(job)
