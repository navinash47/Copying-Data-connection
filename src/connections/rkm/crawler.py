from loguru import logger

from connections.rkm.models import RkmConnection
from connections.rkm.service import Rkm
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import JobChain


def crawl_rkm(job: Job, job_step: JobStep, chain: JobChain, connection: RkmConnection):
    with Rkm(connection) as rkm:
        articles = rkm.list_published_knowledge_articles(
                instance_ids=[job_step.doc_id] if job_step.doc_id else None,
                display_ids=[job_step.doc_display_id] if job_step.doc_display_id else None,
                modified_since=job.modified_since)

        if articles:
            # load job steps
            for article in articles:
                article_id = article['InstanceId']
                logger.info(f"scheduling a LOAD job for RKM KA {article_id}")
                load_job_step = JobStep(JobType.LOAD, job_step.datasource, job_id=job.id, doc_id=article_id)
                chain.queue_job_step(job, load_job_step, connection, execute_now=False)
        else:
            logger.warning(
                f"no RKM articles to index (doc_id={job_step.doc_id}, doc_display_id={job_step.doc_display_id})")

    chain.queue_sync_deletions_if_configured(job, connection)
    chain.execute_job_steps(job)
