from loguru import logger

from connections.confluence.models import ConfluenceConnection
from connections.confluence.service import ConfluenceService
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import JobChain


def crawl_confluence(job: Job, job_step: JobStep, chain: JobChain, connection: ConfluenceConnection):
    with ConfluenceService(connection) as confluenceService:
        page_id = connection.page_id
        logger.info(f"Crawling Confluence page id {page_id}")

        if not page_id:
            logger.warning("Confluence page id not defined")
            return

    for current_page_id in confluenceService.get_page_with_all_child_ids([page_id]):
        logger.info(f"Scheduling a LOAD job for page with id {current_page_id}")
        load_job_step = JobStep(JobType.LOAD, job.datasource, job_id=job.id, doc_id=current_page_id)
        chain.queue_job_step(job, load_job_step, connection, execute_now=False)
    chain.execute_job_steps(job)
