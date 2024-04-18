from loguru import logger

from connections.sharepoint.constants import SUPPORTED_FILES
from connections.sharepoint.models import SharePointConnection
from connections.sharepoint.service import SharePoint
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import JobChain


def crawl_sharepoint(job: Job, job_step: JobStep, chain: JobChain, connection: SharePointConnection):
    sharepoint = SharePoint()
    files = sharepoint.get_files(connection, SUPPORTED_FILES, modified_since=job.modified_since)
    if not files:
        logger.warning(
            f"no Sharepoint articles found (doc_id={job_step.doc_id})"
        )
        return

    for file in files:
        file_id = file.object_id
        library_id = file.drive_id
        logger.info(f"scheduling a LOAD job for Sharepoint file {file_id} with Library id {library_id}")
        load_job_step = JobStep(JobType.LOAD, job_step.datasource, job_id=job.id, doc_id=f"{library_id}/{file_id}")
        chain.queue_job_step(job, load_job_step, connection, execute_now=False)
    chain.execute_job_steps(job)
