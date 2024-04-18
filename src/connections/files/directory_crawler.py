import glob
import os

from loguru import logger

from config import Settings
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import JobChain


def crawl_directory(job: Job, job_step: JobStep, chain: JobChain):
    for file in _find_all_files(Settings.FS_DATA_SOURCE_DIR, Settings.FS_DATA_SOURCE_PATTERN.split(',')):
        logger.info(f"scheduling a LOAD job for file {file}")
        load_job = JobStep(JobType.LOAD, job.datasource, job_id=job.id, file=file)
        chain.queue_job_step(job, load_job, execute_now=False)
    chain.execute_job_steps(job)


def _find_all_files(dir_path: str, patterns: [str]):
    logger.info(f"find all files in {dir_path}")
    for filename_pattern in patterns:
        for filepath in glob.iglob(filename_pattern, root_dir=dir_path, recursive=True):
            full_path = os.path.join(dir_path, filepath)
            yield full_path


