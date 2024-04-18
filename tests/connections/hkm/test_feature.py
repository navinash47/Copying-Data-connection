import pytest

from connections.hkm.crawler import crawl_hkm
from connections.hkm.deleter import sync_hkm_deletions, delete_hkm_article
from connections.hkm.feature import HkmFeature
from connections.hkm.loader import load_hkm_article
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import DeleteDocBy


def test_delete_doc_by():
    job = Job('HKM')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert HkmFeature().get_delete_doc_by(job, job_step) == DeleteDocBy.BY_DOC_ID


@pytest.mark.parametrize('job_step,handler', [
    (JobStep(JobType.CRAWL, 'HKM'), crawl_hkm),
    (JobStep(JobType.LOAD, 'HKM', doc_id='DOC_ID'), load_hkm_article),
    (JobStep(JobType.SYNC_DELETIONS, 'HKM'), sync_hkm_deletions),
    (JobStep(JobType.DELETE, 'HKM', doc_id='DOC_ID'), delete_hkm_article),
])
def test_get_handler(job_step: JobStep, handler):
    feature = HkmFeature()
    job = Job(job_step.datasource)
    assert feature.get_handler(job, job_step) == handler
