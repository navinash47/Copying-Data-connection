from connections.confluence.crawler import crawl_confluence
from connections.confluence.feature import ConfluenceFeature
from connections.confluence.loader import load_confluence_page
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import DeleteDocBy


def test_delete_doc_by():
    job = Job('CNF')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert ConfluenceFeature().get_delete_doc_by(job, job_step) == DeleteDocBy.BY_DOC_ID


def test_accept_job():
    job = Job('CNF')
    assert ConfluenceFeature().accept_job(job)


def test_not_accept_job():
    job = Job('!CNF')
    assert not ConfluenceFeature().accept_job(job)


def test_create_first_job_step():
    job = Job('CNF', doc_id="DOC_ID", doc_display_id="DOC_DISPLAY_ID")
    job_step = JobStep(JobType.CRAWL, job.datasource, doc_id=job.doc_id, display_id=job.doc_display_id)
    result_job_step = ConfluenceFeature().create_first_job_step(job)
    assert result_job_step.job_id == job_step.job_id
    assert result_job_step.datasource == job_step.datasource
    assert result_job_step.doc_id == job_step.doc_id
    assert result_job_step.doc_display_id == job_step.display_id


def test_get_handler_with_crawl():
    job = Job('CNF')
    job_step = JobStep(JobType.CRAWL, job.datasource)
    assert ConfluenceFeature().get_handler(job, job_step) == crawl_confluence


def test_get_handler_with_crawl_loader():
    job = Job('CNF')
    job_step = JobStep(JobType.LOAD, job.datasource)
    assert ConfluenceFeature().get_handler(job, job_step) == load_confluence_page


def test_get_handler_with_load():
    job = Job('CNF')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert ConfluenceFeature().get_handler(job, job_step) == load_confluence_page
