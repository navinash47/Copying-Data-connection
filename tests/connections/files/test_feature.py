from connections.files.feature import UploadFileFeature
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.service import DeleteDocBy


def test_delete_doc_by():
    job = Job('test datasource')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert UploadFileFeature().get_delete_doc_by(job, job_step) == DeleteDocBy.BY_DOC_ID
