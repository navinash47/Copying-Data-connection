import connections.files.file_loader
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy, Feature


class UploadFileFeature(Feature):

    def accept_job_request(self, job_request: JobRequest) -> bool:
        return bool(job_request.upload_file)

    def create_job(self, job_request: JobRequest) -> Job:
        return Job(
            datasource=job_request.datasource,
            upload_file=job_request.upload_file,
            doc_id=job_request.docId,
            doc_display_id=job_request.docDisplayId,
            connection_id=job_request.connectionId)

    def accept_job(self, job: Job) -> bool:
        return bool(job.upload_filename)

    def create_first_job_step(self, job: Job) -> JobStep:
        return JobStep(JobType.LOAD, datasource=job.datasource, doc_id=job.doc_id, doc_display_id=job.doc_display_id)

    def get_handler(self, job: Job, job_step: JobStep):
        if job_step.type == JobType.LOAD:
            return connections.files.file_loader.load_upload_file
        else:
            return None

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return DeleteDocBy.BY_DOC_ID
