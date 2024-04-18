from connections.sharepoint.crawler import crawl_sharepoint
from connections.sharepoint.loader import load_sharepoint_article
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy, Feature
from connections.service import ConnectionLoader, ConnectionRepository
from connections.sharepoint.service import SharePointConnectionLoader


class SharePointFeature(Feature):

    def accept_job_request(self, job_request: JobRequest) -> bool:
        return job_request.datasource == Datasource.SHAREPOINT

    def create_job(self, job_request: JobRequest) -> Job:
        return Job(datasource=job_request.datasource, doc_id=job_request.docId,
                   doc_display_id=job_request.docDisplayId, modified_since=job_request.modifiedSince)

    def accept_job(self, job: Job) -> bool:
        return job.datasource == Datasource.SHAREPOINT

    def create_first_job_step(self, job: Job) -> JobStep:
        return JobStep(JobType.CRAWL, datasource=job.datasource, doc_id=job.doc_id, doc_display_id=job.doc_display_id)

    def get_handler(self, job: Job, job_step: JobStep):
        match job_step.type:
            case JobType.CRAWL:
                return crawl_sharepoint
            case JobType.LOAD:
                return load_sharepoint_article
            case _:
                return None

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return DeleteDocBy.BY_DOC_ID

    def get_connection_loader(self, connection_id: str,
                              connection_repository: ConnectionRepository) -> ConnectionLoader:
        return SharePointConnectionLoader(connection_id, connection_repository)
