import connections.confluence.crawler
import connections.confluence.loader
from connections.confluence.service import ConfluenceConnectionLoader
from connections.service import ConnectionLoader, ConnectionRepository
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy, Feature


class ConfluenceFeature(Feature):

    def accept_job_request(self, job_request: JobRequest) -> bool:
        return job_request.datasource == Datasource.CONFLUENCE

    def accept_job(self, job: Job) -> bool:
        return job.datasource == Datasource.CONFLUENCE

    def create_first_job_step(self, job: Job) -> JobStep:
        return JobStep(JobType.CRAWL, datasource=job.datasource, doc_id=job.doc_id, doc_display_id=job.doc_display_id)

    def get_handler(self, job: Job, job_step: JobStep):
        match job_step.type:
            case JobType.CRAWL:
                return connections.confluence.crawler.crawl_confluence
            case JobType.LOAD:
                return connections.confluence.loader.load_confluence_page
            case _:
                return None

    def get_connection_loader(self, connection_id: str,
                              connection_repository: ConnectionRepository) -> ConnectionLoader:
        return ConfluenceConnectionLoader(connection_id, connection_repository)

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return DeleteDocBy.BY_DOC_ID
