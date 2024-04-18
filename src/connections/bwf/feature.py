from connections.bwf.service import BwfConnectionLoader
from connections.service import ConnectionRepository, ConnectionLoader
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy, Feature


class BwfFeature(Feature):

    def get_connection_loader(self,
                              connection_id: str,
                              connection_repository: ConnectionRepository) -> ConnectionLoader:
        return BwfConnectionLoader(connection_id, connection_repository)

    def accept_job_request(self, job_request: JobRequest) -> bool:
        return job_request.datasource == Datasource.BWF

    def accept_job(self, job: Job) -> bool:
        return job.datasource == Datasource.BWF

    def create_first_job_step(self, job: Job) -> JobStep:
        return JobStep(JobType.CRAWL, datasource=job.datasource, doc_id=job.doc_id, doc_display_id=job.doc_display_id)

    def get_handler(self, job: Job, job_step: JobStep):
        match job_step.type:
            case JobType.CRAWL:
                import connections.bwf.crawler
                return connections.bwf.crawler.crawl_bwf
            case JobType.LOAD:
                import connections.bwf.loader
                return connections.bwf.loader.load_bwf_article
            case JobType.SYNC_DELETIONS:
                import connections.bwf.deleter
                return connections.bwf.deleter.sync_bwf_deletions
            case JobType.DELETE:
                import connections.bwf.deleter
                return connections.bwf.deleter.delete_bwf_knowledge_article
            case _:
                return None

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return DeleteDocBy.BY_DOC_DISPLAY_ID
