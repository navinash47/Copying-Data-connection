from connections.rkm.service import RkmConnectionLoader
from connections.service import ConnectionRepository, ConnectionLoader
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy, Feature


class RkmFeature(Feature):

    def get_connection_loader(self,
                              connection_id: str,
                              connection_repository: ConnectionRepository) -> ConnectionLoader:
        return RkmConnectionLoader(connection_id, connection_repository)

    def accept_job_request(self, job_request: JobRequest) -> bool:
        return job_request.datasource == Datasource.RKM

    def accept_job(self, job: Job) -> bool:
        return job.datasource == Datasource.RKM

    def create_first_job_step(self, job: Job) -> JobStep:
        return JobStep(JobType.CRAWL, datasource=job.datasource, doc_id=job.doc_id, doc_display_id=job.doc_display_id)

    def get_handler(self, job: Job, job_step: JobStep):
        match job_step.type:
            case JobType.CRAWL:
                import connections.rkm.crawler
                return connections.rkm.crawler.crawl_rkm
            case JobType.LOAD:
                import connections.rkm.loader
                return connections.rkm.loader.load_rkm_knowledge_article
            case JobType.SYNC_DELETIONS:
                import connections.rkm.deleter
                return connections.rkm.deleter.sync_rkm_deletions
            case JobType.DELETE:
                import connections.rkm.deleter
                return connections.rkm.deleter.delete_rkm_knowledge_article
            case _:
                return None

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return DeleteDocBy.BY_DOC_DISPLAY_ID
