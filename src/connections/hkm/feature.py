from connections.hkm.service import HkmConnectionLoader
from connections.service import ConnectionRepository, ConnectionLoader
from jobs.constants import Datasource, JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy, Feature


class HkmFeature(Feature):

    def get_connection_loader(self, connection_id: str, connection_repository: ConnectionRepository)\
            -> ConnectionLoader:
        return HkmConnectionLoader(connection_id, connection_repository)

    def accept_job_request(self, job_request: JobRequest) -> bool:
        return job_request.datasource == Datasource.HKM

    def accept_job(self, job: Job) -> bool:
        return job.datasource == Datasource.HKM

    def create_first_job_step(self, job: Job) -> JobStep:
        return JobStep(JobType.CRAWL, datasource=job.datasource, doc_id=job.doc_id, doc_display_id=job.doc_display_id)

    def get_handler(self, job: Job, job_step: JobStep):
        match job_step.type:
            case JobType.CRAWL:
                import connections.hkm.crawler
                return connections.hkm.crawler.crawl_hkm
            case JobType.LOAD:
                import connections.hkm.loader
                return connections.hkm.loader.load_hkm_article
            case JobType.SYNC_DELETIONS:
                import connections.hkm.deleter
                return connections.hkm.deleter.sync_hkm_deletions
            case JobType.DELETE:
                import connections.hkm.deleter
                return connections.hkm.deleter.delete_hkm_article
            case _:
                return None

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return DeleteDocBy.BY_DOC_ID
