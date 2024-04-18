import urllib3
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from loguru import logger
from prometheus_fastapi_instrumentator import Instrumentator

from connections.bwf.feature import BwfFeature
from connections.confluence.feature import ConfluenceFeature
from connections.files.feature import UploadFileFeature
from connections.files.router import files_router
from connections.hkm.feature import HkmFeature
from connections.rkm.feature import RkmFeature
from connections.sharepoint.feature import SharePointFeature
from exceptions import custom_exception_handler
from health.router import health_router
from indexing.service import IndexingJobChain
from jobs.router import job_executions_router, jobs_router
from jobs.service import FeatureService, JobQueue
from middleware.cache_control import CacheControl
from middleware.content_security_policy import ContentSecurityPolicy
from middleware.content_type_options import XContentTypeOptions
from middleware.frame_options import XFrameOptions
from middleware.strict_transport import StrictTransportSecurity
from utils.logging_utils import setup_logging

setup_logging()

# getting rid of the "InsecureRequestWarning: Unverified HTTPS request is being made to host…" warnings.
urllib3.disable_warnings()

app = FastAPI(
    title="Data connection",
    description="Data connections for LLM",
    version="1.0.0"
)

# Create a Prometheus instrumentator for the FastAPI app
instrumentator = Instrumentator(
    excluded_handlers=['.*health.*', '/metrics']
).instrument(app)

app.add_exception_handler(RequestValidationError, custom_exception_handler)

app.add_middleware(GZipMiddleware, minimum_size=512)
app.add_middleware(CacheControl, directive='no-store')
app.add_middleware(ContentSecurityPolicy, policy="frame-ancestors 'none'")
app.add_middleware(StrictTransportSecurity, max_age=432000)
app.add_middleware(XContentTypeOptions)
app.add_middleware(XFrameOptions, directive="DENY")

prefix = '/api/v1.0'

app.include_router(health_router)
app.include_router(jobs_router, prefix=prefix)
app.include_router(files_router, prefix=prefix)
app.include_router(job_executions_router, prefix=prefix)

app.feature_service = FeatureService([RkmFeature(), HkmFeature(), UploadFileFeature(), BwfFeature(), SharePointFeature(), ConfluenceFeature()])
app.job_queue = JobQueue(
    feature_service=app.feature_service,
    job_chain_factory=lambda job_queue: IndexingJobChain(job_queue, app.feature_service))


@app.on_event('startup')
def create_application_index():
    """
    Attempts to verify and possibly create the OpenSearch index.
    No exception is rethrown if that fails so that the application can be started even when OpenSearch is not quite up
    yet.
    """
    from opensearch.client import get_open_search_client
    with get_open_search_client() as open_search_client:
        open_search_client.ensure_application_index_created_no_rethrow()


@app.on_event('startup')
def _enable_metrics():
    instrumentator.expose(endpoint="/metrics", app=app, include_in_schema=False)


if __name__ == "__main__":
    logger.critical("** Running in development mode. Do not run like this in production. **")
    import uvicorn

    uvicorn.run("main:app", host="localhost", port=8000, log_level="debug", reload=True)
