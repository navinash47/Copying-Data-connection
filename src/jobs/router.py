from fastapi import APIRouter, status
from starlette.requests import Request

from .schemas import JobRequest, JobResponse, JobExecution

jobs_router = APIRouter(
    prefix="/jobs",
    tags=["jobs"]
)

job_executions_router = APIRouter(
    prefix='/jobexecutions',
    tags=['job_executions']
)


@jobs_router.post("/", status_code=status.HTTP_202_ACCEPTED, response_model=JobResponse)
def create_job(request: Request, job_request: JobRequest) -> JobResponse:
    """
        Submits a job, which will index specified documents.

        job_request is going to contain the sourceType enum
        and the modifiedTime
    """
    job, job_step = request.app.feature_service.convert_to_job_and_first_step(job_request)
    if job and job_step:
        request.app.job_queue.queue_job_step(job, job_step, execute_now=True)
        return JobResponse(id=job.id)
    else:
        raise ValueError(f"unsupported job request")


@job_executions_router.post('/', status_code=status.HTTP_202_ACCEPTED)
def execute_job(request: Request, job_execution: JobExecution):
    """
    Triggers the execution of the specified job. The job execution will resume if it already started.
    """
    request.app.job_queue.start_or_resume_job(job_execution.jobId)
    return {}
