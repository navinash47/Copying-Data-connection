from typing import Annotated

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from starlette.requests import Request

from connections.files.constants import MAX_FILE_SIZE
from jobs.schemas import JobRequest, JobResponse
from utils.file_types_utils import ContentType

files_router = APIRouter(
    prefix="/files",
    tags=["Files"]
)


@files_router.post("/", status_code=status.HTTP_202_ACCEPTED, response_model=JobResponse)
def uploading_file(request: Request,
                   datasource: Annotated[str, Form()],
                   upload_file: Annotated[UploadFile, File(description="A file read as UploadFile")]) -> JobResponse:
    """
        Uploads a file

        file: Key for uploading the file
    """
    if upload_file.content_type not in [ContentType.WORD, ContentType.PDF]:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"File type of {upload_file.content_type} is not supported",
        )
    if upload_file.size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Weight not supported",
        )

    job_request = JobRequest(datasource=datasource)
    job_request._upload_file = upload_file
    job, job_step = request.app.feature_service.convert_to_job_and_first_step(job_request)
    if job and job_step:
        request.app.job_queue.queue_job_step(job, job_step, execute_now=True)
        return JobResponse(id=job.id)
    else:
        raise ValueError(f"unsupported job request")
