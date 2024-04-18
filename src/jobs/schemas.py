from datetime import datetime
from typing import Optional

from pydantic import BaseModel, PrivateAttr
from starlette.datastructures import UploadFile


class JobRequest(BaseModel):
    """
    """
    datasource: str
    docId: str | None
    docDisplayId: str | None
    uri: Optional[str]
    loadDirectory: bool | None
    modifiedSince: datetime | None
    _upload_file: UploadFile = PrivateAttr(None)
    connectionId: str | None

    @property
    def upload_file(self):
        return self._upload_file


class JobResponse(BaseModel):
    id: str


class JobExecution(BaseModel):
    jobId: str
