""""
Job represents a unit of work in a data connection, while JobStep represents an atomic step of that work. 
Job contains information about the overall job, while JobStep contains information about individual
steps within that job

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

from fastapi import UploadFile

from connections.models import Connection
from helixplatform import ar_core_fields, data_connection_job, data_connection_job_step
from helixplatform.models import Record
from .constants import JobStepStatus, JobType


@dataclass
class Job:
    """
    Represents an independent work unit of Data Connection, while the job steps are the atomic units executed by this
    app e.g., crawling or loading and indexation.

    datasource: datasource providing the documents to index (HKM, etc.).
                The actual technical source of the documents may be different, like a URI or files in a directory.
    """
    datasource: str
    doc_id: str | None = None  # ID of the loaded document if specified and relevant (not so when loading directory)
    doc_display_id: str | None = None  # Display ID of the document to load
    id: str | None = None
    load_directory: bool | None = None
    uri: str | None = None
    file: str | None = None  # file name or path; determines the file type and, when applicable, where to load from
    upload_file: UploadFile | None = None
    __upload_filename: str | None = None  # valued when loaded from the IS record API
    modified_since: datetime | None = None
    connection_id: str | None = None
    sync_deletions: bool | None = None  # `None` means `True` as well.

    @property
    def upload_filename(self):  # read-only property
        if self.upload_file is None:
            return self.__upload_filename
        else:
            return self.upload_file.filename

    @property
    def defaulted_sync_deletions(self) -> bool:  # read-only property
        """ Whether this job should sync deletions. Defaults to `True` if it wasn't specified (None). """
        return self.sync_deletions if self.sync_deletions is not None else True

    def to_record(self) -> Record:
        """ Returns a new Record object containing the persistable data of this Job. """
        record = Record(recordDefinitionName=data_connection_job.FORM)
        if self.id:
            record[ar_core_fields.FIELD_ID] = self.id
        record[data_connection_job.FIELD_DATASOURCE] = self.datasource
        record[data_connection_job.FIELD_DOC_ID] = self.doc_id
        record[data_connection_job.FIELD_DOC_DISPLAY_ID] = self.doc_display_id
        record[data_connection_job.FIELD_FILE] = self.upload_file
        return record

    @staticmethod
    def from_record(record: Record) -> 'Job':
        job = Job(
            id=record[ar_core_fields.FIELD_ID],
            datasource=record[data_connection_job.FIELD_DATASOURCE],
            doc_id=record[data_connection_job.FIELD_DOC_ID],
            doc_display_id=record[data_connection_job.FIELD_DOC_DISPLAY_ID],
            modified_since=record[data_connection_job.FIELD_MODIFIED_SINCE] and
            datetime.fromisoformat(record[data_connection_job.FIELD_MODIFIED_SINCE]),
            connection_id=record[data_connection_job.FIELD_CONNECTION_ID],
            sync_deletions=record.get_as_bool(data_connection_job.FIELD_SYNC_DELETIONS)
        )
        # Note how we only get the filename back from IS in the record API.
        # You have to use the separate record attachment API to get the file contents.
        job.__upload_filename = record[data_connection_job.FIELD_FILE]
        return job


@dataclass
class JobStep:
    type: JobType
    datasource: str
    status: JobStepStatus = JobStepStatus.PENDING
    job_id: str | None = None  # ID of the parent Job
    doc_id: str | None = None  # ID of the loaded document if specified and relevant (not so when loading directory)
    doc_display_id: str | None = None  # Display ID of the document to load
    id: str | None = None  # ID of the job step itself
    display_id: str | None = None  # Display ID of the job step itself
    executing_node: str | None = None
    error_details: str | None = None

    def to_record(self) -> Record:
        """ Returns a new Record object containing the persistable data of this Job. """
        record = Record(recordDefinitionName=data_connection_job_step.FORM)
        if self.id:
            record[ar_core_fields.FIELD_ID] = self.id
        if self.display_id:
            record[ar_core_fields.FIELD_DISPLAY_ID] = self.display_id
        record[ar_core_fields.FIELD_STATUS] = int(self.status)
        record[data_connection_job_step.FIELD_DATASOURCE] = self.datasource
        record[data_connection_job_step.FIELD_DOC_ID] = self.doc_id
        record[data_connection_job_step.FIELD_DOC_DISPLAY_ID] = self.doc_display_id
        record[data_connection_job_step.FIELD_TYPE] = int(self.type)
        record[data_connection_job_step.FIELD_JOB_ID] = self.job_id
        record[data_connection_job_step.FIELD_EXECUTING_NODE] = self.executing_node
        record[data_connection_job_step.FIELD_ERROR_DETAILS] = self.error_details
        return record

    @staticmethod
    def from_record(record: Record) -> 'JobStep':
        """ Returns a new JobStep, whose fields are loaded from the passed Record. """
        record_status = record[ar_core_fields.FIELD_STATUS]
        record_type = record[data_connection_job_step.FIELD_TYPE]
        return JobStep(
            id=record[ar_core_fields.FIELD_ID],
            display_id=record[ar_core_fields.FIELD_DISPLAY_ID],
            status=JobStepStatus(int(record_status)) if record_status is not None else None,
            datasource=record[data_connection_job_step.FIELD_DATASOURCE],
            doc_id=record[data_connection_job_step.FIELD_DOC_ID],
            doc_display_id=record[data_connection_job_step.FIELD_DOC_DISPLAY_ID],
            type=JobType(int(record_type)) if record_type is not None else None,
            job_id=record[data_connection_job_step.FIELD_JOB_ID],
            executing_node=record[data_connection_job_step.FIELD_EXECUTING_NODE],
            error_details=record[data_connection_job_step.FIELD_ERROR_DETAILS],
        )

    @staticmethod
    def from_data_page_dict(record: Dict[str, Any]) -> 'JobStep':
        """ Returns a new JobStep, whose fields are loaded from the passed data page Dict. """
        record_status = record.get(str(ar_core_fields.FIELD_STATUS))
        record_type = record.get(str(data_connection_job_step.FIELD_TYPE))
        return JobStep(
            id=record.get(str(ar_core_fields.FIELD_ID)),
            display_id=record.get(str(ar_core_fields.FIELD_DISPLAY_ID)),
            status=JobStepStatus(int(record_status)) if record_status is not None else None,
            datasource=record.get(str(data_connection_job_step.FIELD_DATASOURCE)),
            doc_id=record.get(str(data_connection_job_step.FIELD_DOC_ID)),
            doc_display_id=record.get(str(data_connection_job_step.FIELD_DOC_DISPLAY_ID)),
            type=JobType(int(record_type)) if record_type is not None else None,
            job_id=record.get(str(data_connection_job_step.FIELD_JOB_ID)),
            executing_node=record.get(str(data_connection_job_step.FIELD_EXECUTING_NODE)),
            error_details=record.get(str(data_connection_job_step.FIELD_ERROR_DETAILS)),
        )

    @staticmethod
    def to_set_status_record(
            id: str, status: JobStepStatus, executing_node: str | None = None, error_details: str = None):
        record = Record(recordDefinitionName=data_connection_job_step.FORM)
        record.id = id
        record[ar_core_fields.FIELD_STATUS] = int(status)
        if executing_node:
            record[data_connection_job_step.FIELD_EXECUTING_NODE] = executing_node
        if error_details:
            record[data_connection_job_step.FIELD_ERROR_DETAILS] = error_details
        elif status != JobStepStatus.ERROR:
            # make sure we clean the error details if the status is not ERROR (and if details weren't explicitly passed)
            record[data_connection_job_step.FIELD_ERROR_DETAILS] = None
        return record


class Work(ABC):
    """ Represents a unit of work executable by the worker threads. """
    @abstractmethod
    def execute(self, job_queue):
        pass


@dataclass
class JobStepWork(Work):
    job: Job
    job_step: JobStep
    connection: Connection

    def execute(self, job_queue):
        job_queue.handle_job_step(self)


@dataclass
class PollMoreWork(Work):
    job_id: str
    datasource: str
    # polled job steps will have greater display IDsa than this job step if specified
    after_display_id: str | None

    def execute(self, job_queue):
        job_queue.handle_poll_more(self)


