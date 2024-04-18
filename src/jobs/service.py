from __future__ import annotations  # enables circular dependencies in type aliases and type hints

from abc import ABC, abstractmethod
from enum import Enum

from loguru import logger
import platform
from requests.exceptions import RequestException
import traceback
from typing import Callable, TypeAlias, Tuple

from config import Settings
from connections.models import Connection
from connections.service import ConnectionLoader, ConnectionRepository
from helixplatform import ar_core_fields, data_connection_job_step, data_connection_job
from helixplatform.service import InnovationSuite
from utils.text_utils import is_blank
from workers.service import WorkerGroup
from .constants import JobStepStatus, JobType
from .models import Job, JobStep, JobStepWork, Work, PollMoreWork
from .schemas import JobRequest


class DeleteDocBy(Enum):
    BY_DOC_ID = 1
    BY_DOC_DISPLAY_ID = 2

    def pick_key_for_delete(self, doc_id: str | None, doc_display_id: str | None) -> tuple[str, str]:
        """
        :return: [key_field, key_value]
        """
        match self:
            case DeleteDocBy.BY_DOC_ID:
                key_field = 'metadata.doc_id'
                key_value = doc_id
            case DeleteDocBy.BY_DOC_DISPLAY_ID:
                key_field = 'metadata.doc_display_id'
                key_value = doc_display_id
            case _:
                raise ValueError(f"unsupported DeleteDocBy value: {self.name}")
        return key_field, key_value


class Feature(ABC):

    @abstractmethod
    def accept_job_request(self, job_request: JobRequest) -> bool:
        pass

    def create_job(self, job_request: JobRequest) -> Job:
        """ Turns the given JobRequest into the corresponding unpersisted Job. """
        return Job(datasource=job_request.datasource,
                   doc_id=job_request.docId,
                   doc_display_id=job_request.docDisplayId,
                   modified_since=job_request.modifiedSince,
                   connection_id=job_request.connectionId)

    @abstractmethod
    def accept_job(self, job: Job) -> bool:
        pass

    @abstractmethod
    def create_first_job_step(self, job: Job) -> JobStep:
        """ Turns the given JobRequest into the corresponding unpersisted Job. """
        pass

    @abstractmethod
    def get_handler(self, job: Job, job_step: JobStep):  # -> JobHandler (defined below)
        """ Returns a JobHandler i.e., a method able to handle the specified job step. """
        pass

    @abstractmethod
    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        """
        Indicates the property, which points at the OpenSearch documents to delete before indexing a new revision
        of that document.
        """
        pass

    def get_connection_loader(self,
                              connection_id: str,
                              connection_repository: ConnectionRepository) -> ConnectionLoader:
        """
        Returns a compatible ConnectionLoader for this feature.  A base connection loader will be returned by default,
        should a more specific instance be required, then override this method and return a custom loader
        :param connection_id: id of the connection object in the repository
        :param connection_repository:  class that can be used to load the connection configuration
        :return: a compatible ConnectionLoader
        """
        return ConnectionLoader(connection_id=connection_id, connection_repository=connection_repository)


class FeatureService:

    def __init__(self, features: [Feature]):
        self.__features: [Feature] = features

    def create_job(self, job_request: JobRequest) -> Job | None:
        for feature in self.__features:
            if feature.accept_job_request(job_request):
                return feature.create_job(job_request)
        return None

    def _find_accepting_feature(self, job: Job) -> Feature | None:
        for feature in self.__features:
            if feature.accept_job(job):
                return feature
        return None

    def create_first_job_step(self, job: Job) -> JobStep | None:
        feature = self._find_accepting_feature(job)
        return feature and feature.create_first_job_step(job) or None

    def convert_to_job_and_first_step(self, job_request: JobRequest) -> Tuple[Job | None, JobStep | None]:
        """
        Tries to create an unpersisted job and its first step out of the specified `JobRequest`.
        Returns (None, None) if no feature supports the passed `JobRequest`
        """
        job = self.create_job(job_request)
        if job:
            job_step = self.create_first_job_step(job)
            if not job_step:
                raise ValueError(f"unable to create a first step out of job {job}")  # should not normally happen
            return job, job_step
        return None, None

    def get_handler(self, job: Job, job_step: JobStep) -> 'JobHandler | None':
        feature = self._find_accepting_feature(job)
        return feature and feature.get_handler(job, job_step) or None

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy | None:
        """
        Returns the strategy to use in order to delete OpenSearch documents for a given Job/JobStep or
        returns None if there is no known such strategy (most likely, there is no feature for the given job).
        """
        feature = self._find_accepting_feature(job)
        return feature and feature.get_delete_doc_by(job, job_step) or None

    def get_connection_loader(self, job: Job, connection_repository: ConnectionRepository) -> ConnectionLoader | None:
        """
        Returns a connection configuration loader for the given Job.
        :param job: the job to load the connection configuration for
        :param connection_repository: repository class that can be used to load the connection configuration
        :return: a loader or None if no matching loader can be found for the job
        """
        if is_blank(job.connection_id):
            return None
        feature = self._find_accepting_feature(job)
        return feature and feature.get_connection_loader(job.connection_id, connection_repository) or None


class JobStepClaimError(Exception):
    """ Raised when changing the status of a JobStep to IN_PROGRESS failed. """

    def __init__(self, job_step: JobStep):
        self.job_step = job_step


class JobStepClaimConflictError(JobStepClaimError):
    """ Raised when trying to claim a JobStep, which is not pending. """


class JobRepository:
    """
    Represents a storage of jobs and their job steps to perform for crawlers and loaders.
    """

    def __init__(self, is_client: InnovationSuite = None):
        self.__is_client = is_client or InnovationSuite()

    def store_job(self, job: Job):
        job_id = self.__is_client.create_record(record=job.to_record())
        job.id = job_id

    def store_job_step(self, job_step: JobStep, job: Job | None = None):
        """
        Stores the specified JobStep.
        If a Job is specified, it will be considered as the parent of the JobStep.
        If the Job is not persisted already, it will also be stored prior to storing the JobStep.
        This method modifies the `id` field of the passed JobStep. The same goes for the passed Job
         if its `id` is not set already.
        """
        if job:
            if not job.id:
                self.store_job(job)
            job_step.job_id = job.id
        elif not job_step.job_id:  # no specified job and no specified job reference
            raise ValueError('cannot store a JobStep without a parent Job reference')

        job_step_id = self.__is_client.create_record(record=job_step.to_record())
        job_step.id = job_step_id

    def __store_job_step_status(
            self,
            job_step_id: str,
            status: JobStepStatus,
            executing_node: str = None,
            error_details: str = None):
        """
          executing_node: if None then this information will not be updated (it won't be set to null).
        """
        record = JobStep.to_set_status_record(job_step_id, status, executing_node, error_details)
        self.__is_client.update_record(record)

    def get_job(self, job_id: str) -> Job:
        record = self.__is_client.get_record(data_connection_job.FORM, job_id)
        return Job.from_record(record)

    def get_job_step(self, job_step_id: str) -> JobStep:
        record = self.__is_client.get_record(data_connection_job_step.FORM, job_step_id)
        return JobStep.from_record(record)

    def has_job_steps(self, job_id: str) -> bool:
        query_expression = "'{field_id}' = \"{job_id}\"".format(
            field_id=data_connection_job_step.FIELD_JOB_ID, job_id=job_id)

        data_page = self.__is_client.get_records(
            data_connection_job_step.FORM,
            property_selection=[ar_core_fields.FIELD_ID],
            query_expression=query_expression,
            page_size=1,
            include_total_size=False
        )
        return bool(data_page.data)

    def get_pending_job_steps(self, job_id: str, limit: int = None, after_display_id: str = None) -> [JobStep]:
        """
        Returns the next `count` PENDING job steps of the given job, starting from the oldest one and in creation order.

        :param job_id: job of the retrieved JobSteps
        :param limit: max number of retrieved JobSteps
        :param after_display_id: if specified, indicates that retrieved JobSteps must be newer than this one.
        """
        query_expression = "'{field_job_id}' = \"{job_id}\" AND '{field_status}' = {pending}".format(
            field_job_id=data_connection_job_step.FIELD_JOB_ID,
            job_id=job_id,
            field_status=ar_core_fields.FIELD_STATUS,
            pending=int(JobStepStatus.PENDING)
        )
        if after_display_id:
            query_expression += " AND '{field_display_id}' > \"{after_display_id}\"".format(
                field_display_id=ar_core_fields.FIELD_DISPLAY_ID, after_display_id=after_display_id)

        data_page = self.__is_client.get_records(
            data_connection_job_step.FORM,
            query_expression=query_expression,
            sort_by=[ar_core_fields.FIELD_DISPLAY_ID],  # ordering by display ID assures oldest is considered first
            page_size=limit
        )
        return [JobStep.from_data_page_dict(record) for record in data_page.data]

    def claim_job_step(self, job_step: JobStep):
        """
        Ensures that the specified JobStep still is available for execution and then set its status to IN_PROGRESS.
        """
        # TODO try to leverage the optimistic locking.
        #  [Philippe] So far, I wasn't able to observe it working on my server, using the REST API.
        #             Optimistic locking would give better assurance no two nodes are running the same step.

        # make sure the step still is available for execution
        reloaded_job_step = self.__is_client.get_record(data_connection_job_step.FORM, job_step.id)
        job_step.status = JobStepStatus(int(reloaded_job_step[ar_core_fields.FIELD_STATUS]))
        job_step.executing_node = reloaded_job_step[data_connection_job_step.FIELD_EXECUTING_NODE]

        if job_step.status != JobStepStatus.PENDING:
            raise JobStepClaimConflictError(job_step)

        # mark the step as IN_PROGRESS
        node = platform.node()
        try:
            self.__store_job_step_status(job_step.id, JobStepStatus.IN_PROGRESS, node)
        except RequestException as e:
            raise JobStepClaimError(job_step) from e
        job_step.status = JobStepStatus.IN_PROGRESS
        job_step.executing_node = node

    def mark_job_step_as_done(self, job_step_id: str):
        self.__store_job_step_status(job_step_id, JobStepStatus.DONE)

    def mark_job_step_as_error(self, job_step_id: str, error_details: str):
        self.__store_job_step_status(job_step_id, JobStepStatus.ERROR, error_details=error_details)


class JobQueuing(ABC):
    @abstractmethod
    def queue_job_step(self, job: Job, job_step: JobStep, connection: Connection, execute_now: bool) -> str:
        """ Queues a new JobStep to execute. Returns the ID of the queued ``JobStep``. """

    @abstractmethod
    def execute_job_steps(self, job: Job):
        """ Launches the execution of the pending steps of the specified job. """


class JobChain:
    """
    Passed to job handlers as a facade to perform further decoupled actions.
    Subclasses can define more action methods.

    This class is meant to decouple the job-specific logic from the larger requirements regarding documents, indexation,
    etc.
    """

    def __init__(self, job_queue: JobQueuing):
        self.__job_queue = job_queue

    def queue_job_step(self, job: Job, job_step: JobStep, connection: Connection, execute_now: bool = False) -> str:
        """ Queues the specified ``JobStep`` for execution and returns its DB ID. """
        return self.__job_queue.queue_job_step(job, job_step, connection, execute_now)

    def queue_sync_deletions_if_configured(self, job: Job, connection: Connection):
        """ If configured in the specified ``Job``, creates and queues a ``SYNC_DELETIONS`` job step.
            Returns the queued job step ID or ``None`` if none was created. """
        if job.defaulted_sync_deletions:
            # sync deletions job steps
            return self.queue_job_step(
                job,
                JobStep(
                    JobType.SYNC_DELETIONS,
                    job.datasource,
                    job_id=job.id,
                    doc_id=job.doc_id,
                    doc_display_id=job.doc_display_id),
                connection)
        else:
            return None

    def execute_job_steps(self, job: Job):
        return self.__job_queue.execute_job_steps(job)


class JobQueue(JobQueuing):

    def __init__(
            self,
            feature_service: FeatureService,
            job_repository: JobRepository | None = None,
            job_chain_factory: JobChainFactory | None = None,
            worker_group: WorkerGroup | None = None,
            connection_repository: ConnectionRepository | None = None):
        self.__feature_service = feature_service
        self.__job_repository = job_repository or JobRepository()
        self.__job_chain_factory = job_chain_factory or JobChain(self)
        self.__worker_group = worker_group or WorkerGroup(self.handle_work)
        self.__connection_repository = connection_repository or ConnectionRepository()

    def queue_job_step(self, job: Job, job_step: JobStep, connection: Connection, execute_now: bool) -> str:
        """
        Queues the specified job and step for execution, optionally triggering an immediate execution.

        :param job: job of the job step to queue
        :param job_step: job step to queue
        :param connection: job connection configuration
        :param execute_now: if True, then the job step is immediately submitted to the worker threads.
                            Otherwise, it won't be executed until later, when a poll-more work will be executed,
                            for instance.

        :return: the ID of the queued job step.
        """
        self.__job_repository.store_job_step(job_step, job)
        if execute_now:
            self.notify_job_step_work(job, job_step, connection)
        return job.id

    def notify_job_step_work(self, job: Job, job_step: JobStep, connection: Connection):
        """ Notify the worker threads that the specified job step is to be handled. """
        self.__worker_group.submit_work(JobStepWork(job, job_step, connection))  # wake up a worker immediately

    def notify_poll_more_work(self, job_id: str, datasource: str, after_display_id: str = None):
        self.__worker_group.submit_work(PollMoreWork(job_id, datasource, after_display_id))

    def __claim_job_step(self, job_step: JobStep):
        try:
            self.__job_repository.claim_job_step(job_step)
        except JobStepClaimConflictError:
            logger.warning("job step cannot be claimed anymore, skipping: {job_step}", job_step=job_step)
        except JobStepClaimError:
            logger.error("error while claiming job step, skipping: {job_step}", job_step=job_step)
        except Exception:
            logger.exception("error while claiming {job_step}:", job_step=job_step)

    @logger.catch
    def handle_work(self, work: Work):
        work.execute(self)

    def handle_job_step(self, work: JobStepWork):
        job = work.job
        job_step = work.job_step
        connection = work.connection

        # Get the handler
        handler = self.__feature_service.get_handler(job, job_step)
        if not handler:
            logger.warning("unsupported job type, skipping: {job_step}", job_step=job_step)
            return

        # Claim the job step
        self.__claim_job_step(job_step)
        if job_step.status != JobStepStatus.IN_PROGRESS:
            return

        # Execute the job step
        try:
            handler(job, job_step, self.__job_chain_factory(self), connection)
            self.__job_repository.mark_job_step_as_done(job_step.id)
        except Exception:
            logger.exception("error while handling {job_step}:", job_step=job_step)
            error_details = traceback.format_exc()
            self.__job_repository.mark_job_step_as_error(job_step.id, error_details)

    def execute_job_steps(self, job: Job):
        self.poll_more(job.id, job.datasource)

    def handle_poll_more(self, work: PollMoreWork):
        self.poll_more(work.job_id, work.datasource, work.after_display_id)

    def poll_more(self, job_id: str, datasource: str, after_display_id: str = None):
        pending_steps = self.__job_repository.get_pending_job_steps(
            job_id, limit=Settings.JOB_STEP_BATCH_SIZE, after_display_id=after_display_id)
        if not pending_steps:
            logger.info('no more steps to poll for job {job} ({datasource})', job=job_id, datasource=datasource)
            return

        logger.debug('attempting to resume {count} pending steps for job {job} ({datasource})',
                     count=len(pending_steps), job=job_id, datasource=datasource)

        job = self.__job_repository.get_job(job_id)
        loader = self.__feature_service.get_connection_loader(job, self.__connection_repository)
        connection = loader and loader.load() or None
        max_display_id = pending_steps[0].display_id
        for job_step in pending_steps:
            self.notify_job_step_work(job, job_step, connection)
            if job_step.display_id > max_display_id:
                max_display_id = job_step.display_id

        # We assume that this method won't be called _while_ the job steps are generated. Therefore, we can optimize
        # the case where we get partial batch and skip the next polling.
        if len(pending_steps) >= Settings.JOB_STEP_BATCH_SIZE:
            self.notify_poll_more_work(job_id, datasource, max_display_id)

    def start_or_resume_job(self, job_id: str):
        job = self.__job_repository.get_job(job_id)
        if self.__job_repository.has_job_steps(job_id):
            # Resume job
            self.poll_more(job_id, job.datasource)

        else:
            # Start job
            job_step = self.__feature_service.create_first_job_step(job)
            if job_step:
                loader = self.__feature_service.get_connection_loader(job, self.__connection_repository)
                connection = loader and loader.load() or None
                self.queue_job_step(job, job_step, connection, execute_now=True)
            else:
                logger.warning("ignoring job {job} ({datasource}): unable to derive steps from it",
                               job=job.id, datasource=job.datasource)


JobChainFactory: TypeAlias = Callable[[JobQueue], JobChain]

# JobHandler can handle the specified `Job` and use the passed to `JobChain` to spawn other steps, which will further
# handle the task.
JobHandler: TypeAlias = Callable[[Job, JobStep, JobChain, Connection], None]

JobHandlerFactory: TypeAlias = Callable[[JobStep], JobHandler]
