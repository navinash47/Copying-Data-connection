from datetime import datetime, timezone

import pytest
from pytest_mock import MockerFixture

from config import Settings
from connections.models import Connection
from connections.service import ConnectionLoader, ConnectionRepository
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import Feature, FeatureService, DeleteDocBy, JobChain, JobQueuing
from workers.service import WorkerGroup


def test_worker_group_max_workers_setting():
    def do_work_stub():
        pass  # unused implementation

    Settings.MAX_JOB_WORKERS = 1
    worker_group = WorkerGroup(do_work_stub)
    try:
        assert worker_group.executor._max_workers == 1
    finally:
        worker_group.executor.shutdown()


class FakeFeature(Feature):
    def __init__(self, datasource: str, delete_doc_by: DeleteDocBy):
        self.__datasource = datasource
        self.__delete_doc_by = delete_doc_by

    def accept_job_request(self, job_request: JobRequest) -> bool:
        raise NotImplementedError()

    def accept_job(self, job: Job) -> bool:
        return job.datasource == self.__datasource

    def create_first_job_step(self, job: Job) -> JobStep:
        raise NotImplementedError()

    def get_handler(self, job: Job, job_step: JobStep):
        raise NotImplementedError()

    def get_delete_doc_by(self, job: Job, job_step: JobStep) -> DeleteDocBy:
        return self.__delete_doc_by


@pytest.mark.parametrize(
    'datasource, expected_delete_doc_by',
    [
        ('DATASOURCE 1', DeleteDocBy.BY_DOC_ID),
        ('DATASOURCE 2', DeleteDocBy.BY_DOC_DISPLAY_ID),
        ('DATASOURCE 3', None),
    ]
)
def test_feature_service_get_delete_doc_by(datasource: str, expected_delete_doc_by: DeleteDocBy):
    feature_service = FeatureService([
        FakeFeature('DATASOURCE 1', DeleteDocBy.BY_DOC_ID),
        FakeFeature('DATASOURCE 2', DeleteDocBy.BY_DOC_DISPLAY_ID)
    ])
    job = Job(datasource)
    job_step = JobStep(JobType.LOAD, job.datasource)
    assert feature_service.get_delete_doc_by(job, job_step) == expected_delete_doc_by


def test_feature_connection_loader_creation(mocker: MockerFixture):
    connection_id = 'ABC1234'
    connection_repository = mocker.Mock(ConnectionRepository)

    feature = FakeFeature('DS1', DeleteDocBy.BY_DOC_ID)
    loader = feature.get_connection_loader(connection_id, connection_repository)

    assert loader._connection_id == connection_id
    assert loader._connection_repository == connection_repository


def test_feature_service_connection_loader_creation(mocker: MockerFixture):
    connection_id = 'ABC1234'
    connection_repository = mocker.Mock(ConnectionRepository)
    job = Job(datasource='DS1', id='5678', connection_id='connection-1')
    feature = mocker.Mock(Feature)
    feature.accept_job.return_value = True
    mock_loader = ConnectionLoader(connection_id, connection_repository)
    feature.get_connection_loader.return_value = mock_loader

    service = FeatureService([feature])

    loader = service.get_connection_loader(job, connection_repository)

    assert loader == mock_loader
    feature.accept_job.assert_called_once_with(job)
    feature.get_connection_loader.assert_called_once_with('connection-1', connection_repository)


@pytest.mark.parametrize('connection_id', [None, '', '  '])
def test_feature_service_connection_loader_with_unsupported_connection_id(connection_id: str, mocker: MockerFixture):
    connection_repository = mocker.Mock(ConnectionRepository)
    job = Job(datasource='DS1', id='5678', connection_id=connection_id)
    feature = mocker.Mock(Feature)

    service = FeatureService([feature])

    loader = service.get_connection_loader(job, connection_repository)

    assert loader is None
    feature.accept_job.assert_not_called()
    feature.get_connection_loader.assert_not_called()


def test_feature_service_connection_loader_no_matching_features(mocker: MockerFixture):
    connection_repository = mocker.Mock(ConnectionRepository)
    job = Job(datasource='DS1', id='5678')

    service = FeatureService([])

    loader = service.get_connection_loader(job, connection_repository)

    assert loader is None


def test_create_job():
    datasource = 'ds-1'
    doc_id = 'the-dod-id'
    doc_display_id = 'the-display-id'
    modified_since = datetime(2023, 12, 21, 14, 37, 12, tzinfo=timezone.utc)
    connection_id = 'the-connection-id'

    job_request = JobRequest(
        datasource=datasource,
        docId=doc_id,
        docDisplayId=doc_display_id,
        modifiedSince=modified_since,
        connectionId=connection_id
    )

    feature = FakeFeature(datasource, DeleteDocBy.BY_DOC_ID)

    job = feature.create_job(job_request)

    assert job.id is None
    assert job.datasource == datasource
    assert job.doc_id == doc_id
    assert job.doc_display_id == doc_display_id
    assert job.modified_since == modified_since
    assert job.connection_id == connection_id


@pytest.mark.parametrize('configured', [True, False])
def test_job_chain_queue_sync_deletions_if_configured(mocker: MockerFixture, configured: bool):
    job_queue = mocker.Mock(JobQueuing)
    queue_job_step_mock = job_queue.queue_job_step
    queue_job_step_mock.return_value = 'JOB_STEP_ID'

    job_chain = JobChain(job_queue)

    job = Job('DATASOURCE', id='JOB_ID', doc_id='DOC_ID', doc_display_id='DOC_DISPLAY_ID', sync_deletions=configured)
    connection = Connection('CONNECTION_ID')

    job_step_id = job_chain.queue_sync_deletions_if_configured(job, connection)

    if configured:
        assert job_step_id == 'JOB_STEP_ID'

        queue_job_step_mock.assert_called_once()
        assert queue_job_step_mock.mock_calls[0].args[0] == job
        job_step = queue_job_step_mock.mock_calls[0].args[1]
        assert job_step.type == JobType.SYNC_DELETIONS
        assert job_step.job_id == 'JOB_ID'
        assert job_step.datasource == 'DATASOURCE'
        assert job_step.doc_id == 'DOC_ID'
        assert job_step.doc_display_id == 'DOC_DISPLAY_ID'
    else:
        assert job_step_id is None
        queue_job_step_mock.assert_not_called()
