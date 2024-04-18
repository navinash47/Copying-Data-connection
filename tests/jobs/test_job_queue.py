import pytest
from pytest_mock import MockerFixture

from config import Settings
from connections.models import Connection
from jobs.constants import JobStepStatus, JobType
from jobs.models import Job, JobStep, JobStepWork, PollMoreWork
from jobs.service import FeatureService, JobChain, JobQueue, JobRepository
from workers.service import WorkerGroup


def mock_store_job_step(job_step: JobStep, job: Job):
    job.id = 'JOB_ID'
    job_step.id = 'JOB_STEP_ID'
    job_step.job_id = job.id


@pytest.mark.parametrize('execute_now', [True, False])
def test_queue_job_step(mocker: MockerFixture, execute_now: bool):
    feature_service = mocker.Mock(FeatureService)
    job_repository = mocker.Mock(JobRepository)
    job_repository.store_job_step.side_effect = mock_store_job_step
    job_chain = mocker.Mock(JobChain)
    connection = mocker.Mock(Connection)
    trigger_work = mocker.patch('jobs.service.WorkerGroup.submit_work', return_value=None)

    job = Job('RKM')
    job_step = JobStep(JobType.CRAWL, 'RKM')

    job_queue = JobQueue(feature_service, job_repository, lambda: job_chain)
    job_queue.queue_job_step(job, job_step, connection, execute_now=execute_now)

    job_repository.store_job_step.assert_called_once_with(job_step, job)
    assert job.id == 'JOB_ID'
    assert job_step.id == 'JOB_STEP_ID'
    assert job_step.job_id == job.id
    if execute_now:
        trigger_work.assert_called_once_with(JobStepWork(job, job_step, connection))
    else:
        trigger_work.assert_not_called()


def mock_claim_job_status(job_step: JobStep):
    job_step.status = JobStepStatus.IN_PROGRESS


def mock_mark_job_step_as_done(job_step: JobStep):
    job_step.status = JobStepStatus.DONE


def mock_job_step_handler(job: Job, job_step: JobStep, job_chain: JobChain):
    pass


def test_handle_job_step(mocker: MockerFixture):
    feature_service = mocker.Mock(FeatureService)
    job_step_handler = mocker.patch('connections.rkm.crawler.crawl_rkm')
    feature_service.get_handler.return_value = job_step_handler
    job_repository = mocker.Mock(JobRepository)
    job_repository.claim_job_step.side_effect = mock_claim_job_status
    job_repository.mark_job_step_as_done.return_value = None
    job_chain = mocker.Mock(JobChain)
    connection = mocker.Mock(Connection)

    job = Job('RKM', id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, 'RKM', id='JOB_STEP_ID', job_id=job.id)

    job_queue = JobQueue(feature_service, job_repository, lambda a_job_queue: job_chain)
    job_queue.handle_work(JobStepWork(job, job_step, connection))

    feature_service.get_handler.assert_called_once_with(job, job_step)
    job_repository.claim_job_step.assert_called_once_with(job_step)
    job_step_handler.assert_called_once_with(job, job_step, job_chain, connection)
    job_repository.mark_job_step_as_done.assert_called_once_with(job_step.id)


def test_handle_job_step_with_error(mocker: MockerFixture):
    feature_service = mocker.Mock(FeatureService)
    job_step_handler = mocker.patch('connections.rkm.crawler.crawl_rkm', side_effect=ValueError('error_during_handling'))
    feature_service.get_handler.return_value = job_step_handler
    job_repository = mocker.Mock(JobRepository)
    job_repository.claim_job_step.side_effect = mock_claim_job_status
    job_repository.mark_job_step_as_done.return_value = None
    job_chain = mocker.Mock(JobChain)
    connection = mocker.Mock(Connection)

    job = Job('RKM', id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, 'RKM', id='JOB_STEP_ID', job_id=job.id)

    job_queue = JobQueue(feature_service, job_repository, lambda a_job_queue: job_chain)
    job_queue.handle_work(JobStepWork(job, job_step, connection))

    feature_service.get_handler.assert_called_once_with(job, job_step)
    job_repository.claim_job_step.assert_called_once_with(job_step)
    job_step_handler.assert_called_once_with(job, job_step, job_chain, connection)
    job_repository.mark_job_step_as_error.assert_called_once()
    assert job_repository.mark_job_step_as_error.mock_calls[0].args[0] == job_step.id
    assert 'error_during_handling' in job_repository.mark_job_step_as_error.mock_calls[0].args[1]


def test_handle_poll_more_with_no_pending_job_steps(mocker: MockerFixture):
    feature_service = mocker.Mock(FeatureService)
    job_chain = mocker.Mock(JobChain)
    worker_group = mocker.Mock(WorkerGroup)

    job_repository = mocker.Mock(JobRepository)
    job_repository.get_pending_job_steps.return_value = []

    job_queue = JobQueue(feature_service, job_repository, lambda a_job_queue: job_chain, worker_group)
    job_queue.handle_poll_more(PollMoreWork('JOB_ID', 'RKM', 'AFTER_DISPLAY_ID'))

    job_repository.get_pending_job_steps.assert_called_once_with(
        'JOB_ID', limit=Settings.JOB_STEP_BATCH_SIZE, after_display_id='AFTER_DISPLAY_ID')
    job_repository.get_job.assert_not_called()
    worker_group.assert_not_called()


def test_handle_poll_more_with_one_pending_job_step(mocker: MockerFixture):
    Settings.JOB_STEP_BATCH_SIZE = 10

    feature_service = mocker.Mock(FeatureService)
    job_chain = mocker.Mock(JobChain)
    worker_group = mocker.Mock(WorkerGroup)

    job_repository = mocker.Mock(JobRepository)
    job = Job(id='JOB_ID', datasource='RKM')
    job_repository.get_job.return_value = job
    job_step = JobStep(
        id='JOB_STEP_ID', display_id='JOB_STEP_DISPLAY_ID', job_id=job.id, datasource=job.datasource, type=JobType.LOAD)
    job_repository.get_pending_job_steps.return_value = [job_step]

    job_queue = JobQueue(feature_service, job_repository, lambda a_job_queue: job_chain, worker_group)
    job_queue.handle_poll_more(PollMoreWork(job.id, job.datasource, 'AFTER_DISPLAY_ID'))

    job_repository.get_pending_job_steps.assert_called_once_with(
        job.id, limit=Settings.JOB_STEP_BATCH_SIZE, after_display_id='AFTER_DISPLAY_ID')
    job_repository.get_job.assert_called_once_with(job.id)
    worker_group.submit_work.assert_called_once()
    submitted_work = worker_group.submit_work.mock_calls[0].args[0]
    assert isinstance(submitted_work, JobStepWork)
    assert submitted_work.job == job
    assert submitted_work.job_step == job_step


def test_handle_poll_more_with_a_full_batch(mocker: MockerFixture):
    Settings.JOB_STEP_BATCH_SIZE = 2

    feature_service = mocker.Mock(FeatureService)
    job_chain = mocker.Mock(JobChain)
    worker_group = mocker.Mock(WorkerGroup)

    job_repository = mocker.Mock(JobRepository)
    job = Job(id='JOB_ID', datasource='RKM')
    job_repository.get_job.return_value = job
    job_step_1 = JobStep(
        id='JOB_STEP_ID_1',
        display_id='JOB_STEP_DISPLAY_ID_1',
        job_id=job.id,
        datasource=job.datasource,
        type=JobType.LOAD)
    job_step_2 = JobStep(
        id='JOB_STEP_ID_2',
        display_id='JOB_STEP_DISPLAY_ID_2',
        job_id=job.id,
        datasource=job.datasource,
        type=JobType.LOAD)
    job_repository.get_pending_job_steps.return_value = [job_step_1, job_step_2]

    job_queue = JobQueue(feature_service, job_repository, lambda a_job_queue: job_chain, worker_group)
    job_queue.handle_poll_more(PollMoreWork(job.id, job.datasource, 'AFTER_DISPLAY_ID'))

    job_repository.get_pending_job_steps.assert_called_once_with(
        job.id, limit=Settings.JOB_STEP_BATCH_SIZE, after_display_id='AFTER_DISPLAY_ID')
    job_repository.get_job.assert_called_once_with(job.id)
    worker_group.submit_work.assert_called()
    assert len(worker_group.submit_work.mock_calls) == 3

    submitted_works = [call.args[0] for call in worker_group.submit_work.mock_calls]

    assert isinstance(submitted_works[0], JobStepWork)
    assert submitted_works[0].job == job
    assert submitted_works[0].job_step == job_step_1

    assert isinstance(submitted_works[1], JobStepWork)
    assert submitted_works[1].job == job
    assert submitted_works[1].job_step == job_step_2

    assert isinstance(submitted_works[2], PollMoreWork)
    assert submitted_works[2].job_id == job.id
    assert submitted_works[2].datasource == job.datasource
    assert submitted_works[2].after_display_id == job_step_2.display_id
