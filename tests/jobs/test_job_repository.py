import platform
from typing import Dict

import pytest
from pytest_mock import MockerFixture
from requests.exceptions import HTTPError
import responses

from helixplatform import ar_core_fields, data_connection_job_step, data_connection_job
from helixplatform.models import Record, RecordDataPage
from helixplatform.service import InnovationSuite
from jobs.constants import JobStepStatus, JobType
from jobs.models import Job, JobStep
from jobs.service import JobRepository, JobStepClaimError, JobStepClaimConflictError


def test_store_job(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.create_record.return_value = 'RECORD_ID'

    job = Job(datasource='RKM')

    job_repository = JobRepository(innovation_suite)
    job_repository.store_job(job)

    assert job.id == 'RECORD_ID'
    innovation_suite.create_record.assert_called_once()
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000150'] == 'RKM'


def test_store_job_step(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.create_record.side_effect = ['JOB_ID', 'JOB_STEP_ID']

    job = Job(datasource='RKM')
    job_step = JobStep(JobType.CRAWL, datasource='RKM')

    job_repository = JobRepository(innovation_suite)
    job_repository.store_job_step(job_step, job)

    assert job.id == 'JOB_ID'
    assert job_step.id == 'JOB_STEP_ID'
    assert job_step.job_id == job.id
    assert len(innovation_suite.create_record.mock_calls) == 2
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000150'] == 'RKM'
    assert innovation_suite.create_record.mock_calls[1].kwargs['record']['490000150'] == 'RKM'
    assert innovation_suite.create_record.mock_calls[1].kwargs['record']['490000153'] == '0'  # CRAWL
    assert innovation_suite.create_record.mock_calls[1].kwargs['record']['490000154'] == 'JOB_ID'


def test_store_job_step_with_persisted_job(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.create_record.return_value = 'JOB_STEP_ID'

    job = Job(datasource='RKM', id='JOB_ID')
    job_step = JobStep(JobType.CRAWL, datasource='RKM')

    job_repository = JobRepository(innovation_suite)
    job_repository.store_job_step(job_step, job)

    assert job.id == 'JOB_ID'  # no change expected
    assert job_step.id == 'JOB_STEP_ID'
    assert job_step.job_id == job.id
    innovation_suite.create_record.assert_called_once()
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000150'] == 'RKM'
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000153'] == '0'  # CRAWL
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000154'] == 'JOB_ID'


def test_store_job_step_with_job_id(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.create_record.return_value = 'JOB_STEP_ID'

    job_step = JobStep(JobType.CRAWL, datasource='RKM', job_id='JOB_ID')

    job_repository = JobRepository(innovation_suite)
    job_repository.store_job_step(job_step)

    assert job_step.id == 'JOB_STEP_ID'
    assert job_step.job_id == 'JOB_ID'
    innovation_suite.create_record.assert_called_once()
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000150'] == 'RKM'
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000153'] == '0'  # CRAWL
    assert innovation_suite.create_record.mock_calls[0].kwargs['record']['490000154'] == 'JOB_ID'


def test_store_job_step_without_job_id(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.jwt_login.return_value = 'JWT_TOKEN'
    innovation_suite.create_record.return_value = 'JOB_STEP_ID'

    job_step = JobStep(JobType.CRAWL, datasource='RKM', job_id=None)

    job_repository = JobRepository(innovation_suite)
    try:
        job_repository.store_job_step(job_step)
        assert False
    except ValueError:
        pass  # expected


def test_claim_job_step(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)

    fresh_record = Record(recordDefinitionName=data_connection_job_step.FORM)
    fresh_record[ar_core_fields.FIELD_STATUS] = str(JobStepStatus.PENDING)
    fresh_record[data_connection_job_step.FIELD_ERROR_DETAILS] = None
    fresh_record[data_connection_job_step.FIELD_EXECUTING_NODE] = None
    innovation_suite.get_record.return_value = fresh_record
    innovation_suite.update_record.return_value = None

    job_step = JobStep(JobType.CRAWL, id='JOB_STEP_ID', datasource='RKM', job_id='JOB_ID')

    job_repository = JobRepository(innovation_suite)
    job_repository.claim_job_step(job_step)

    assert job_step.status == JobStepStatus.IN_PROGRESS
    assert job_step.executing_node == platform.node()
    innovation_suite.get_record.assert_called_once_with(data_connection_job_step.FORM, 'JOB_STEP_ID')
    innovation_suite.update_record.assert_called_once()
    update_record = innovation_suite.update_record.mock_calls[0].args[0]
    assert update_record[ar_core_fields.FIELD_STATUS] == str(int(job_step.status))
    assert update_record[data_connection_job_step.FIELD_EXECUTING_NODE] == job_step.executing_node
    assert update_record[data_connection_job_step.FIELD_ERROR_DETAILS] is None


def test_claim_job_step_with_already_claimed_step(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.jwt_login.return_value = 'JWT_TOKEN'

    fresh_record = Record(recordDefinitionName=data_connection_job_step.FORM)
    fresh_record[ar_core_fields.FIELD_STATUS] = str(JobStepStatus.DONE)
    fresh_record[data_connection_job_step.FIELD_ERROR_DETAILS] = None
    fresh_record[data_connection_job_step.FIELD_EXECUTING_NODE] = 'some other node'
    innovation_suite.get_record.return_value = fresh_record

    job_step = JobStep(JobType.CRAWL, id='JOB_STEP_ID', datasource='RKM', job_id='JOB_ID')

    job_repository = JobRepository(innovation_suite)
    try:
        job_repository.claim_job_step(job_step)
        assert False  # expecting an error
    except JobStepClaimConflictError as e:
        assert e.job_step.id == job_step.id
        assert e.job_step.status == JobStepStatus.DONE
        assert e.job_step.executing_node is not None
        assert e.job_step.executing_node == 'some other node'
        innovation_suite.update_record.assert_not_called()


def test_claim_job_step_with_http_error(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.jwt_login.return_value = 'JWT_TOKEN'

    fresh_record = Record(recordDefinitionName=data_connection_job_step.FORM)
    fresh_record[ar_core_fields.FIELD_STATUS] = str(JobStepStatus.PENDING)
    fresh_record[data_connection_job_step.FIELD_ERROR_DETAILS] = None
    fresh_record[data_connection_job_step.FIELD_EXECUTING_NODE] = 'some other node'
    innovation_suite.get_record.return_value = fresh_record

    request_exception = HTTPError(responses.put('http://example'))
    innovation_suite.update_record.side_effect = request_exception

    job_step = JobStep(JobType.CRAWL, id='JOB_STEP_ID', datasource='RKM', job_id='JOB_ID')

    job_repository = JobRepository(innovation_suite)
    try:
        job_repository.claim_job_step(job_step)
        assert False  # error expected
    except JobStepClaimError as e:
        assert e.job_step.id == job_step.id
        assert e.job_step.status == JobStepStatus.PENDING
        assert e.job_step.executing_node is not None
        assert e.job_step.executing_node == 'some other node'
        assert e.__cause__ == request_exception

        innovation_suite.update_record.assert_called_once()


def test_mark_job_step_as_done(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.update_record.return_value = None

    job_repository = JobRepository(innovation_suite)
    job_repository.mark_job_step_as_done('JOB_ID')

    innovation_suite.update_record.assert_called_once()
    update_record: Record = innovation_suite.update_record.mock_calls[0].args[0]
    assert update_record[ar_core_fields.FIELD_STATUS] == str(int(JobStepStatus.DONE))
    assert data_connection_job_step.FIELD_EXECUTING_NODE not in update_record.fieldInstances
    assert data_connection_job_step.FIELD_ERROR_DETAILS not in update_record.fieldInstances


def test_mark_job_step_as_error(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)
    innovation_suite.update_record.return_value = None

    job_repository = JobRepository(innovation_suite)
    job_repository.mark_job_step_as_error('JOB_ID', 'ERROR_DETAILS')

    innovation_suite.update_record.assert_called_once()
    update_record: Record = innovation_suite.update_record.mock_calls[0].args[0]
    assert update_record[ar_core_fields.FIELD_STATUS] == str(int(JobStepStatus.ERROR))
    assert data_connection_job_step.FIELD_EXECUTING_NODE not in update_record.fieldInstances
    assert update_record[data_connection_job_step.FIELD_ERROR_DETAILS] == 'ERROR_DETAILS'


def test_get_job(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)

    record = Record(recordDefinitionName=data_connection_job.FORM, id='JOB_ID')
    record[ar_core_fields.FIELD_ID] = record.id
    record[data_connection_job.FIELD_DATASOURCE] = 'RKM'
    innovation_suite.get_record.return_value = record

    job_repository = JobRepository(innovation_suite)
    job = job_repository.get_job('JOB_ID')

    innovation_suite.get_record.assert_called_once_with(data_connection_job.FORM, 'JOB_ID')
    assert job.id == 'JOB_ID'
    assert job.datasource == 'RKM'


def test_get_job_step(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)

    record = Record(recordDefinitionName=data_connection_job_step.FORM, id='JOB_STEP_ID')
    record[ar_core_fields.FIELD_ID] = record.id
    record[ar_core_fields.FIELD_DISPLAY_ID] = 'JOB_STEP_DISPLAY_ID'
    record[data_connection_job.FIELD_DATASOURCE] = 'RKM'
    innovation_suite.get_record.return_value = record

    job_repository = JobRepository(innovation_suite)
    job_step = job_repository.get_job_step('JOB_STEP_ID')

    innovation_suite.get_record.assert_called_once_with(data_connection_job_step.FORM, 'JOB_STEP_ID')
    assert job_step.id == 'JOB_STEP_ID'
    assert job_step.display_id == 'JOB_STEP_DISPLAY_ID'
    assert job_step.datasource == 'RKM'


@pytest.mark.parametrize('data,expected_result', [
    ([{ar_core_fields.FIELD_ID: 'JOB_STEP_ID'}], True),
    ([], False),
])
def test_has_job_steps(mocker: MockerFixture, data: [Dict], expected_result: bool):
    innovation_suite = mocker.Mock(InnovationSuite)

    data_page = RecordDataPage(totalSize=None, data=data)
    innovation_suite.get_records.return_value = data_page

    job_repository = JobRepository(innovation_suite)
    assert job_repository.has_job_steps('JOB_ID') == expected_result

    innovation_suite.get_records.assert_called_once()
    assert innovation_suite.get_records.mock_calls[0].args[0] == data_connection_job_step.FORM
    assert innovation_suite.get_records.mock_calls[0].kwargs['query_expression'] ==\
           f"'{data_connection_job_step.FIELD_JOB_ID}' = \"JOB_ID\""


def test_get_pending_job_steps_with_empty_result(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)

    data_page = RecordDataPage(totalSize=None, data=[])
    innovation_suite.get_records.return_value = data_page

    job_repository = JobRepository(innovation_suite)
    result = job_repository.get_pending_job_steps('JOB_ID', limit=50)

    assert result == []
    innovation_suite.get_records.assert_called_once()
    assert innovation_suite.get_records.mock_calls[0].args[0] == data_connection_job_step.FORM
    assert innovation_suite.get_records.mock_calls[0].kwargs['page_size'] == 50
    query_expression = innovation_suite.get_records.mock_calls[0].kwargs['query_expression']
    assert query_expression == f"'{data_connection_job_step.FIELD_JOB_ID}' = \"JOB_ID\" AND" \
                               f" '{ar_core_fields.FIELD_STATUS}' = {JobStepStatus.PENDING}"


def test_get_pending_job_steps_after_display_id(mocker: MockerFixture):
    innovation_suite = mocker.Mock(InnovationSuite)

    record = {str(ar_core_fields.FIELD_ID): 'JOB_STEP_ID'}
    data_page = RecordDataPage(totalSize=None, data=[record])
    innovation_suite.get_records.return_value = data_page

    job_repository = JobRepository(innovation_suite)
    result = job_repository.get_pending_job_steps('JOB_ID', limit=50, after_display_id='AFTER_DISPLAY_ID')

    assert len(result) == 1
    assert result[0].id == 'JOB_STEP_ID'
    innovation_suite.get_records.assert_called_once()
    assert innovation_suite.get_records.mock_calls[0].args[0] == data_connection_job_step.FORM
    assert innovation_suite.get_records.mock_calls[0].kwargs['page_size'] == 50
    query_expression = innovation_suite.get_records.mock_calls[0].kwargs['query_expression']
    assert query_expression == f"'{data_connection_job_step.FIELD_JOB_ID}' = \"JOB_ID\" AND" \
                               f" '{ar_core_fields.FIELD_STATUS}' = {JobStepStatus.PENDING} AND" \
                               f" '{ar_core_fields.FIELD_DISPLAY_ID}' > \"AFTER_DISPLAY_ID\""
