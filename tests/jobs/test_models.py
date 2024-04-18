from datetime import datetime, timezone

import pytest

from helixplatform import ar_core_fields, data_connection_job_step
from helixplatform.models import Record
from jobs.constants import JobStepStatus, JobType
from jobs.models import Job, JobStep


def test_to_set_status_record_in_progress():
    record = JobStep.to_set_status_record('TEST_ID', JobStepStatus.IN_PROGRESS)
    assert record.id == 'TEST_ID'
    assert record[ar_core_fields.FIELD_STATUS] == str(int(JobStepStatus.IN_PROGRESS))
    assert record[data_connection_job_step.FIELD_EXECUTING_NODE] is None
    assert str(data_connection_job_step.FIELD_EXECUTING_NODE) not in record.fieldInstances
    assert record[data_connection_job_step.FIELD_ERROR_DETAILS] is None
    # error details explicitly set to null
    assert str(data_connection_job_step.FIELD_ERROR_DETAILS) in record.fieldInstances


def test_to_set_status_record_in_progress_with_execution_node():
    record = JobStep.to_set_status_record('TEST_ID', JobStepStatus.IN_PROGRESS, executing_node='NODE')
    assert record.id == 'TEST_ID'
    assert record[ar_core_fields.FIELD_STATUS] == str(int(JobStepStatus.IN_PROGRESS))
    assert record[data_connection_job_step.FIELD_EXECUTING_NODE] == 'NODE'
    assert record[data_connection_job_step.FIELD_ERROR_DETAILS] is None


def test_to_set_status_record_in_progress_with_error_details():
    record = JobStep.to_set_status_record('TEST_ID', JobStepStatus.IN_PROGRESS, error_details='ERROR')
    assert record.id == 'TEST_ID'
    assert record[ar_core_fields.FIELD_STATUS] == str(int(JobStepStatus.IN_PROGRESS))
    assert record[data_connection_job_step.FIELD_EXECUTING_NODE] is None
    assert record[data_connection_job_step.FIELD_ERROR_DETAILS] == 'ERROR'


def test_to_set_status_record_error_with_error_details():
    record = JobStep.to_set_status_record('TEST_ID', JobStepStatus.ERROR, error_details='ERROR')
    assert record.id == 'TEST_ID'
    assert record[ar_core_fields.FIELD_STATUS] == str(int(JobStepStatus.ERROR))
    assert record[data_connection_job_step.FIELD_EXECUTING_NODE] is None
    assert record[data_connection_job_step.FIELD_ERROR_DETAILS] == 'ERROR'


def test_job_step_from_data_page_dict():
    job_step = JobStep.from_data_page_dict({
        str(ar_core_fields.FIELD_ID): 'ID',
        str(ar_core_fields.FIELD_DISPLAY_ID): 'DISPLAY_ID',
        str(ar_core_fields.FIELD_STATUS): int(JobStepStatus.PENDING),
        str(data_connection_job_step.FIELD_TYPE): int(JobType.CRAWL),
        str(data_connection_job_step.FIELD_DATASOURCE): 'RKM'
    })
    assert job_step.id == 'ID'
    assert job_step.display_id == 'DISPLAY_ID'
    assert job_step.status == JobStepStatus.PENDING
    assert job_step.type == JobType.CRAWL
    assert job_step.datasource == 'RKM'


def test_create_job_from_record_all_values():
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:DataConnectionJob')
    record[379] = 'AGGADGG8ECDC2ASBADLGSBADLG919H'  # GUID
    record[490000150] = 'RKM'   # DataSource
    record[490000151] = 'a-document-id'
    record[490000152] = 'a-document-display-id'
    record[490000155] = '2023-12-15T23:19:54.050Z'  # Modified Since
    record[490000156] = 'the-connection-id'
    record[490000157] = '1'

    job = Job.from_record(record)

    assert job.id == 'AGGADGG8ECDC2ASBADLGSBADLG919H'
    assert job.datasource == 'RKM'
    assert job.doc_id == 'a-document-id'
    assert job.doc_display_id == 'a-document-display-id'
    assert job.modified_since == datetime(2023, 12, 15, 23, 19, 54, 50000, tzinfo=timezone.utc)
    assert job.connection_id == 'the-connection-id'
    assert job.sync_deletions is True


def test_create_job_from_record_required_values():
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:DataConnectionJob')
    record[379] = 'AGGDI202EKB9FASFX5EOSFX5EO5SX6'  # GUID
    record[490000150] = 'HKM'   # DataSource
    record[490000151] = None
    record[490000152] = None
    record[490000155] = None
    record[490000156] = None
    record[490000157] = None

    job = Job.from_record(record)

    assert job.id == 'AGGDI202EKB9FASFX5EOSFX5EO5SX6'
    assert job.datasource == 'HKM'
    assert job.doc_id is None
    assert job.doc_display_id is None
    assert job.modified_since is None
    assert job.connection_id is None
    assert job.sync_deletions is None


def test_create_job_from_record_missing_values():
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:DataConnectionJob')
    record[379] = 'AGGDI202EKB9FASFX5F6SFX5F65TBA'  # GUID
    record[490000150] = 'BWF'  # DataSource

    job = Job.from_record(record)

    assert job.id == 'AGGDI202EKB9FASFX5F6SFX5F65TBA'
    assert job.datasource == 'BWF'
    assert job.doc_id is None
    assert job.doc_display_id is None
    assert job.modified_since is None
    assert job.connection_id is None
    assert job.sync_deletions is None


@pytest.mark.parametrize('sync_deletions,expected', [(None, True), (True, True), (False, False)])
def test_defaulted_sync_deletions(sync_deletions, expected):
    job = Job('RKM', sync_deletions=sync_deletions)
    assert job.defaulted_sync_deletions == expected
