import pytest
from pytest_mock import MockerFixture

from connections.service import ConnectionRepository
from connections.sharepoint.feature import SharePointFeature
from connections.sharepoint.service import SharePointConnectionLoader
from jobs.constants import JobType
from jobs.models import Job, JobStep
from jobs.schemas import JobRequest
from jobs.service import DeleteDocBy


@pytest.mark.parametrize('datasource,accepted', [
    ('', False),
    (' ', False),
    ('RKM', False),
    ('SPT', True),
])
def test_accept_job(datasource: str, accepted: bool):
    job = Job(datasource=datasource)
    assert SharePointFeature().accept_job(job) is accepted


@pytest.mark.parametrize('datasource,accepted', [
    ('', False),
    (' ', False),
    ('RKM', False),
    ('SPT', True),
])
def test_accept_job_request(datasource: str, accepted: bool):
    job_request = JobRequest(datasource=datasource)
    assert SharePointFeature().accept_job_request(job_request) is accepted


def test_create_first_job_step():
    job = Job(datasource='SPT', doc_id='doc-1', doc_display_id='doc-display-1')
    job_step = SharePointFeature().create_first_job_step(job)

    assert job_step.type == JobType.CRAWL
    assert job_step.datasource == 'SPT'
    assert job_step.doc_id == 'doc-1'
    assert job_step.doc_display_id == 'doc-display-1'


def test_delete_doc_by():
    job = Job('SPT')
    job_step = JobStep(JobType.LOAD, job.datasource, doc_id='DOC ID')
    assert SharePointFeature().get_delete_doc_by(job, job_step) == DeleteDocBy.BY_DOC_ID


def test_get_connection_loader(mocker: MockerFixture):
    connection_id = 'connection-1234'
    connection_repository = mocker.Mock(ConnectionRepository)

    feature = SharePointFeature()
    loader = feature.get_connection_loader(connection_id, connection_repository)

    assert isinstance(loader, SharePointConnectionLoader)
    assert loader._connection_id == connection_id
    assert loader._connection_repository == connection_repository
