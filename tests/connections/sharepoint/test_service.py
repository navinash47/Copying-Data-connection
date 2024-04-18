import datetime

import pytest
from O365.drive import DriveItem, Drive, Folder
from O365.sharepoint import Site, Sharepoint
from pytest_mock import MockerFixture

from connections.service import ConnectionRepository
from connections.sharepoint.constants import SUPPORTED_FILES
from connections.sharepoint.models import SharePointConnection
from connections.sharepoint.service import SharePointConnectionLoader, SharePoint
from helixplatform.models import Record


@pytest.fixture()
def connection(mocker: MockerFixture) -> SharePointConnection:
    connection = mocker.Mock(SharePointConnection)
    connection.client_id = mocker.Mock()
    connection.client_secret = mocker.Mock()
    connection.tenant_id = mocker.Mock()
    connection.site = "test.sharepoint.com/sites/test_site"
    connection.id = mocker.Mock()
    return connection


def test_get_record_definition_name(mocker: MockerFixture):
    connection_id = 'connection-1234'
    connection_repository = mocker.Mock(ConnectionRepository)
    loader = SharePointConnectionLoader(connection_id, connection_repository)
    assert loader.get_record_definition_name() == 'com.bmc.dsom.hgm:Connection_SharePoint'


def test_from_record(mocker: MockerFixture):
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:Connection_SharePoint')
    record[379] = '1234'
    record[490000250] = 'client-id-value'
    record[490000251] = 'client-secret-value'
    record[490000252] = 'tenant-id-value'
    record[490000253] = 'tenant-name-value'
    record[490000254] = 'site-value'

    connection_id = 'connection-1234'
    connection_repository = mocker.Mock(ConnectionRepository)
    connection = SharePointConnectionLoader(connection_id, connection_repository).from_record(record)

    assert isinstance(connection, SharePointConnection)
    assert connection.id == '1234'
    assert connection.client_id == 'client-id-value'
    assert connection.client_secret == 'client-secret-value'
    assert connection.tenant_id == 'tenant-id-value'
    assert connection.tenant_name == 'tenant-name-value'
    assert connection.site == 'site-value'


def test_get_files_from_sharepoint(mocker: MockerFixture, connection: SharePointConnection):
    sharepoint = SharePoint()
    mocker.patch('O365.account.Account.authenticate', return_value=True)
    sharepoint_mock = mocker.Mock(Sharepoint)
    mocker.patch('O365.account.Account.sharepoint', return_value=sharepoint_mock)
    site = mocker.Mock(Site)
    sharepoint_mock.get_site.return_value = site
    library = mocker.Mock(Drive)
    site.list_document_libraries.return_value = [library]

    file = mocker.Mock(DriveItem)
    file.is_file = True
    file.is_folder = False
    file.mime_type = "application/pdf"
    file.name = "test.pdf"

    library.get_items.return_value = [file]
    files = sharepoint.get_files(connection, SUPPORTED_FILES)
    assert len(files) == 1


def test_get_files_from_sharepoint_throw_exception_not_valid_credentials(mocker: MockerFixture,
                                                                         connection: SharePointConnection):
    sharepoint = SharePoint()
    mocker.patch('O365.account.Account.authenticate', return_value=False)
    with pytest.raises(RuntimeError):
        sharepoint.get_files(connection, SUPPORTED_FILES)


def test_get_files_from_sharepoint_throw_exception_wrong_site_format(mocker: MockerFixture,
                                                                     connection: SharePointConnection):
    sharepoint = SharePoint()
    connection.site = "WRONG_SITE_FORMAT"
    mocker.patch('O365.account.Account.authenticate', return_value=True)
    sharepoint_mock = mocker.Mock(Sharepoint)
    mocker.patch('O365.account.Account.sharepoint', return_value=sharepoint_mock)
    with pytest.raises(RuntimeError):
        sharepoint.get_files(connection, SUPPORTED_FILES)


def test_get_files_from_sharepoint_not_supported_file_extensions(mocker: MockerFixture,
                                                                 connection: SharePointConnection):
    sharepoint = SharePoint()
    mocker.patch('O365.account.Account.authenticate', return_value=True)
    sharepoint_mock = mocker.Mock(Sharepoint)
    mocker.patch('O365.account.Account.sharepoint', return_value=sharepoint_mock)
    site = mocker.Mock(Site)
    sharepoint_mock.get_site.return_value = site
    library = mocker.Mock(Drive)
    site.list_document_libraries.return_value = [library]

    file = mocker.Mock(DriveItem)
    file.is_file = True
    file.is_folder = False
    file.mime_type = "any_other_type"
    file.name = "test.xlsx"

    library.get_items.return_value = [file]
    files = sharepoint.get_files(connection, SUPPORTED_FILES)
    assert len(files) == 0


def test_get_files_from_sharepoint_only_folders(mocker: MockerFixture,
                                                connection: SharePointConnection):
    sharepoint = SharePoint()
    mocker.patch('O365.account.Account.authenticate', return_value=True)
    sharepoint_mock = mocker.Mock(Sharepoint)
    mocker.patch('O365.account.Account.sharepoint', return_value=sharepoint_mock)
    site = mocker.Mock(Site)
    sharepoint_mock.get_site.return_value = site
    library = mocker.Mock(Drive)
    site.list_document_libraries.return_value = [library]

    folder = mocker.Mock(Folder)
    folder.is_file = False
    folder.is_folder = True
    folder.name = "test_folder"
    folder.get_items.return_value = []

    library.get_items.return_value = [folder]
    files = sharepoint.get_files(connection, SUPPORTED_FILES)
    assert len(files) == 0


def test_get_files_from_sharepoint_with_modified_since_field(mocker: MockerFixture,
                                                             connection: SharePointConnection):
    date_format = '%Y-%m-%dT%H:%M:%S.%f%z'
    date = "2024-01-29T13:20:00.000Z"
    modified_since = "2024-01-29T13:10:00.000Z"

    sharepoint = SharePoint()
    mocker.patch('O365.account.Account.authenticate', return_value=True)
    sharepoint_mock = mocker.Mock(Sharepoint)
    mocker.patch('O365.account.Account.sharepoint', return_value=sharepoint_mock)
    site = mocker.Mock(Site)
    sharepoint_mock.get_site.return_value = site
    library = mocker.Mock(Drive)
    site.list_document_libraries.return_value = [library]

    file = mocker.Mock(DriveItem)
    file.is_file = True
    file.is_folder = False
    file.mime_type = "application/pdf"
    file.modified = datetime.datetime.strptime(date, date_format)
    file.name = "test.pdf"

    library.get_items.return_value = [file]
    files = sharepoint.get_files(connection, SUPPORTED_FILES, datetime.datetime.strptime(modified_since, date_format))
    assert len(files) == 1


def test_get_files_from_sharepoint_with_modified_since_field_and_no_match(mocker: MockerFixture,
                                                                          connection: SharePointConnection):
    date_format = '%Y-%m-%dT%H:%M:%S.%f%z'
    date = "2024-01-29T13:20:00.000Z"
    modified_since = "2024-01-29T13:30:00.000Z"

    sharepoint = SharePoint()
    mocker.patch('O365.account.Account.authenticate', return_value=True)
    sharepoint_mock = mocker.Mock(Sharepoint)
    mocker.patch('O365.account.Account.sharepoint', return_value=sharepoint_mock)
    site = mocker.Mock(Site)
    sharepoint_mock.get_site.return_value = site
    library = mocker.Mock(Drive)
    site.list_document_libraries.return_value = [library]

    file = mocker.Mock(DriveItem)
    file.is_file = True
    file.is_folder = False
    file.mime_type = "application/pdf"
    file.modified = datetime.datetime.strptime(date, date_format)
    file.name = "test.pdf"

    library.get_items.return_value = [file]
    files = sharepoint.get_files(connection, SUPPORTED_FILES, datetime.datetime.strptime(modified_since, date_format))
    assert len(files) == 0
