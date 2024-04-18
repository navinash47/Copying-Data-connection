from pytest_mock import MockerFixture

from connections.service import ConnectionLoader, ConnectionRepository
from helixplatform.models import Record
from helixplatform.service import InnovationSuite


def test_get_connection_from_repository(mocker: MockerFixture):
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:Connection')
    client = mocker.Mock(InnovationSuite)
    client.get_record.return_value = record

    repository = ConnectionRepository(client)

    returned = repository.get_connection('com.bmc.dsom.hgm:Connection', '1234')
    assert returned == record
    client.get_record.assert_called_once_with('com.bmc.dsom.hgm:Connection', '1234')


def test_load_connection(mocker: MockerFixture):
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:Connection')
    record[379] = '1234'
    repository = mocker.Mock(ConnectionRepository)
    repository.get_connection.return_value = record

    loader = ConnectionLoader('1234', repository)
    connection = loader.load()

    assert connection.id == '1234'
    repository.get_connection.asssert_called_once_with('com.bmc.dsom.hgm:Connection', '1234')
