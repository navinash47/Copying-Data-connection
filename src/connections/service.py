from abc import ABC

from connections.models import Connection
from helixplatform import ar_core_fields
from helixplatform.models import Record
from helixplatform.service import InnovationSuite


class ConnectionRepository:
    """
    Repository class to provide access to Job Connection configurations
    """

    def __init__(self, is_client: InnovationSuite = None):
        self.__is_client = is_client or InnovationSuite()

    def get_connection(self, form: str, connection_id: str) -> Record:
        return self.__is_client.get_record(form, connection_id)


class ConnectionLoader(ABC):

    def __init__(self, connection_id: str, connection_repository: ConnectionRepository) -> None:
        super().__init__()
        self._connection_id = connection_id
        self._connection_repository = connection_repository

    def load(self) -> Connection:
        """
        Load a connection configuration object from the repository
        :return: connection information
        """
        record = self._connection_repository.get_connection(self.get_record_definition_name(), self._connection_id)
        return self.from_record(record)

    @classmethod
    def get_record_definition_name(cls):
        """
        Provides the name of the record definition that contains the connection configuration information for this
        connection loader.
        :return: the record definition name that holds the connection configuration
        """
        return "com.bmc.dsom.hgm:Connection"

    @classmethod
    def from_record(cls, record: Record) -> Connection:
        """
        Create a new connection object from the given record instance
        :param record: record to convert to a connection
        :return: the created connection
        """
        return Connection(
            id=record[ar_core_fields.FIELD_ID],
        )
