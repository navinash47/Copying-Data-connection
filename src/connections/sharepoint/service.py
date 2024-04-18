from datetime import datetime
from typing import List

from O365 import Account
from O365.drive import Drive, File, DriveItem
from O365.sharepoint import Site

from connections.models import Connection
from connections.service import ConnectionLoader
from connections.sharepoint.models import SharePointConnection
from helixplatform import ar_core_fields
from helixplatform.models import Record


class SharePointConnectionLoader(ConnectionLoader):
    FIELD_CLIENT_ID = 490000250
    FIELD_CLIENT_SECRET = 490000251
    FIELD_TENANT_ID = 490000252
    FIELD_TENANT_NAME = 490000253
    FIELD_SITE = 490000254

    @classmethod
    def get_record_definition_name(cls):
        return "com.bmc.dsom.hgm:Connection_SharePoint"

    @classmethod
    def from_record(cls, record: Record) -> Connection:
        return SharePointConnection(
            id=record[ar_core_fields.FIELD_ID],
            client_id=record[cls.FIELD_CLIENT_ID],
            client_secret=record[cls.FIELD_CLIENT_SECRET],
            tenant_id=record[cls.FIELD_TENANT_ID],
            tenant_name=record[cls.FIELD_TENANT_NAME],
            site=record[cls.FIELD_SITE],
        )


class SharePoint:

    def _get_site(self, connection: SharePointConnection) -> Site:
        credentials = (connection.client_id, connection.client_secret)
        account = Account(credentials, auth_flow_type='credentials', tenant_id=connection.tenant_id)
        if not account.authenticate(store_token=False):
            raise RuntimeError('Error authenticating user with id {}', connection.id)
        sharepoint = account.sharepoint()
        hostname, path = self.get_site_details(connection.site)
        return sharepoint.get_site(hostname, path)

    @staticmethod
    def get_site_details(site: str) -> List[str]:
        details = site.split('/')
        if len(details) == 1:
            raise RuntimeError('Not valid site format')
        hostname = details[0]
        path = f'{details[1]}/{details[2]}'
        return hostname, path

    def get_all_files_from_folder(self, library: Drive, files: List[File], supported_files: List[str],
                                  modified_since: datetime | None = None):
        for file in library.get_items():
            if file.is_folder:
                self.get_all_files_from_folder(file, files, supported_files, modified_since)
            else:
                if file.mime_type in supported_files and (not modified_since or file.modified >= modified_since):
                    files.append(file)

    def get_files(self, connection: SharePointConnection, supported_files: List[str],
                  modified_since: datetime | None = None) -> List[str]:
        site = self._get_site(connection)
        libraries = site.list_document_libraries()
        files = []
        for library in libraries:
            self.get_all_files_from_folder(library, files, supported_files, modified_since)
        return files

    def get_file(self, connection: SharePointConnection, library_id: str, file_id: str, ) -> DriveItem:
        site = self._get_site(connection)
        library = site.get_document_library(library_id)
        file = library.get_item(file_id)
        return file
