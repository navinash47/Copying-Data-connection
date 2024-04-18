from dataclasses import asdict
from datetime import datetime

import pytest
from pytest_mock import MockerFixture

from connections.confluence.schemas import AttachmentMetaData, ConfluencePage
from connections.confluence.service import ConfluenceConnection
from connections.confluence.service import ConfluenceService


@pytest.fixture
def confluence_connection():
    return ConfluenceConnection(id="test_id", page_id="test_page_id", url="test_url", access_token="")


@pytest.fixture
def mock_confluence(mocker: MockerFixture):
    mock_confluence = mocker.Mock()
    mocker.patch('connections.confluence.service.Confluence', return_value=mock_confluence)
    return mock_confluence


def test_get_child_pages_ids_success(confluence_connection, mock_confluence):
    test_cases = [
        {
            "page_id": 'page1',
            "mock_child_ids": {'page1': ['page2', 'page3'], 'page2': ['page4'], 'page3': [], 'page4': []},
            "expected": ['page1', 'page2', 'page4', 'page3']
        },
    ]

    confluence_service = ConfluenceService(confluence_connection)

    for test_case in test_cases:
        page_id = test_case["page_id"]
        mock_child_ids = test_case["mock_child_ids"]
        expected = test_case["expected"]

        confluence_service.confluence.get_child_id_list.side_effect = lambda x: mock_child_ids.get(x, [])

        result = confluence_service.get_page_with_all_child_ids([page_id])

        assert sorted(list(result)) == sorted(expected)


def test_get_child_for_pages_without_children(confluence_connection, mock_confluence):
    parent_page_id = 'parent_id'

    mock_child_pages_ids = []
    mock_confluence.get_child_id_list.return_value = mock_child_pages_ids

    confluence_service = ConfluenceService(confluence_connection)

    result = list(confluence_service.get_page_with_all_child_ids([parent_page_id]))
    assert result == [parent_page_id]
    mock_confluence.get_child_id_list.assert_called_once_with(parent_page_id)


def test_get_child_pages_ids_throw_exception(confluence_connection, mock_confluence):
    parent_page_id = 'parent_id'

    mock_confluence.get_child_id_list.side_effect = Exception("Test exception")

    confluence_service = ConfluenceService(confluence_connection)

    with pytest.raises(Exception) as excinfo:
        list(confluence_service.get_page_with_all_child_ids([parent_page_id]))

    assert "Test exception" in str(excinfo.value)
    mock_confluence.get_child_id_list.assert_called_once_with(parent_page_id)


def test_get_page_content_success(confluence_connection, mock_confluence):
    page_id = '124'
    page_content = {'id': page_id, 'title': 'PageTitle', 'space': {'name': 'ns'},
                    'body': {'storage': {'value': 'pageContent'}}, 'type': 'page',
                    'version': {'when': '2024-01-26T17:31:36.287-06:00'},
                    '_links': {'webui': '/display/Space/PageTitle', 'base': 'https://base_url_test'},
                    'status': 'current'}
    mock_confluence.get_page_by_id.return_value = page_content

    confluence_service = ConfluenceService(confluence_connection)

    result = confluence_service.get_page('124')

    page = ConfluencePage(
        id_=page_id,
        title='PageTitle',
        content='pageContent',
        space_name='ns',
        base_url='https://base_url_test',
        web_url='https://base_url_test/display/Space/PageTitle',
        last_modified=datetime.strptime('2024-01-26T17:31:36.287-06:00', '%Y-%m-%dT%H:%M:%S.%f%z'))

    assert result == page
    mock_confluence.get_page_by_id.assert_called_once_with(page_id, expand='space,body.storage,version')


def test_get_page_content_throw_exception(confluence_connection, mock_confluence):
    parent_page_id = 'parent_id'
    mock_confluence.get_page_by_id.side_effect = Exception("Test exception")

    confluence_service = ConfluenceService(confluence_connection)
    with pytest.raises(Exception) as excinfo:
        confluence_service.get_page(parent_page_id)
    assert "Test exception" in str(excinfo.value)


def test_get_page_attachments_metadata(confluence_connection, mock_confluence):
    page = ConfluencePage(
        id_='124',
        title='title',
        content='content',
        space_name='ns',
        base_url='https://base_url_test',
        web_url='https://base_url_test/display/NameSpace/ConfluencePage',
        last_modified=datetime.strptime('2024-01-26T17:31:36.287-06:00', '%Y-%m-%dT%H:%M:%S.%f%z'))

    page_attachment_data = {'results': [{'id': 'page_attachment_data_id', 'status': 'current', 'title': 'test.pdf',

                                         'metadata': {'mediaType': 'application/pdf', 'labels': {'_links': {'self': 'self_link'}}},
                                         '_links': {'webui': 'webui_url', 'download': 'download_url'},
                                         'version': {'when': '2024-01-03T14:39:42.090-06:00'}}]}
    mock_confluence.get_attachments_from_content.return_value = page_attachment_data
    confluence_service = ConfluenceService(confluence_connection)

    result = confluence_service.get_page_attachments_metadata(page)

    assert result == [
        AttachmentMetaData(id_='page_attachment_data_id', title='test.pdf', source='ns/124/page_attachment_data_id',
                           mime_type='application/pdf', status='current', download_link='download_url',
                           web_url='https://base_url_testwebui_url',
                           last_modified=datetime.strptime('2024-01-03T14:39:42.090-06:00', '%Y-%m-%dT%H:%M:%S.%f%z'))]
