from opensearchpy.client.indices import IndicesClient
from opensearchpy.exceptions import OpenSearchException, NotFoundError, RequestError
from opensearchpy.transport import Transport
from pytest_mock import MockerFixture

from config import Settings
from health.models import HealthStatus
from opensearch.client import IndexHealthIndicator, get_open_search_url, OpenSearchClient


def test_get_health_server_index_exists(mocker: MockerFixture):
    open_search = mocker.Mock(OpenSearchClient)
    transport = mocker.Mock(Transport)
    mocker.patch('opensearch.client.get_open_search_client', return_value=open_search)

    open_search.transport = transport
    transport.perform_request.return_value = True

    indicator = IndexHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.UP


def test_get_health_server_index_does_not_exists(mocker: MockerFixture):
    open_search = mocker.Mock(OpenSearchClient)
    mocker.patch('opensearch.client.get_open_search_client', return_value=open_search)

    open_search.ping.return_value = False

    indicator = IndexHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.DOWN


def test_get_health_server_index_handles_error(mocker: MockerFixture):
    open_search = mocker.Mock(OpenSearchClient)
    mocker.patch('opensearch.client.get_open_search_client', return_value=open_search)

    open_search.ping.side_effect = OpenSearchException('bad news')

    indicator = IndexHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.DOWN


def test_get_open_search_url():
    Settings.OPENSEARCH_HOST = 'myhost'
    Settings.OPENSEARCH_PORT = 4444
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = 'user1'
    Settings.OPENSEARCH_USER_PASSWORD = 'pwd1'

    url = get_open_search_url()

    assert url == 'https://user1:pwd1@myhost:4444'


def test_get_open_search_insecure():
    Settings.OPENSEARCH_HOST = 'myhost'
    Settings.OPENSEARCH_PORT = 4444
    Settings.OPENSEARCH_SECURE = False
    Settings.OPENSEARCH_USER = 'user1'
    Settings.OPENSEARCH_USER_PASSWORD = 'pwd1'

    url = get_open_search_url()

    assert url == 'http://user1:pwd1@myhost:4444'


def test_get_open_search_url_no_port():
    Settings.OPENSEARCH_HOST = 'myhost'
    Settings.OPENSEARCH_PORT = None
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = 'user1'
    Settings.OPENSEARCH_USER_PASSWORD = 'pwd1'

    url = get_open_search_url()

    assert url == 'https://user1:pwd1@myhost'


def test_get_open_search_url_no_user():
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = 'pwd1'

    url = get_open_search_url()

    assert url == 'https://anotherhost:6666'


def test_get_open_search_url_no_password():
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = 'user1'
    Settings.OPENSEARCH_USER_PASSWORD = ''

    url = get_open_search_url()

    assert url == 'https://anotherhost:6666'


def test_get_open_search_url_no_credentials():
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''

    url = get_open_search_url()

    assert url == 'https://anotherhost:6666'


def test_get_opensearch_url_reserved_characters_uname_pwd():
    Settings.OPENSEARCH_HOST = 'hostname'
    Settings.OPENSEARCH_PORT = 9200
    # ?#@ are all reserved
    Settings.OPENSEARCH_USER = 'ad:m&#?/in'
    Settings.OPENSEARCH_USER_PASSWORD = 'P+Kc#@74D/?#A4Wq'

    url = get_open_search_url()
    assert url == 'https://ad%3Am%26%23%3F/in:P%2BKc%23%4074D/%3F%23A4Wq@hostname:9200'


def test_ensure_application_index_created_when_exists(mocker: MockerFixture):
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'
    index = {'name': Settings.OPENSEARCH_INDEX}

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.return_value = index

    client.ensure_application_index_created()
    client.indices.get.assert_called_once_with(Settings.OPENSEARCH_INDEX)

    # Next call should hit the cached value.
    client.indices.get.reset_mock()
    client.ensure_application_index_created()
    client.indices.get.assert_not_called()


def test_ensure_application_index_created_successfully_creates(mocker: MockerFixture):
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.side_effect = NotFoundError(404,
                                                   'index_not_found_exception',
                                                   'no such index [my-index]',
                                                   'my-index',
                                                   'index_or_alias')
    index = {'name': Settings.OPENSEARCH_INDEX}
    client.indices.create.return_value = index

    client.ensure_application_index_created()

    client.indices.get.assert_called_once()
    client.indices.create.assert_called_once()
    assert client.indices.create.mock_calls[0].args[0] == Settings.OPENSEARCH_INDEX
    assert type(client.indices.create.mock_calls[0].kwargs['body']) is dict


def test_ensure_application_index_created_tries_but_index_already_created(mocker: MockerFixture):
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.side_effect = NotFoundError(404,
                                                   'index_not_found_exception',
                                                   'no such index [my-index]',
                                                   'my-index',
                                                   'index_or_alias')
    client.indices.create.side_effect = RequestError(400,
                                                     'resource_already_exists_exception',
                                                     'index already exists [my-index]',
                                                     'my-index',
                                                     'index_or_alias')

    client.ensure_application_index_created()  # the point is that the exception should be caught and not rethrown

    client.indices.get.assert_called_once()
    client.indices.create.assert_called_once()
    assert client.indices.create.mock_calls[0].args[0] == Settings.OPENSEARCH_INDEX
    assert type(client.indices.create.mock_calls[0].kwargs['body']) is dict

    # Next call should hit the cached value.
    client.indices.get.reset_mock()
    client.ensure_application_index_created()
    client.indices.get.assert_not_called()


def test_ensure_application_index_created_tries_with_error_at_creation(mocker: MockerFixture):
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.side_effect = NotFoundError(404,
                                                   'index_not_found_exception',
                                                   'no such index [my-index]',
                                                   'my-index',
                                                   'index_or_alias')
    client.indices.create.side_effect = RequestError(406,
                                                     'not_acceptable',
                                                     'unacceptable index creation [my-index]',
                                                     'my-index',
                                                     'index_or_alias')

    try:
        client.ensure_application_index_created()
    except RequestError as e:
        assert e.error == 'not_acceptable'


def test_ensure_application_index_created_no_rethrow_when_exists(mocker: MockerFixture):
    # Just checking the minimum successful scenario to make sure we are calling the underlying
    # `ensure_application_index_created()` method.
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'
    index = {'name': Settings.OPENSEARCH_INDEX}

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.return_value = index

    client.ensure_application_index_created_no_rethrow()
    client.indices.get.assert_called_once_with(Settings.OPENSEARCH_INDEX)


def test_ensure_application_index_created_no_error_when_down(mocker: MockerFixture):
    # Just checking the minimum successful scenario to make sure we are calling the underlying
    # `ensure_application_index_created()` method.
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.side_effect = ConnectionError('closed by peer')

    client.ensure_application_index_created_no_rethrow()
    client.indices.get.assert_called_once_with(Settings.OPENSEARCH_INDEX)
    client.indices.create.assert_not_called()


def test_search_scroll_documents_with_scrolling(mocker: MockerFixture):
    mocker.patch('config.Settings.OPENSEARCH_HOST', 'another_host')
    mocker.patch('config.Settings.OPENSEARCH_PORT', 6666)
    mocker.patch('config.Settings.OPENSEARCH_SECURE', True)
    mocker.patch('config.Settings.OPENSEARCH_USER', '')
    mocker.patch('config.Settings.OPENSEARCH_USER_PASSWORD', '')
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'my-index')

    client = OpenSearchClient()
    client.transport = mocker.Mock(Transport)
    client.transport.perform_request.side_effect = [
        # call to initial 'search'
        {
            '_scroll_id': 'SCROLL_ID_1',
            'hits': {
                'hits': [
                    {'_source': {'metadata': {'doc_id': 'DOC_ID_1'}}},
                    {'_source': {'metadata': {'doc_id': 'DOC_ID_2'}}}
                ]
            }
        },
        # 1st call to 'scroll'
        {
            '_scroll_id': 'SCROLL_ID_2',
            'hits': {
                'hits': [
                    {'_source': {'metadata': {'doc_id': 'DOC_ID_3'}}},
                ]
            }
        },
        # 2nd call to 'scroll'
        {
            '_scroll_id': 'SCROLL_ID_3',
            'hits': {
                'hits': [
                    {'_source': {'metadata': {'doc_id': 'DOC_ID_4'}}},
                ]
            }
        },
        # 3rd call to 'scroll'
        {
            '_scroll_id': 'SCROLL_ID_4',
            'hits': {
                'hits': []
            }
        },
    ]

    results = list(client.search_scroll_documents('RKM', size=5, scroll='5m'))

    assert len(results) == 4
    assert results[0]['doc_id'] == 'DOC_ID_1'
    assert results[1]['doc_id'] == 'DOC_ID_2'
    assert results[2]['doc_id'] == 'DOC_ID_3'
    assert results[3]['doc_id'] == 'DOC_ID_4'

    perform_request_calls = client.transport.perform_request.mock_calls
    assert len(perform_request_calls) == 4
    assert perform_request_calls[0].args[1] == '/my-index/_search'
    assert perform_request_calls[1].args[1] == '/_search/scroll'
    assert perform_request_calls[1].kwargs['body']['scroll_id'] == 'SCROLL_ID_1'
    assert perform_request_calls[2].args[1] == '/_search/scroll'
    assert perform_request_calls[2].kwargs['body']['scroll_id'] == 'SCROLL_ID_2'
    assert perform_request_calls[3].args[1] == '/_search/scroll'
    assert perform_request_calls[3].kwargs['body']['scroll_id'] == 'SCROLL_ID_3'


def test_search_scroll_documents_with_empty_result(mocker: MockerFixture):
    mocker.patch('config.Settings.OPENSEARCH_HOST', 'another_host')
    mocker.patch('config.Settings.OPENSEARCH_PORT', 6666)
    mocker.patch('config.Settings.OPENSEARCH_SECURE', True)
    mocker.patch('config.Settings.OPENSEARCH_USER', '')
    mocker.patch('config.Settings.OPENSEARCH_USER_PASSWORD', '')
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'my-index')

    client = OpenSearchClient()
    client.transport = mocker.Mock(Transport)
    client.transport.perform_request.side_effect = [
        # call to initial 'search'
        {
            '_scroll_id': 'SCROLL_ID',
            'hits': {
                'hits': []
            }
        },
    ]

    results = list(client.search_scroll_documents('RKM', size=5, scroll='5m'))

    assert len(results) == 0

    perform_request_calls = client.transport.perform_request.mock_calls
    assert len(perform_request_calls) == 1


def test_search_scroll_documents_with_search_by_key(mocker: MockerFixture):
    mocker.patch('config.Settings.OPENSEARCH_HOST', 'another_host')
    mocker.patch('config.Settings.OPENSEARCH_PORT', 6666)
    mocker.patch('config.Settings.OPENSEARCH_SECURE', True)
    mocker.patch('config.Settings.OPENSEARCH_USER', '')
    mocker.patch('config.Settings.OPENSEARCH_USER_PASSWORD', '')
    mocker.patch('config.Settings.OPENSEARCH_INDEX', 'my-index')

    client = OpenSearchClient()
    client.transport = mocker.Mock(Transport)
    client.transport.perform_request.return_value = {
        '_scroll_id': 'SCROLL_ID_1',
        'hits': {
            'hits': []
        }
    }

    results = list(
        client.search_scroll_documents(
            'RKM', search_by_key_field='doc_id', search_by_key_value='SOME_DOC_ID'))

    assert len(results) == 0
    client.transport.perform_request.assert_called_once()
    body = client.transport.perform_request.mock_calls[0].kwargs['body']
    assert any(
        term
        for term in body['query']['bool']['must']
        if 'doc_id' in term['term'] and term['term']['doc_id']['value'] == 'SOME_DOC_ID')


def test_ensure_mappings_called_when_index_already_exists(mocker: MockerFixture):
    Settings.OPENSEARCH_HOST = 'anotherhost'
    Settings.OPENSEARCH_PORT = 6666
    Settings.OPENSEARCH_SECURE = True
    Settings.OPENSEARCH_USER = ''
    Settings.OPENSEARCH_USER_PASSWORD = ''
    Settings.OPENSEARCH_INDEX = 'my-index'
    index = {'name': Settings.OPENSEARCH_INDEX}

    client = OpenSearchClient()
    client.indices = mocker.Mock(IndicesClient)
    client.indices.get.return_value = index

    client.ensure_application_index_created()
    client.indices.get.assert_called_once_with(Settings.OPENSEARCH_INDEX)
    client.indices.put_mapping.assert_called_once()
    assert client.indices.put_mapping.mock_calls[0].kwargs['index'] == Settings.OPENSEARCH_INDEX
    assert isinstance(client.indices.put_mapping.mock_calls[0].kwargs['body'], dict)
