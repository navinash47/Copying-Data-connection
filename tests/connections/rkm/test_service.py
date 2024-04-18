from datetime import datetime
import pytest
from urllib.parse import quote

import responses
from pytest_mock import MockerFixture
from responses import matchers

from config import Settings
from connections.rkm.models import RkmConnection
from connections.rkm.service import Rkm
from tests.utils import responses_utils


@pytest.fixture
def connection():
    return RkmConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


@responses.activate
def test_get_reference_with_jwt_token(mocker: MockerFixture, connection: RkmConnection):
    mocker.patch('config.Settings.RKM_URL', 'http://rkm.example.com')
    responses.post('http://rkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')

    reference_guid = 'TEST_GUID'
    reference_text = 'TEST_REFERENCE_TEXT'
    responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:ReferenceTemplate")}',
        match=[
            matchers.header_matcher({'impersonated-user-id': 'IMPERSONATED-USER'}),
            matchers.query_param_matcher({'q': f"('179' = \"{reference_guid}\")"}, strict_match=False)],
        status=200,
        json={'entries': [{'values': {'Reference': reference_text}}]})

    rkm = Rkm(connection)

    reference = rkm.get_reference(reference_guid)

    assert reference['Reference'] == reference_text


@responses.activate
def test_get_reference_without_connection(mocker: MockerFixture):
    mocker.patch('config.Settings.RKM_URL', 'http://rkm.example.com')
    responses.post('http://rkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')

    reference_guid = 'TEST_GUID'
    reference_text = 'TEST_REFERENCE_TEXT'
    responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:ReferenceTemplate")}',
        match=[
            responses_utils.no_header_matcher('impersonated-user-id'),
            matchers.query_param_matcher({'q': f"('179' = \"{reference_guid}\")"}, strict_match=False)],
        status=200,
        json={'entries': [{'values': {'Reference': reference_text}}]})

    rkm = Rkm(None)

    reference = rkm.get_reference(reference_guid)

    assert reference['Reference'] == reference_text


@responses.activate
def test_get_reference_without_jwt_token(mocker: MockerFixture, connection: RkmConnection):
    mocker.patch('config.Settings.RKM_URL', 'http://rkm.example.com')

    reference_guid = 'TEST_GUID'
    reference_text = 'TEST_REFERENCE_TEXT'
    jwt_token = 'TEST_JWT_TOKEN'
    responses.post('http://rkm.example.com/api/jwt/login', body=jwt_token)
    responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:ReferenceTemplate")}',
        match=[
            matchers.query_param_matcher({'q': f"('179' = \"{reference_guid}\")"}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': {'Reference': reference_text}}]})

    rkm = Rkm(connection)

    reference = rkm.get_reference('TEST_GUID')

    assert reference['Reference'] == reference_text


@responses.activate
def test_list_published_knowledge_articles(connection: RkmConnection):
    Settings.RKM_URL = 'http://rkm.example.com'

    jwt_token = 'TEST_JWT_TOKEN'
    jwt_token_call = responses.post('http://rkm.example.com/api/jwt/login', body=jwt_token)

    # 1st request: return a couple of articles.
    article_entry_1 = {'Request ID': 'REQUEST_ID_1'}
    article_entry_2 = {'Request ID': 'REQUEST_ID_2'}
    first_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 0}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': article_entry_1}, {'values': article_entry_2}]})

    # 2nd request: return an empty list of articles.
    second_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 2}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': []})

    rkm = Rkm(connection)
    result = list(rkm.list_published_knowledge_articles())

    assert result == [article_entry_1, article_entry_2]
    assert jwt_token_call.call_count == 1
    assert first_list_call.call_count == 1
    assert second_list_call.call_count == 1


@responses.activate
def test_list_published_knowledge_articles_with_modified_since(connection: RkmConnection):
    modified_since = "2023-11-20T10:00:00.123Z"
    date_format = '%Y-%m-%dT%H:%M:%S.%f%z'
    date_obj = datetime.strptime(modified_since, date_format)
    Settings.RKM_URL = 'http://rkm.example.com'

    jwt_token = 'TEST_JWT_TOKEN'
    jwt_token_call = responses.post('http://rkm.example.com/api/jwt/login', body=jwt_token)

    # 1st request: return a couple of articles.
    article_entry_1 = {'Request ID': 'REQUEST_ID_1'}
    article_entry_2 = {'Request ID': 'REQUEST_ID_2'}
    first_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700 AND '302300535' >= \"1700474400\"", 'offset': 0},
                                         strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': article_entry_1}, {'values': article_entry_2}]})

    # 2nd request: return an empty list of articles.
    second_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': f"'302312185' = 700 AND '302300535' >= \"1700474400\"", 'offset': 2},
                                         strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': []})

    rkm = Rkm(connection)
    result = list(rkm.list_published_knowledge_articles(modified_since=date_obj))

    assert result == [article_entry_1, article_entry_2]
    assert jwt_token_call.call_count == 1
    assert first_list_call.call_count == 1
    assert second_list_call.call_count == 1


@responses.activate
def test_list_published_knowledge_articles_with_3_listings(connection: RkmConnection):
    Settings.RKM_URL = 'http://rkm.example.com'
    jwt_token = 'TEST_JWT_TOKEN'
    responses.post('http://rkm.example.com/api/jwt/login', status=200, body=jwt_token)

    # 1st request: return a couple of articles.
    article_entry_1 = {'Request ID': 'REQUEST_ID_1'}
    article_entry_2 = {'Request ID': 'REQUEST_ID_2'}
    first_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 0}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': article_entry_1}, {'values': article_entry_2}]})

    # 2nd request: return a couple more.
    article_entry_3 = {'Request ID': 'REQUEST_ID_3'}
    article_entry_4 = {'Request ID': 'REQUEST_ID_4'}
    second_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 2}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': article_entry_3}, {'values': article_entry_4}]})

    # 3rd request: return an empty list of articles.
    third_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 4}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': []})

    rkm = Rkm(connection)
    result = list(rkm.list_published_knowledge_articles())

    assert result == [article_entry_1, article_entry_2, article_entry_3, article_entry_4]
    assert first_list_call.call_count == 1
    assert second_list_call.call_count == 1
    assert third_list_call.call_count == 1


@responses.activate
def test_list_published_knowledge_article_display_ids(mocker: MockerFixture, connection: RkmConnection):
    mocker.patch('config.Settings.RKM_URL', 'http://rkm.example.com')

    jwt_token = 'TEST_JWT_TOKEN'
    jwt_token_call = responses.post('http://rkm.example.com/api/jwt/login', body=jwt_token)

    # 1st request: return a couple of articles.
    article_entry_1 = {'Request ID': 'REQUEST_ID_1', 'DocID': 'KA_ID_1'}
    article_entry_2 = {'Request ID': 'REQUEST_ID_2', 'DocID': 'KA_ID_2'}
    first_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 0}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': article_entry_1}, {'values': article_entry_2}]})

    # 2nd request: return an empty list of articles.
    second_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher({'q': "'302312185' = 700", 'offset': 2}, strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': []})

    rkm = Rkm(connection)
    result = rkm.list_published_knowledge_article_display_ids()
    result_as_set = set()
    result_as_set.update(result)

    assert result_as_set == {article_entry_1['DocID'], article_entry_2['DocID']}
    assert jwt_token_call.call_count == 1
    assert first_list_call.call_count == 1
    assert second_list_call.call_count == 1


@responses.activate
def test_list_published_knowledge_article_display_ids_with_display_id_filter(
        mocker: MockerFixture, connection: RkmConnection):
    mocker.patch('config.Settings.RKM_URL', 'http://rkm.example.com')

    jwt_token = 'TEST_JWT_TOKEN'
    jwt_token_call = responses.post('http://rkm.example.com/api/jwt/login', body=jwt_token)

    # 1st request: return a couple of articles.
    article_entry_1 = {'Request ID': 'REQUEST_ID_1', 'DocID': 'KA_ID_1'}
    article_entry_2 = {'Request ID': 'REQUEST_ID_2', 'DocID': 'KA_ID_2'}
    first_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher(
                {
                    'q': "'302312185' = 700 AND (('302300507' = \"KA_ID_1\" OR '302300507' = \"KA_ID_2\"))",
                    'offset': 0
                },
                strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': [{'values': article_entry_1}, {'values': article_entry_2}]})

    # 2nd request: return an empty list of articles.
    second_list_call = responses.get(
        f'http://rkm.example.com/api/arsys/v1/entry/{quote("RKM:KnowledgeArticleManager")}',
        match=[
            matchers.query_param_matcher(
                {
                    'q': "'302312185' = 700 AND (('302300507' = \"KA_ID_1\" OR '302300507' = \"KA_ID_2\"))",
                    'offset': 2
                },
                strict_match=False),
            matchers.header_matcher({'Authorization': f"AR-JWT {jwt_token}"})
        ],
        status=200,
        json={'entries': []})

    rkm = Rkm(connection)
    result = rkm.list_published_knowledge_article_display_ids(display_ids=['KA_ID_1', 'KA_ID_2'])
    result_as_set = set()
    result_as_set.update(result)

    assert result_as_set == {article_entry_1['DocID'], article_entry_2['DocID']}
    assert jwt_token_call.call_count == 1
    assert first_list_call.call_count == 1
    assert second_list_call.call_count == 1
