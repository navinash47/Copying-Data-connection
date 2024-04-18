import os
from datetime import datetime

import pytest
import responses
from pytest_mock import MockerFixture
from responses import matchers

from connections.bwf.models import BwfConnection
from connections.bwf.service import Bwf
from utils.io_utils import read_json_dict


@pytest.fixture
def bwf_settings(mocker: MockerFixture):
    mocker.patch('config.Settings.BWF_URL', 'http://example.com')
    yield


@pytest.fixture
def connection():
    return BwfConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


@responses.activate
def test_get_article_ids(bwf_settings, connection: BwfConnection):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[
            matchers.header_matcher({'impersonated-user-id': 'IMPERSONATED-USER'}),
            matchers.query_param_matcher(
                {'propertySelection': '379', 'queryExpression': "'302300500' = \"5000\"", 'startIndex': 0},
                strict_match=False)],
        status=200,
        json={'totalSize': None, 'data': [{'379': 'ID1'}, {'379': 'ID2'}, {'379': 'ID3'}]}
    )
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[
            matchers.header_matcher({'impersonated-user-id': 'IMPERSONATED-USER'}),
            matchers.query_param_matcher(
                {'propertySelection': '379', 'queryExpression': "'302300500' = \"5000\"", 'startIndex': 3},
                strict_match=False)],
        status=200,
        json={'totalSize': None, 'data': []}
    )

    client = Bwf(connection)
    article_ids = client.get_article_ids()
    assert len(article_ids) == 3
    for i in range(0, 3):
        assert article_ids[i] == f'ID{i + 1}'


@responses.activate
def test_get_article_ids_with_modified_since(bwf_settings, connection: BwfConnection):
    modified_since = "2023-11-20T10:00:00.123Z"
    date_format = '%Y-%m-%dT%H:%M:%S.%f%z'
    date_obj = datetime.strptime(modified_since, date_format)
    print(date_obj.timestamp())
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {'propertySelection': '379', 'queryExpression': "'302300500' = \"5000\" AND '6' >= \"1700474400\"",
             'startIndex': 0},
            strict_match=False)],
        status=200,
        json={'totalSize': None, 'data': [{'379': 'ID1'}, {'379': 'ID2'}, {'379': 'ID3'}]}
    )
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {'propertySelection': '379', 'queryExpression': "'302300500' = \"5000\" AND '6' >= \"1700474400\"",
             'startIndex': 3},
            strict_match=False)],
        status=200,
        json={'totalSize': None, 'data': []}
    )

    client = Bwf(connection)
    article_ids = client.get_article_ids(modified_since=date_obj)
    assert len(article_ids) == 3
    for i in range(0, 3):
        assert article_ids[i] == f'ID{i + 1}'


@responses.activate
def test_get_article_ids_with_display_id(bwf_settings, connection: BwfConnection):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {
                'propertySelection': '379',
                'queryExpression': "'302300500' = \"5000\" AND '302300507' = \"DISPLAY_ID\"",
                'startIndex': 0
            },
            strict_match=False)],
        status=200,
        json={'totalSize': None, 'data': [{'379': 'ID'}]}
    )

    client = Bwf(connection)
    article_ids = client.get_article_ids(display_id='DISPLAY_ID')
    assert article_ids == ['ID']


@responses.activate
def test_get_article(bwf_settings, connection: BwfConnection):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/com.bmc.dsm.knowledge/rx/application/knowledge/AGGADG1AAP0ICAOQVYJ6OPZVOTL7BU',
        status=200,
        json=read_json_dict(os.path.dirname(__file__), 'com.bmc.dsm.knowledge_rx_application_knowledge.json')
    )

    client = Bwf(connection)
    article = client.get_article('AGGADG1AAP0ICAOQVYJ6OPZVOTL7BU')

    assert article.uuid == 'AGGADG1AAP0ICAOQVYJ6OPZVOTL7BU'
    assert article.content_id == 'KA-000000000005'
    assert len(article.contents) == 1
    assert article.contents[0].label == 'Reference'
    assert article.contents[0].content == 'This is the content.'
    assert article.locale == 'en'
    assert article.template_name == 'HowTo'
    assert article.external
    assert article.title == 'EMEA Car Allowance Policy'


@responses.activate
def test_get_article_without_connection(bwf_settings):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/com.bmc.dsm.knowledge/rx/application/knowledge/AGGADG1AAP0ICAOQVYJ6OPZVOTL7BU',
        status=200,
        json=read_json_dict(os.path.dirname(__file__), 'com.bmc.dsm.knowledge_rx_application_knowledge.json')
    )

    client = Bwf(None)
    article = client.get_article('AGGADG1AAP0ICAOQVYJ6OPZVOTL7BU')

    assert article.uuid == 'AGGADG1AAP0ICAOQVYJ6OPZVOTL7BU'
    assert article.content_id == 'KA-000000000005'
    assert len(article.contents) == 1
    assert article.contents[0].label == 'Reference'
    assert article.contents[0].content == 'This is the content.'
    assert article.locale == 'en'
    assert article.template_name == 'HowTo'
    assert article.external
    assert article.title == 'EMEA Car Allowance Policy'


# The implementation code of `get_article_display_ids()` is mostly common with  `get_article_ids()`.
# We therefore have reduced functional coverage below, focusing on things that are different.
@responses.activate
def test_get_article_display_ids(bwf_settings, connection: BwfConnection):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {'propertySelection': '302300507', 'queryExpression': "'302300500' = \"5000\"", 'startIndex': 0},
            strict_match=False)],
        status=200,
        json={
            'totalSize': None,
            'data': [{'302300507': 'DISPLAY_ID1'}, {'302300507': 'DISPLAY_ID2'}, {'302300507': 'DISPLAY_ID3'}]
        }
    )
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {'propertySelection': '302300507', 'queryExpression': "'302300500' = \"5000\"", 'startIndex': 3},
            strict_match=False)],
        status=200,
        json={'totalSize': None, 'data': []}
    )

    client = Bwf(connection)
    article_ids = client.get_article_display_ids()
    assert len(article_ids) == 3
    for i in range(0, 3):
        assert article_ids[i] == f'DISPLAY_ID{i + 1}'


@responses.activate
def test_get_article_display_ids_with_display_id_filter(bwf_settings, connection):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {
                'propertySelection': '302300507',
                'queryExpression': "'302300500' = \"5000\" AND '302300507' = \"DISPLAY_ID\"",
                'startIndex': 0
            },
            strict_match=False)],
        status=200,
        json={
            'totalSize': None,
            'data': [{'302300507': 'DISPLAY_ID'}]
        }
    )

    client = Bwf(connection)
    article_ids = client.get_article_display_ids(display_id='DISPLAY_ID')
    assert len(article_ids) == 1
    assert article_ids[0] == f'DISPLAY_ID'


@pytest.mark.parametrize('returned_companies,expected_company', [
    ([], None),
    (['Petramco'], 'Petramco'),
    (['Petramco', 'Calbro Services'], 'Petramco')  # only testing consistency in picking the *first* company received
])
@responses.activate
def test_get_article(bwf_settings, connection: BwfConnection, returned_companies, expected_company):
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher(
            {
                'propertySelection': '1000000001',
                'queryExpression': "'302300500' = \"5000\" AND '379' = \"ARTICLE_ID\"",
                'startIndex': 0
            },
            strict_match=False)],
        status=200,
        json={
            'totalSize': None,
            'data': [{'1000000001': company} for company in returned_companies]
        }
    )

    client = Bwf(connection)
    company = client.get_article_company('ARTICLE_ID')
    assert company == expected_company
