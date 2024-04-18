import os

import pytest
import requests
import responses
from pytest_mock import MockerFixture
from responses import matchers

from connections.hkm.constants import PAGE_SIZE
from connections.hkm.models import HkmConnection
from connections.hkm.service import Hkm, HkmConnectionLoader
from helixplatform.models import Record
from utils.io_utils import read_json_dict

from tests.utils import responses_utils


@pytest.fixture
def connection():
    return HkmConnection(id='CONNECTION_ID', user='IMPERSONATED-USER')


def test_hkm_connection_loader_record_definition():
    assert HkmConnectionLoader.get_record_definition_name() == 'com.bmc.dsom.hgm:Connection_HKM'


@pytest.mark.parametrize('user', ['USER', None])
def test_hkm_connection_loader_from_record(user: str | None):
    record = Record(recordDefinitionName='com.bmc.dsom.hgm:Connection_HKM')
    record[379] = 'ID'
    record[490000260] = user

    connection = HkmConnectionLoader.from_record(record)

    assert connection.id == 'ID'
    assert connection.user == user


@responses.activate
def test_get_list_of_content_ids(mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')
    responses.post('http://hkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/search?knowledgeStates=Published" \
          f"&pageSize=100&enablePagination=true&pageNumber=1"
    responses.get(url, status=200, json={"totalPages": 1, "result": [{"contentId": 12345}]})
    hkm = Hkm(connection)
    results = hkm._get_list_of_content_ids(1, PAGE_SIZE)
    assert results.pages == 1
    assert len(results.content_ids) == 1


@responses.activate
def test_get_article(mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')

    responses.post(
        'http://hkm.example.com/api/jwt/login',
        match=[responses_utils.no_header_matcher('impersonated-user-id')],
        status=200,
        body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/article/1013352"
    responses.get(
        url,
        match=[matchers.header_matcher({'impersonated-user-id': 'IMPERSONATED-USER'})],
        status=200,
        json=read_json_dict(os.path.dirname(__file__), 'hkm_article.json'))

    hkm = Hkm(connection)
    result = hkm.get_article(1013352)

    assert result.content_id == 1013352
    assert len(result.translations) == 7
    assert result.translations[0].culture == "en_US"
    assert result.translations[0].title == "Take a snapshot"
    assert result.translations[0].issue is None
    assert result.translations[0].environment is None
    assert result.translations[0].resolution == "resolution en_US"
    assert result.translations[0].cause is None
    assert result.translations[0].tags == ["adobe-acrobat"]
    assert result.translations[1].culture == "zh_CN"
    assert result.translations[1].title == "拍一张快照"
    assert result.translations[1].issue == "issue zh_CN"
    assert result.translations[1].environment == "environment zh_CN"
    assert result.translations[1].resolution == "resolution zh_CN"
    assert result.translations[1].cause == "cause zh_CN"
    assert result.translations[1].tags == ["adobe-acrobat"]


@responses.activate
def test_get_article_ids(mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')
    responses.post('http://hkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/search?knowledgeStates=Published" \
          f"&pageSize=100&enablePagination=true&pageNumber=1"
    responses.get(
        url,
        status=200,
        json={"totalPages": 1, "result": [{"contentId": 123}, {"contentId": 456}, {"contentId": 123}]})
    hkm = Hkm(connection)
    result = hkm.get_article_ids()

    assert result == {123, 456}


@responses.activate
def test_get_article_ids_with_content_id_filter(mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')
    responses.post('http://hkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/article/1013352"
    responses.get(
        url,
        status=200,
        json=read_json_dict(os.path.dirname(__file__), 'hkm_article.json'))
    hkm = Hkm(connection)
    result = hkm.get_article_ids(1013352)

    assert result == {1013352}


@responses.activate
def test_get_article_ids_with_content_id_filter_but_unpublished(mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')
    responses.post('http://hkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/article/1013352"

    article_json_dict = read_json_dict(os.path.dirname(__file__), 'hkm_article.json')
    for translation in article_json_dict['translations']:
        translation['knowledgeState'] = 'draft'  # the article isn't published anymore

    responses.get(url, status=200, json=article_json_dict)
    hkm = Hkm(connection)
    result = hkm.get_article_ids(1013352)

    assert result == set()


@responses.activate
def test_get_article_ids_with_content_id_filter_but_gone(mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')
    responses.post('http://hkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/article/1013352"

    article_json_dict = read_json_dict(os.path.dirname(__file__), 'hkm_article.json')
    for translation in article_json_dict['translations']:
        translation['knowledgeState'] = 'draft'  # the article isn't published anymore

    responses.get(url, status=500, json=[
        {
            "messageType": "ERROR",
            "messageNumber": 234010,
            "messageText": "Failed to get the knowledge article : 1013352",
            "appendedText": "[ERROR (234010): Failed to get the knowledge article : 1013352;"
                            " org.springframework.web.client.HttpClientErrorException$NotFound:"
                            " 404 Not Found:"
                            " \"{<CR><LF>  \"type\": \"https://tools.ietf.org/html/rfc7231#section-6.5.4\",<CR><LF>"
                            "  \"title\": \"Not Found\",<CR><LF>  \"status\": 404,<CR><LF>"
                            "  \"traceId\": \"00-ee8ba970a1b1ef9ce3ab2c1df54f9659-fd9d8f3035c1b672-00\"<CR><LF>}\""
                            "]"
        }
    ])
    hkm = Hkm(connection)
    result = hkm.get_article_ids(1013352)

    assert result == set()


@responses.activate
def test_get_article_ids_with_content_id_filter_but_comaround_errors_out(
        mocker: MockerFixture, connection: HkmConnection):
    url_base = 'http://hkm.example.com'
    mocker.patch('config.Settings.HKM_URL', url_base)
    mocker.patch('config.Settings.HKM_USER', 'user')
    mocker.patch('config.Settings.HKM_PASSWORD', 'password')
    responses.post('http://hkm.example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    url = f"{url_base}/api/rx/application/knowledge/article/1013352"

    article_json_dict = read_json_dict(os.path.dirname(__file__), 'hkm_article.json')
    for translation in article_json_dict['translations']:
        translation['knowledgeState'] = 'draft'  # the article isn't published anymore

    responses.get(url, status=500, json=[
        {
            "messageType": "ERROR",
            "messageNumber": 234010,
            "messageText": "Failed to get the knowledge article : 1013352",
            "appendedText": "[ERROR (234010): Failed to get the knowledge article : 1013352;"
                            " org.springframework.web.client.HttpClientErrorException$NotFound:"
                            " 503 Service Unavailable:"
                            " \"{<CR><LF>  \"type\": \"https://tools.ietf.org/html/rfc7231#section-6.5.4\",<CR><LF>"
                            "  \"title\": \"Not Found\",<CR><LF>  \"status\": 404,<CR><LF>"
                            "  \"traceId\": \"00-ee8ba970a1b1ef9ce3ab2c1df54f9659-fd9d8f3035c1b672-00\"<CR><LF>}\""
                            "]"
        }
    ])
    hkm = Hkm(connection)
    with pytest.raises(requests.exceptions.HTTPError):
        hkm.get_article_ids(1013352)
