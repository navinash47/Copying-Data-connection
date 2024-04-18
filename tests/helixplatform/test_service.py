import datetime
import json
import os
from typing import Callable, Tuple

import pytest
import responses
from requests import PreparedRequest, HTTPError
from responses import matchers
from pytest_mock import MockerFixture
from urllib3._collections import HTTPHeaderDict

from config import Settings
from health.models import HealthStatus
from helixplatform.models import Record, Attachment
from helixplatform.service import HelixPlatformHealthIndicator, InnovationSuite
from utils.io_utils import read_json_dict

from tests.utils import responses_utils


@responses.activate
def test_get_health_server_up(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://helix.example.com')

    jwt_token = 'TEST_JWT_TOKEN'
    responses.post('http://helix.example.com/api/jwt/login', body=jwt_token)
    responses.post('http://helix.example.com/api/jwt/logout')

    indicator = HelixPlatformHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.UP


@responses.activate
def test_get_health_bad_password(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://helix.example.com')
    responses.post('http://helix.example.com/api/jwt/login', status=401)

    indicator = HelixPlatformHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.DOWN


@responses.activate
def test_get_health_server_error(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://helix.example.com')

    responses.post('http://helix.example.com/api/jwt/login', status=500)

    indicator = HelixPlatformHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.DOWN


@responses.activate
def test_get_health_missing_configuration(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', '')

    indicator = HelixPlatformHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.DOWN


@responses.activate
def test_get_health_bad_configuration(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'not/really/anything/logical')

    indicator = HelixPlatformHealthIndicator()

    health = indicator.get_health()

    assert health.status is HealthStatus.DOWN


@responses.activate
def test_get_record_with_error(mocker: MockerFixture):  # checks error reporting c.f., ArErrorFilter
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post(
        'http://example.com/api/jwt/login',
        match=[responses_utils.no_header_matcher('impersonated-user-id')],
        status=200,
        body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/record'
        '/recordinstance/com.bmc.dsom.hgm%3ADataConnectionJob/AGGADGG8ECDC2ASBADLGSBADLG919H',
        match=[
            matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN'}),
            responses_utils.no_header_matcher('impersonated-user-id')],
        status=500,
        body='[{"messageType": "ERROR", "messageNumber": 999, "messageText": "something went wrong"}]'
    )

    client = InnovationSuite()
    with pytest.raises(HTTPError) as http_error:
        client.get_record(record_definition='com.bmc.dsom.hgm:DataConnectionJob',
                          record_id='AGGADGG8ECDC2ASBADLGSBADLG919H')

    # would have expected '500 Internal Server Error` but not in our control
    assert '500 Server Error' in str(http_error)
    assert 'ERROR (999) something went wrong' in str(http_error)


@responses.activate
def test_get_record(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post(
        'http://example.com/api/jwt/login',
        match=[responses_utils.no_header_matcher('impersonated-user-id')],
        status=200,
        body='TEST_JWT_TOKEN')
    response_json = read_json_dict(os.path.dirname(__file__), 'test_get_record_response.json')
    responses.get(
        'http://example.com/api/rx/application/record'
        '/recordinstance/com.bmc.dsom.hgm%3ADataConnectionJob/AGGADGG8ECDC2ASBADLGSBADLG919H',
        match=[
            matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN'}),
            responses_utils.no_header_matcher('impersonated-user-id')],
        status=200,
        json=response_json
    )

    client = InnovationSuite()
    record = client.get_record(record_definition='com.bmc.dsom.hgm:DataConnectionJob',
                               record_id='AGGADGG8ECDC2ASBADLGSBADLG919H')

    assert record.resourceType == 'com.bmc.arsys.rx.services.record.domain.RecordInstance'
    assert record.id == 'AGGADGG8ECDC2ASBADLGSBADLG919H'
    assert record.displayId == '000000000000001'
    assert record.recordDefinitionName == 'com.bmc.dsom.hgm:DataConnectionJob'
    assert record.fieldInstances['1'].value == '000000000000001'
    assert record.fieldInstances['490000150'].value == 'HKM'


@responses.activate
def test_get_record_with_impersonate(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post(
        'http://example.com/api/jwt/login',
        match=[responses_utils.no_header_matcher('impersonated-user-id')],
        status=200,
        body='TEST_JWT_TOKEN')
    response_json = read_json_dict(os.path.dirname(__file__), 'test_get_record_response.json')
    responses.get(
        'http://example.com/api/rx/application/record'
        '/recordinstance/com.bmc.dsom.hgm%3ADataConnectionJob/AGGADGG8ECDC2ASBADLGSBADLG919H',
        match=[
            matchers.header_matcher(
                {'Authorization': 'AR-JWT TEST_JWT_TOKEN', 'impersonated-user-id': 'IMPERSONATED-USER'})
        ],
        status=200,
        json=response_json
    )

    client = InnovationSuite(impersonated_user='IMPERSONATED-USER')
    record = client.get_record(record_definition='com.bmc.dsom.hgm:DataConnectionJob',
                               record_id='AGGADGG8ECDC2ASBADLGSBADLG919H')

    assert record is not None  # no need to run more assertions: the focus is on the impersonation header


@responses.activate
def test_get_attachment(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    attachment_content = b'ATTACHMENT_CONTENT'
    responses.get(
        'http://example.com/api/rx/application/record/attachment'
        '/com.bmc.dsom.hgm%3ADataConnectionJob/AGGADGG8ECDC2ASBADLGSBADLG919H/123',
        match=[matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN'})],
        status=200,
        content_type='application/pdf',
        headers={
            'Content-Disposition': "attachment; filename*=UTF-8''my_document.pdf"
        },
        body=attachment_content
    )

    client = InnovationSuite()
    attachment = client.get_attachment(record_definition='com.bmc.dsom.hgm:DataConnectionJob',
                                       record_id='AGGADGG8ECDC2ASBADLGSBADLG919H',
                                       field_id=123)

    assert attachment.content == attachment_content
    assert attachment.filename == 'my_document.pdf'
    assert attachment.content_type == 'application/pdf'


@responses.activate
def test_create_record(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.post(
        'http://example.com/api/rx/application/record/recordinstance',
        match=[
            matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN'}),
            matchers.json_params_matcher({
                'recordDefinitionName': 'com.bmc.dsom.hgm:DataConnectionJob',
                'fieldInstances': {
                    '123': {'id': 123, 'value': '123_value'},
                    '456': {'id': 456, 'value': '456_value'}
                }})
        ],
        status=201,
        headers={'Location': 'http://example.com/api/rx/application/record/recordinstance'
                             '/com.bmc.dsom.hgm:DataConnectionJob/TEST_RECORD_ID'},
        body=''
    )

    record = Record(recordDefinitionName='com.bmc.dsom.hgm:DataConnectionJob')
    record[123] = '123_value'
    record[456] = '456_value'

    client = InnovationSuite()
    record_id = client.create_record(record)

    assert record_id == 'TEST_RECORD_ID'


@responses.activate
def test_create_record_with_attachment(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    attachment = Attachment(filename='my_document.pdf', content=b'PDF_CONTENT', content_type='application/pdf')
    responses.post(
        'http://example.com/api/rx/application/record/recordinstance',
        match=[
            matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN'}),
            matchers.multipart_matcher({
                '456': ['my_document.pdf', b'PDF_CONTENT', 'application/pdf'],
                'recordInstance': json.dumps({
                    'recordDefinitionName': 'com.bmc.dsom.hgm:DataConnectionJob',
                    'fieldInstances': {
                        '123': {'id': 123, 'value': '123_value'},
                    }
                })
            })
        ],
        status=201,
        headers={'Location': 'http://example.com/api/rx/application/record/recordinstance'
                             '/com.bmc.dsom.hgm:DataConnectionJob/TEST_RECORD_ID'},
        body=''
    )

    record = Record(recordDefinitionName='com.bmc.dsom.hgm:DataConnectionJob')
    record[123] = '123_value'
    record[456] = attachment

    client = InnovationSuite()
    record_id = client.create_record(record)

    assert record_id == 'TEST_RECORD_ID'


@responses.activate
def test_update_record(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.put(
        'http://example.com/api/rx/application/record/recordinstance/'
        'com.bmc.dsom.hgm%3ADataConnectionJob/TEST_RECORD_ID',
        match=[
            matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN'}),
            matchers.json_params_matcher({
                'recordDefinitionName': 'com.bmc.dsom.hgm:DataConnectionJob',
                'id': 'TEST_RECORD_ID',
                'fieldInstances': {
                    '123': {'id': 123, 'value': '123_value'},
                    '456': {'id': 456, 'value': '456_value'}
                }})
        ],
        status=204,
        body=''
    )

    record = Record(recordDefinitionName='com.bmc.dsom.hgm:DataConnectionJob', id='TEST_RECORD_ID')
    record[123] = '123_value'
    record[456] = '456_value'

    client = InnovationSuite()
    client.update_record(record)


@responses.activate
def test_ar_auth_filer_on_401(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN_1')
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN_2')
    responses.get(
        'http://example.com/api/rx/application/user/$USER$',
        status=200,
        json={'user': 0}
    )
    responses.get(
        'http://example.com/api/rx/application/user/$USER$',
        match=[matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN_1'})],
        status=401,
        json={'user': 1}
    )
    responses.get(
        'http://example.com/api/rx/application/user/$USER$',
        match=[matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN_2'})],
        status=200,
        headers={
            'Session-Expiration': 'Fri, 06 Oct 2099 01:42:22 GMT',
            'Absolute-Session-Expiration': 'Fri, 06 Oct 2099 02:42:22 GMT'
        },
        json={'user': 2}
    )

    client = InnovationSuite()
    client.get_current_user()  # should initialize the JWT token
    assert client.ar_auth_filter.jwt_token.jwt_token == 'TEST_JWT_TOKEN_1'

    user = client.get_current_user()
    assert user == {'user': 2}
    jwt_token = client.ar_auth_filter.jwt_token
    assert jwt_token.jwt_token == 'TEST_JWT_TOKEN_2'
    assert jwt_token.expiry == datetime.datetime(year=2099, month=10, day=6, hour=1, minute=42, second=22)


@responses.activate
def test_ar_auth_filer_on_token_expiry(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN_1')
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN_2')
    responses.get(
        'http://example.com/api/rx/application/user/$USER$',
        status=200,
        headers={
            'Session-Expiration': 'Fri, 06 Oct 2000 01:42:22 GMT',  # expiry in the past
            'Absolute-Session-Expiration': 'Fri, 06 Oct 2000 02:42:22 GMT'  # expiry in the past
        },
        json={'user': 0}
    )
    responses.get(
        'http://example.com/api/rx/application/user/$USER$',
        match=[matchers.header_matcher({'Authorization': 'AR-JWT TEST_JWT_TOKEN_2'})],
        status=200,
        json={'user': 1}
    )

    client = InnovationSuite()
    client.get_current_user()  # should initialize the JWT token
    assert client.ar_auth_filter.jwt_token.jwt_token == 'TEST_JWT_TOKEN_1'

    user = client.get_current_user()
    assert user == {'user': 1}
    jwt_token = client.ar_auth_filter.jwt_token
    assert jwt_token.jwt_token == 'TEST_JWT_TOKEN_2'


@responses.activate
def test_get_all_records(mocker: MockerFixture):
    Settings.INNOVATION_SUITE_URL = 'http://example.com'

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher({
            'dataPageType': 'com.bmc.arsys.rx.application.record.datapage.RecordInstanceDataPageQuery',
            'recorddefinition': 'my:record',
            'propertySelection': '1,379',
            'sortBy': '6,1',
            'startIndex': 0,
            'pageSize': 10,
            'queryExpression': 'QUERY_EXPRESSION',
            'shouldIncludeTotalSize': 'false'
        })],
        status=200,
        json={
            'totalSize': None,
            'data': [{'1': 'REC1', '379': 'ID1'}, {'1': 'REC2', '379': 'ID2'}, {'1': 'REC3', '379': 'ID3'}]
        }
    )
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher({
            'dataPageType': 'com.bmc.arsys.rx.application.record.datapage.RecordInstanceDataPageQuery',
            'recorddefinition': 'my:record',
            'propertySelection': '1,379',
            'sortBy': '6,1',
            'startIndex': 3,  # only difference with previous call
            'pageSize': 10,
            'queryExpression': 'QUERY_EXPRESSION',
            'shouldIncludeTotalSize': 'false'
        })],
        status=200,
        json={
            'totalSize': None,
            'data': []
        }
    )

    client = InnovationSuite()
    results = client.get_all_records(record_definition='my:record',
                                     property_selection=[1, 379],
                                     sort_by=[6, 1],
                                     page_size=10,
                                     query_expression='QUERY_EXPRESSION')
    assert len(results) == 3
    for i in range(0, 3):
        assert results[i]['1'] == f'REC{i + 1}'
        assert results[i]['379'] == f'ID{i + 1}'


@responses.activate
def test_get_all_records_no_results(mocker: MockerFixture):
    Settings.INNOVATION_SUITE_URL = 'http://example.com'

    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN')
    responses.get(
        'http://example.com/api/rx/application/datapage',
        match=[matchers.query_param_matcher({
            'dataPageType': 'com.bmc.arsys.rx.application.record.datapage.RecordInstanceDataPageQuery',
            'recorddefinition': 'my:record',
            'propertySelection': '1,379',
            'sortBy': '6,1',
            'startIndex': 0,
            'pageSize': 10,
            'queryExpression': 'QUERY_EXPRESSION',
            'shouldIncludeTotalSize': 'false'
        })],
        status=200,
        json={
            'totalSize': None,
            'data': []
        }
    )

    client = InnovationSuite()
    results = client.get_all_records(record_definition='my:record',
                                     property_selection=[1, 379],
                                     sort_by=[6, 1],
                                     page_size=10,
                                     query_expression='QUERY_EXPRESSION')
    assert len(results) == 0


@responses.activate
def test_properly_encode_login_request(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    responses.post('http://example.com/api/jwt/login',
                   match=[
                       # can only match that we add the params as being set for url encoding
                       # https://github.com/getsentry/responses/blob/master/README.rst#url-encoded-data
                       matchers.urlencoded_params_matcher({'username': 'hannah+?/admin', 'password': 'Pas/?@sw0+rd!'})
                   ],
                   status=200,
                   body='TEST_JWT_TOKEN')

    client = InnovationSuite()
    token = client.jwt_login('hannah+?/admin', 'Pas/?@sw0+rd!')
    assert token == 'TEST_JWT_TOKEN'


@responses.activate
def test_ar_jwt_cookie_not_stored_in_session(mocker: MockerFixture):
    mocker.patch('config.Settings.INNOVATION_SUITE_URL', 'http://example.com')

    response_headers = HTTPHeaderDict()
    response_headers.add('Set-Cookie', 'cookie-A=value_A')
    response_headers.add('Set-Cookie', 'AR-JWT=AR_JWT_TOKEN, cookie-B=value_')
    response_headers.add('Set-Cookie', 'cookie-B=value_B')
    responses.post('http://example.com/api/jwt/login', status=200, body='TEST_JWT_TOKEN', headers=response_headers)

    client = InnovationSuite()
    token = client.jwt_login('user', 'password')
    assert token == 'TEST_JWT_TOKEN'
    assert 'AR-JWT' not in client.session.cookies
    assert client.session.cookies['cookie-A'] == 'value_A'
    assert client.session.cookies['cookie-B'] == 'value_B'
