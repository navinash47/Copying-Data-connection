import datetime
import os

import pytest

from helixplatform.models import Record, ArJwtToken, ArError
from utils.io_utils import read_file_to_str


def test_ar_jwt_token_with_expiry():
    token = ArJwtToken(jwt_token='JWT_TOKEN', expiry=datetime.datetime.utcnow())
    new_expiry = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
    token2 = token.with_expiry(expiry=new_expiry)
    assert token2.jwt_token == token.jwt_token
    assert token2.expiry == new_expiry


def test_record_get_set_fields():
    record = Record(recordDefinitionName='test_record_def')

    assert record[123] is None

    record[123] = 'abc'
    assert record[123] == 'abc'

    record[456] = 'def'
    assert record[123] == 'abc'
    assert record.fieldInstances['123'].value == 'abc'
    assert record[456] == 'def'
    assert record.fieldInstances['456'].value == 'def'

    del record[123]
    assert record[123] is None
    assert record[456] == 'def'


@pytest.mark.parametrize(
    'field_value,expected_result',
    [(None, None), ('0', False), ('1', True), (0, False), (1, True)])
def test_get_as_bool(field_value, expected_result):
    record = Record(recordDefinitionName='test_record_def')
    record[123] = field_value
    assert record.get_as_bool(123) == expected_result


@pytest.mark.parametrize(
    'errors_json,message_type,message_number,message_text,appended_text',
    [
        ("""[
    {
        "messageType": "ERROR",
        "messageText": "Authentication failed",
        "messageAppendedText": "hannah_adminx",
        "messageNumber": 623
    }
]""", 'ERROR', 623, 'Authentication failed', 'hannah_adminx'),
        ("""[
    {
        "messageType": "ERROR",
        "messageNumber": 0,
        "messageText": "",
        "appendedText": "hannah_admin"
    }
]""", 'ERROR', 0, '', 'hannah_admin'),
        ("""[
    {
        "messageType": "FAILURE",
        "messageNumber": 123,
        "messageText": null,
        "appendedText": null
    }
]""", 'FAILURE', 123, None, None)])
def test_parse_errors_else_none(
        errors_json: str | bytes,
        message_type: str,
        message_number: int,
        message_text: str | None,
        appended_text: str | None):
    errors = ArError.parse_errors_json_else_none(errors_json)
    assert len(errors) == 1
    error = errors[0]
    assert error.message_type == message_type
    assert error.message_number == message_number
    assert error.message_text == message_text
    assert error.appended_text == appended_text


def test_parse_errors_else_none_from_not_json():
    errors = ArError.parse_errors_json_else_none('not JSON')
    assert errors is None


def test_parse_errors_else_none_from_empty_array():
    errors = ArError.parse_errors_json_else_none('[]')
    assert len(errors) == 0


def test_parse_errors_else_none_with_uncommon_input():
    errors = ArError.parse_errors_json_else_none("""[
    {
        "messageType": "message type 1",
        "messageNumber": 123,
        "messageText": "message text 1",
        "appendedText": "appended text 1"
    },
    null,
    null,  
    {
        "messageType": "message type 2",
        "messageNumber": 456,
        "messageText": "message text 2",
        "appendedText": "appended text 2"
    }
]""")
    assert len(errors) == 2

    error = errors[0]
    assert error.message_type == "message type 1"
    assert error.message_number == 123
    assert error.message_text == "message text 1"
    assert error.appended_text == "appended text 1"

    error = errors[1]
    assert error.message_type == "message type 2"
    assert error.message_number == 456
    assert error.message_text == "message text 2"
    assert error.appended_text == "appended text 2"


def test_parse_errors_else_none_with_invalid_property():
    errors = ArError.parse_errors_json_else_none("""[
    {
        "messageType": "message type",
        "messageNumber": "not a number",  # should be an integer
        "messageText": "message text",
        "appendedText": "appended text"
    }
]""")
    assert errors is None


@pytest.mark.parametrize('errors_json', [
    ("""[
        {
            "messageType": "message type",
            "messageText": "message text",
            "appendedText": "appended text"
        }
    ]"""),
    ("""[
        {
            "messageNumber": 123,
            "messageText": "message text",
            "appendedText": "appended text"
        }
    ]""")])
def test_parse_errors_else_none_with_missing_property(errors_json):
    errors = ArError.parse_errors_json_else_none(errors_json)
    assert errors is None


def test_parse_leniently_with_ar_json():
    errors_json = """[{
            "messageType": "ERROR",
            "messageText": "Authentication failed",
            "messageAppendedText": "hannah_admin",
            "messageNumber": 623
        }
]"""
    errors = ArError.parse_leniently(errors_json)
    assert len(errors) == 1
    assert errors[0].message_type == 'ERROR'
    assert errors[0].message_number == 623
    assert errors[0].message_text == 'Authentication failed'
    assert errors[0].appended_text == 'hannah_admin'


def test_parse_leniently_with_rsso_response():
    rsso_response = read_file_to_str(os.path.dirname(__file__), 'rsso_401_response.html')
    errors = ArError.parse_leniently(rsso_response)
    assert len(errors) == 1
    assert errors[0].message_type == 'ERROR'
    assert errors[0].message_number == -1
    assert errors[0].message_text is None
    assert (errors[0].appended_text ==
            'Since your browser does not support JavaScript,'
            ' you must enable it and refresh\nthe page or press the Redirect button once to proceed.')


def test_parse_leniently_with_jetty_response():
    rsso_response = read_file_to_str(os.path.dirname(__file__), 'jetty_404_response.html')
    errors = ArError.parse_leniently(rsso_response)
    assert len(errors) == 1
    assert errors[0].message_type == 'ERROR'
    assert errors[0].message_number == -1
    assert errors[0].message_text is None
    assert 'The origin server did not find a current representation' in errors[0].appended_text
    assert ('<' not in errors[0].appended_text)  # no markup


@pytest.mark.parametrize('to_parse', [None, '', ' ', '\n', '<HTML> </HTML>'])
def test_parse_leniently_with_empty_equivalent_input(to_parse: str):
    errors = ArError.parse_leniently(to_parse)
    assert errors is None


def test_parse_leniently_with_html_markup_only():
    errors = ArError.parse_leniently('<HTML> </HTML>')
    assert errors is None


def test_parse_leniently_with_text():
    errors = ArError.parse_leniently('Failed to login. Please contact the Administrator.')
    assert len(errors) == 1
    assert str(errors[0]) == 'Failed to login. Please contact the Administrator.'


@pytest.mark.parametrize('ar_error,expected_str', [
    (
        ArError(message_type='ERROR',
                message_number=623,
                message_text='Authentication failed',
                appended_text='hannah_admin'),
        'ERROR (623) Authentication failed; hannah_admin'
    ),
    (
        ArError(message_type='ERROR',
                message_number=623,
                message_text=None,
                appended_text='hannah_admin'),
        'ERROR (623) ; hannah_admin'
    ),
    (
        ArError(message_type='ERROR',
                message_number=-1,
                message_text=None,
                appended_text='hannah_admin'),
        'hannah_admin'
    ),
])
def test_str(ar_error, expected_str):
    assert str(ar_error) == expected_str
