from datetime import datetime, tzinfo, timedelta, timezone

import pytest

from utils.http_utils import get_content_disposition_filename, parse_rfc_5322_datetime, parse_content_type, \
    is_content_type_of_mime_type


@pytest.mark.parametrize(
    'content_disposition,expected_filename', [
        ('form-data; name="fieldName"; filename="filename.jpg"', 'filename.jpg'),
        ('form-data; name="fieldName"; filename="my%20beautiful+file.jpg"', 'my beautiful file.jpg'),
        ("attachment; filename*=UTF-8''my_document.pdf",         'my_document.pdf'),
    ]
)
def test_get_content_disposition_filename(content_disposition, expected_filename):
    assert get_content_disposition_filename(content_disposition) == expected_filename


PDT_TIMEZONE = timezone(timedelta(days=-1, seconds=61200))


@pytest.mark.parametrize(
    'rfc_5322,offset_naive,expected_iso,expected_tz', [
        ('Fri, 06 Oct 2023 02:42:22 GMT', False, '2023-10-06T02:42:22Z', timezone.utc),
        ('Fri, 06 Oct 2023 02:42:22 GMT', True, '2023-10-06T02:42:22', None),
        ('Fri, 06 Oct 2023 02:42:22 PDT', False, '2023-10-06T02:42:22-07:00', PDT_TIMEZONE),
        ('Fri, 06 Oct 2023 02:42:22 PDT', True, '2023-10-06T09:42:22', None),
    ]
)
def test_parse_rfc_5322_datetime(rfc_5322: str, offset_naive: bool, expected_iso: str, expected_tz: tzinfo):
    parsed_datetime = parse_rfc_5322_datetime(rfc_5322, offset_naive)
    expected_datetime = datetime.fromisoformat(expected_iso)
    assert parsed_datetime == expected_datetime
    assert parsed_datetime.tzinfo == expected_tz


@pytest.mark.parametrize('content_type,expected_mime_type,expected_attributes', [
    (None, None, None),
    ('', None, None),
    ('application/json', 'application/json', {}),
    ('application/json;charset=UTF-8', 'application/json', {'charset': 'UTF-8'}),
    ('form-data; name="myFile"; filename="foo.txt"', 'form-data', {'name': 'myFile', 'filename': 'foo.txt'}),
])
def test_parse_content_type(content_type: str, expected_mime_type: str, expected_attributes: str):
    mime_type, attributes = parse_content_type(content_type)
    assert mime_type == expected_mime_type
    assert attributes == expected_attributes


@pytest.mark.parametrize('content_type,mime_type,expected_result', [
    (None, 'text/plain', False),
    ('', 'text/plain', False),
    ('application/json', 'text/plain', False),
    ('text/plain', 'text/plain', True),
    ('application/json;charset=UTF-8', 'application/json', True),
])
def test_is_content_type_of_mime_type(content_type: str, mime_type: str, expected_result: bool):
    assert is_content_type_of_mime_type(content_type, mime_type) == expected_result
