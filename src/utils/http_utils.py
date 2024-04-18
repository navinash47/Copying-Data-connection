from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from typing import Dict
from urllib.parse import unquote_plus


def get_content_disposition_filename(content_disposition: str | None) -> str | None:
    """
    Parses the passed `Content-Disposition` header value and returns its filename or None if it doesn't contain any.
    Returns None if None is passed.
    """
    if content_disposition:
        email = EmailMessage()
        email['Content-Disposition'] = content_disposition
        filename = email.get_filename()
        return unquote_plus(filename) if filename else None
    else:
        return None


def parse_content_type(content_type: str | None) -> (str, Dict[str, str]):
    """
    Parses the specified ``Content-Type`` header value into a tuple containing the MIME type and a dictionary of the
    attributes. Returns ``None`` if ``content_type`` is ``None`` or if it cannot be parsed.
    """
    if content_type:
        email = EmailMessage()
        email['Content-Type'] = content_type
        params = email.get_params()  # -> e.g., params = [('application/json', ''), ('charset', 'UTF-8')]
        if params:
            return params[0][0], dict(params[1:])
    return None, None


def is_content_type_of_mime_type(content_type: str | None, mime_type: str) -> bool:
    """
    Returns whether the specified ``Content-Type`` header value has the specified MIME type.

    :param content_type: e.g., ``application/json;charset=UTF-8``
    :param mime_type:  e.g., ``application/json``
    """
    content_type_mime_type, _ = parse_content_type(content_type)
    return content_type_mime_type == mime_type


def parse_rfc_5322_datetime(s: str, offset_naive: bool = False) -> datetime | None:
    """
    Parses an RFC-5322 date-time (e.g., "Fri, 06 Oct 2023 02:42:22 GMT") into a ``datetime`` object.

    :param s: text to parse
    :param offset_naive: if True, this method will return the parsed datetime in UTC with a timezone of `None`.
                         This is mostly meant to help normalizing the parsed date/times.
    :return: ``None`` if ``s`` is ``None``.
    """
    dt = parsedate_to_datetime(s) if s else None
    if dt is not None and offset_naive:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt
