from __future__ import annotations

import collections
from dataclasses import dataclass
from datetime import datetime
import json
from json import JSONDecodeError

from typing import Dict, Any, List, AnyStr

from pydantic import BaseModel, ValidationError
from starlette.datastructures import UploadFile

from helixplatform.constants import AR_TRUE_AS_STR
from utils.io_utils import is_file_like
from utils.text_utils import html_to_markdown, trim_plain_text


@dataclass(frozen=True)
class ArJwtToken:
    """
    An AR session token along with its expiry. The objects are immutable because the idea is to replace the whole
    object instead of modifying its properties. Different threads should see a consistent token/expiry pair at all
    times.
    """
    jwt_token: str
    expiry: datetime

    def with_expiry(self, expiry: datetime) -> ArJwtToken:
        """ Returns copy of this ArJwtToken with the same token and the specified expiry datetime. """
        return ArJwtToken(self.jwt_token, expiry)


class FieldValue(BaseModel):
    id: int
    # Attachment, UploadFile and file-like objects are allowed to support attachment.
    value: str | Any | None
    permissionType: str = None  # e.g. "CHANGE"
    resourceType: str = "com.bmc.arsys.rx.services.record.domain.FieldInstance"


@dataclass
class Attachment:
    content: bytes
    filename: str | None
    content_type: str | None

    def read(self):
        return self.content


class Record(BaseModel):
    """
    Represents an Innovation Suite record as exposed to its REST API.
    """
    recordDefinitionName: str
    id: str | None = None  # ID (379)
    displayId: str | None = None  # Request ID (1)
    fieldInstances: Dict[str, FieldValue] = None  # keys are the field IDs (as strings, like returned by the API)
    resourceType: str = 'com.bmc.arsys.rx.services.record.domain.RecordInstance'

    def __getitem__(self, field_id: int):
        """ Returns the field value corresponding to the given field ID or None if not found. """
        if not self.fieldInstances:
            return None

        field_id_as_str = str(field_id)
        # get() will return None if not found (`[]` raises an error)
        field_value = self.fieldInstances.get(field_id_as_str)
        return field_value.value if field_value else None

    def get_as_bool(self, field_id: int) -> bool | None:
        """ Returns a field value as a bool or None if None. """
        raw_value = self[field_id]
        if raw_value is None:
            return None
        elif isinstance(raw_value, str):
            return raw_value == AR_TRUE_AS_STR
        else:
            return bool(raw_value)

    def __setitem__(self, field_id: int, value: Any):
        if not self.fieldInstances:
            self.fieldInstances = {}

        field_id_as_str = str(field_id)
        self.fieldInstances[field_id_as_str] = FieldValue(id=field_id, value=value)

    def __delitem__(self, field_id: int):
        field_id_as_str = str(field_id)
        del self.fieldInstances[field_id_as_str]


def record_to_json_dict(record: Record, exclude_unset: bool = True):
    """
    Returns a Dict whose contents translates directly to JSON (using `json.dumps()`).
    The default parameters will be typical for exchanges with IS.
    """
    return record.dict(exclude_unset=exclude_unset)


def record_to_json(record: Record):
    """ Returns the JSON representation of the specified Record. """
    return json.dumps(record_to_json_dict(record))


def record_to_request_files(record: Record) -> Dict[str, Any] | None:
    """
    Returns a Dict whose structure fits the `files` parameter of the _requests_ library i.e., the data structure
    specifying a multipart MIME message. This is determined by the presence of one or several file-like field values.
    If no so value exists, then `None` is returned. This indicates that a regular payload (e.g. JSON) should be used
    with the HTTP request.
    """
    if not record.fieldInstances:
        return None

    file_like_field_ids = []
    for field_id, field_value in record.fieldInstances.items():
        if field_value and is_file_like(field_value.value):
            file_like_field_ids.append(field_id)

    if not file_like_field_ids:
        return None

    files = {}
    for file_like_field_id in file_like_field_ids:
        value = record.fieldInstances[file_like_field_id].value
        if isinstance(value, UploadFile):
            # See https://docs.python-requests.org/en/latest/user/quickstart/#post-a-multipart-encoded-file.
            files[file_like_field_id] = [value.filename, value.file, value.content_type]
        elif isinstance(value, Attachment):
            files[file_like_field_id] = [value.filename, value.content, value.content_type]
        else:
            files[file_like_field_id] = value
        del record.fieldInstances[file_like_field_id]
    files['recordInstance'] = record_to_json(record)
    return files


class RecordDataPage(BaseModel):
    totalSize: int | None
    data: List[Dict[str, Any]] | None  # simplified Record structure


AR_ERROR_MAX_PLAIN_TEXT_CAPTURE = 255


class ArError(BaseModel):
    """ Represents an AR or IS error as returned within 4xx or 5xx REST responses. """
    message_type: str
    message_number: int
    message_text: str | None
    appended_text: str | None

    @staticmethod
    def from_dict(dict_: Dict[str, Any] | None) -> ArError | None:
        if not dict_:
            return None
        return ArError(message_type=dict_['messageType'],
                       message_number=dict_['messageNumber'],
                       message_text=dict_.get('messageText'),
                       # Seems that IS side: 'appendedText'; AR: 'messageAppendedText'
                       appended_text=dict_.get('appendedText') or dict_.get('messageAppendedText') or None)

    @staticmethod
    def parse_errors_json_else_none(errors_json: str) -> List[ArError] | None:
        try:
            array = json.loads(errors_json)
            if isinstance(array, collections.abc.Sequence):
                return [error for error in [ArError.from_dict(item) for item in array if isinstance(item, Dict)]]
        except (JSONDecodeError, ValidationError, KeyError):
            return None

    @staticmethod
    def parse_leniently(s: str | None) -> List[ArError] | None:
        """
        Parses a string into a list of errors using a couple of strategies in order to capture some of the details.
        In the case where the error isn't an JSON-formatted AR error, one ``ArError`` will be returned with the
        salvaged text as its appended text.

        :return: ``None`` if ``s`` is ``None`` or empty.
        """
        if not s:
            return None
        s = str(s)  # make sure we don't deal bytes from here on

        ar_errors = ArError.parse_errors_json_else_none(s)
        if not ar_errors:
            plain_text = trim_plain_text(html_to_markdown(s))
            if not plain_text:
                return None
            else:
                # make sure we don't log very long documents
                truncated_plain_text = plain_text[:AR_ERROR_MAX_PLAIN_TEXT_CAPTURE]
                ar_errors = [
                    ArError(
                        message_type='ERROR', message_number=-1, message_text=None, appended_text=truncated_plain_text)
                ]
        return ar_errors

    def __str__(self) -> str:
        if self.message_number > 0 or self.message_text:
            return f"{self.message_type} ({self.message_number}) {self.message_text or ''}; {self.appended_text or ''}"
        else:  # not originally a regular AR error
            return self.appended_text
