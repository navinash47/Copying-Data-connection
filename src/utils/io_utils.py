import json
import os
from typing import Dict, AnyStr

from .object_utils import has_callable_attr


def read_file_to_str(dirname, file) -> AnyStr:
    """
    Loads a text file into a parsed dict.

        read_json_dict(os.path.dirname(__file__), 'some_resource_file.json')
    """
    file_path = str(os.path.join(dirname, file))
    with open(file_path, mode='r') as file:
        return file.read()


def read_json_dict(dirname, file) -> Dict:
    """
    Loads a JSON file into a parsed dict.

        read_json_dict(os.path.dirname(__file__), 'some_resource_file.json')
    """
    file_path = str(os.path.join(dirname, file))
    with open(file_path, mode='r') as file:
        return json.load(file)


def is_file_like(o) -> bool:
    """ Indicates whether `o` is a file-like object. """
    return has_callable_attr(o, 'read')
