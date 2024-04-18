from typing import Callable

from requests import PreparedRequest


def no_header_matcher(header: str) -> Callable[[PreparedRequest], tuple[bool, str]]:
    """
    Builds and returns a Responses request matcher, which makes sure that
    :param header:
    :return:
    """
    def match(request: PreparedRequest) -> tuple[bool, str]:
        if header in request.headers:
            return False, f"Headers do not match: found `${header}: ${request.headers[header]}` when not expected"
        else:
            return True, ''
    return match
