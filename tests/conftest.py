import functools
import pytest

from fastapi.testclient import TestClient


@pytest.fixture()
def client_factory():
    return functools.partial(
        TestClient
    )
