import pytest

from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from middleware.strict_transport import StrictTransportSecurity


def dummy_page():
    return JSONResponse(None, status_code=204)


def test_default_header(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(StrictTransportSecurity)]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 204
    assert response.headers['Strict-Transport-Security'] == "max-age=432000"


def test_custom_header(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(StrictTransportSecurity, max_age=5555555)]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 204
    assert response.headers['Strict-Transport-Security'] == "max-age=5555555"


def test_invalid_header(client_factory):
    with pytest.raises(SyntaxError):
        app = FastAPI(
            routes=[APIRoute('/dummy', endpoint=dummy_page)],
            middleware=[Middleware(StrictTransportSecurity, max_age=-5)]
        )

        client = client_factory(app)
        response = client.get('/dummy')
