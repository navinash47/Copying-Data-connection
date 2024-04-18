import pytest

from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from middleware.frame_options import XFrameOptions


def dummy_page():
    return JSONResponse(None, status_code=204)


def test_default_header(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(XFrameOptions)]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 204
    assert response.headers['X-Frame-Options'] == "DENY"


def test_custom_header(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(XFrameOptions, directive='SAMEORIGIN')]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 204
    assert response.headers['X-Frame-Options'] == "SAMEORIGIN"


def test_invalid_header(client_factory):
    with pytest.raises(SyntaxError):
        app = FastAPI(
            routes=[APIRoute('/dummy', endpoint=dummy_page)],
            middleware=[Middleware(XFrameOptions, directive='INVALID')]
        )

        client = client_factory(app)
        client.get('/dummy')
