import pytest

from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from middleware.cache_control import CacheControl


def dummy_page():
    return JSONResponse(None, status_code=200)


def cacheable_page():
    return JSONResponse(None, status_code=200, headers={'Cache-Control': 'public, max-age=604800'})


def test_default_header_added_missing(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(CacheControl)]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 200
    assert response.headers['Cache-Control'] == 'no-store'


def test_default_header_not_added_when_set_on_response(client_factory):
    app = FastAPI(
        routes=[APIRoute('/cacheable_page', endpoint=cacheable_page)],
        middleware=[Middleware(CacheControl)]
    )

    client = client_factory(app)
    response = client.get('/cacheable_page')

    assert response.status_code == 200
    assert response.headers['Cache-Control'] == 'public, max-age=604800'


def test_custom_header_added_missing(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(CacheControl, directive='max-age=31536000, immutable')]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 200
    assert response.headers['Cache-Control'] == 'max-age=31536000, immutable'


def test_custom_header_not_added_when_set_on_response(client_factory):
    app = FastAPI(
        routes=[APIRoute('/cacheable_page', endpoint=cacheable_page)],
        middleware=[Middleware(CacheControl, directive='max-age=31536000, immutable')]
    )

    client = client_factory(app)
    response = client.get('/cacheable_page')

    assert response.status_code == 200
    assert response.headers['Cache-Control'] == 'public, max-age=604800'
