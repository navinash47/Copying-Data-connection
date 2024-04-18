from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from middleware.content_security_policy import ContentSecurityPolicy


def dummy_page():
    return JSONResponse(None, status_code=204)


def test_default_header(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(ContentSecurityPolicy)]
    )

    client = client_factory(app)
    response = client.get('/dummy')

    assert response.status_code == 204
    assert response.headers['Content-Security-Policy'] == "frame-ancestors 'none'"


def test_custom_header(client_factory):
    app = FastAPI(
        routes=[APIRoute('/dummy', endpoint=dummy_page)],
        middleware=[Middleware(ContentSecurityPolicy, policy="frame-ancestors 'none'; script-src 'self'")]
    )

    client = client_factory(app)
    response = client.get('/dummy',)

    assert response.status_code == 204
    assert response.headers['Content-Security-Policy'] == "frame-ancestors 'none'; script-src 'self'"
