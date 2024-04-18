from main import app
from fastapi.responses import JSONResponse, PlainTextResponse


@app.get('/test')
def dummy_page():
    return JSONResponse(None, status_code=200)


@app.get('/large_page')
def large_page():
    return PlainTextResponse("x" * 600, status_code=200)


@app.get('/small_page')
def small_page():
    return PlainTextResponse("x" * 100, status_code=200)


def test_middleware_security_headers(client_factory):
    client = client_factory(app)

    response = client.get('/test')

    assert response.status_code == 200
    assert response.headers['Cache-Control'] == "no-store"
    assert response.headers['Content-Security-Policy'] == "frame-ancestors 'none'"
    assert response.headers['Strict-Transport-Security'] == "max-age=432000"
    assert response.headers['X-Content-Type-Options'] == 'nosniff'
    assert response.headers['X-Frame-Options'] == "DENY"


def test_middleware_with_gzip(client_factory):
    client = client_factory(app)

    response = client.get('/large_page')

    assert response.headers['Content-Encoding'] == 'gzip'
    assert response.headers['Vary'] == 'Accept-Encoding'
    assert int(response.headers['Content-Length']) < 600


def test_middleware_without_gzip(client_factory):
    client = client_factory(app)

    response = client.get('/small_page')

    assert 'Content-Encoding' not in response.headers
    assert int(response.headers['Content-Length']) == 100
