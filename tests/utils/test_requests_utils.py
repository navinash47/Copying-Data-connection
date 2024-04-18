import typing

from pytest_mock import MockerFixture
import requests, requests.exceptions
import responses
from requests import ConnectionError, HTTPError, Response
from requests.adapters import HTTPAdapter

from utils.requests_utils import LoggingFilter, FilteringAdapter, AdapterFilter, AdapterFilterChain


class FakeFilter(AdapterFilter):

    def __init__(self, counter: typing.Generator):
        super().__init__()
        self.counter = counter
        self.recorded_method: str | None = None
        self. recorded_url: str | None = None
        self.rank: int | None = None

    def send(self, chain: AdapterFilterChain) -> Response:
        self.rank = next(self.counter)

        self.recorded_method = chain.request.method
        self.recorded_url = chain.request.url
        response = chain.send()
        assert response == chain.response
        return response


def counter(up_to: int):
    for i in range(1, up_to + 2):
        yield i


def test_adapter_filter():
    c = counter(2)
    filter_1 = FakeFilter(counter=c)
    filter_2 = FakeFilter(counter=c)

    session = requests.Session()
    session.mount('http://', FilteringAdapter(HTTPAdapter(), [filter_1, filter_2]))

    url = 'http://example.com/'
    responses.get(url, status=200, body='Hello!')

    response = session.get(url)

    assert response.status_code == 200

    assert filter_1.rank == 1
    assert filter_1.recorded_method == 'GET'
    assert filter_1.recorded_url == 'http://example.com/'
    assert filter_2.rank == 2
    assert filter_2.recorded_method == 'GET'
    assert filter_2.recorded_url == 'http://example.com/'


@responses.activate
def test_logging_filter(mocker: MockerFixture):  # happy path
    trace_mock = mocker.patch('utils.requests_utils.logger.trace')
    debug_mock = mocker.patch('utils.requests_utils.logger.debug')

    logging_filter = LoggingFilter()
    session = requests.Session()
    session.mount('http://', FilteringAdapter(HTTPAdapter(), [logging_filter]))

    method = 'GET'
    url = 'http://example.com/'
    responses.get(url, status=200, body='Hello!')

    response = session.get(url)

    trace_mock.assert_called_once()
    assert trace_mock.mock_calls[0].kwargs['method'] == method
    assert trace_mock.mock_calls[0].kwargs['url'] == url

    debug_mock.assert_called_once()
    assert debug_mock.mock_calls[0].kwargs['method'] == method
    assert debug_mock.mock_calls[0].kwargs['url'] == url
    assert debug_mock.mock_calls[0].kwargs['status_code'] == str(response.status_code)


@responses.activate
def test_logging_filter_with_http_error(mocker: MockerFixture):
    trace_mock = mocker.patch('utils.requests_utils.logger.trace')
    debug_mock = mocker.patch('utils.requests_utils.logger.debug')

    logging_filter = LoggingFilter()
    session = requests.Session()
    session.mount('http://', FilteringAdapter(HTTPAdapter(), [logging_filter]))

    method = 'GET'
    url = 'http://example.com/'
    responses.get(url, status=401, body='Nope!')

    response = session.get(url)

    trace_mock.assert_called_once()
    assert trace_mock.mock_calls[0].kwargs['method'] == method
    assert trace_mock.mock_calls[0].kwargs['url'] == url

    debug_mock.assert_called_once()
    assert debug_mock.mock_calls[0].kwargs['method'] == method
    assert debug_mock.mock_calls[0].kwargs['url'] == url
    assert debug_mock.mock_calls[0].kwargs['status_code'] == str(response.status_code)


@responses.activate
def test_logging_filter_with_http_error_without_passing_response(mocker: MockerFixture):
    trace_mock = mocker.patch('utils.requests_utils.logger.trace')
    debug_mock = mocker.patch('utils.requests_utils.logger.debug')

    logging_filter = LoggingFilter()
    session = requests.Session()
    session.mount('http://', FilteringAdapter(HTTPAdapter(), [logging_filter]))

    method = 'GET'
    url = 'http://example.com/'
    responses.get(url, status=401, body=HTTPError('server error'))

    try:
        session.get(url)
        assert False  # expecting `raise_for_status()` to raise an HTTPError
    except HTTPError:
        pass

    trace_mock.assert_called_once()
    assert trace_mock.mock_calls[0].kwargs['method'] == method
    assert trace_mock.mock_calls[0].kwargs['url'] == url

    debug_mock.assert_called_once()
    assert debug_mock.mock_calls[0].kwargs['method'] == method
    assert debug_mock.mock_calls[0].kwargs['url'] == url
    assert debug_mock.mock_calls[0].kwargs['status_code'] == 'N/A'


@responses.activate
def test_logging_filter_with_other_error(mocker: MockerFixture):
    trace_mock = mocker.patch('utils.requests_utils.logger.trace')
    error_mock = mocker.patch('utils.requests_utils.logger.error')

    logging_filter = LoggingFilter()
    session = requests.Session()
    session.mount('http://', FilteringAdapter(HTTPAdapter(), [logging_filter]))

    method = 'GET'
    url = 'http://example.com/'
    responses.get(url, status=401, body=ConnectionError('unreachable host'))

    try:
        session.get(url)
        assert False
    except ConnectionError:
        pass

    trace_mock.assert_called_once()
    assert trace_mock.mock_calls[0].kwargs['method'] == method
    assert trace_mock.mock_calls[0].kwargs['url'] == url

    error_mock.assert_called_once()
    assert error_mock.mock_calls[0].kwargs['method'] == method
    assert error_mock.mock_calls[0].kwargs['url'] == url
    assert 'unreachable host' in error_mock.mock_calls[0].kwargs['error']