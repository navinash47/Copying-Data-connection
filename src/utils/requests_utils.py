import copy
from abc import ABC, abstractmethod

from loguru import logger
import requests.exceptions
import time
from typing import Mapping

from requests import PreparedRequest, Response
from requests.adapters import BaseAdapter
from requests.cookies import extract_cookies_to_jar


class BaseAdapterWrapper(BaseAdapter):
    def __init__(self, delegate: BaseAdapter):
        super().__init__()
        self.delegate = delegate

    def send(self,
             request: PreparedRequest,
             stream: bool = False,
             timeout: None | float | tuple[float, float] | tuple[float, None] = None,
             verify: bool | str = True,
             cert: None | bytes | str | tuple[bytes | str, bytes | str] = None,
             proxies: Mapping[str, str] | None = None) -> Response:
        return self.delegate.send(request, stream, timeout, verify, cert, proxies)

    def close(self) -> None:
        self.delegate.close()


class AdapterFilterChain:
    """ Abstract out the signature of BaseAdapter.send(). """

    def __init__(self,
                 adapter: 'FilteringAdapter',
                 filter: 'AdapterFilter',
                 request: PreparedRequest,
                 stream: bool = ...,
                 timeout: None | float | tuple[float, float] | tuple[float, None] = ...,
                 verify: bool | str = ...,
                 cert: None | bytes | str | tuple[bytes | str, bytes | str] = ...,
                 proxies: Mapping[str, str] | None = ...):
        self.__filtering_adapter = adapter
        self.__filter = filter
        self.request = request
        self.stream = stream
        self.timeout = timeout
        self.verify = verify
        self.cert = cert
        self.proxies = proxies
        self.response = None

    def _clone_with_next_filter(self) -> 'AdapterFilterChain':
        clone = copy.copy(self)
        clone.__filter = self.__filter.next if self.__filter else None
        return clone

    def reset(self):
        if self.response is None:
            raise RuntimeError('call send() before being able to reset')

        # read content and close response to allow to reuse connection (inspired by Requests auth code)
        _ = self.response.content
        self.response.close()

        new_request = self.request.copy()
        extract_cookies_to_jar(new_request._cookies, self.response.request, self.response.raw)
        new_request.prepare_cookies(new_request._cookies)
        self.request = new_request

    def send(self) -> Response:
        if self.__filter:
            next_chain = self._clone_with_next_filter()
            self.response = self.__filter.send(next_chain)
        else:
            self.response = self.send_to_adapter(self.__filtering_adapter.delegate)
        return self.response

    def resend(self) -> Response:
        """
        Closes the previously received response, resets the request and resends it against the FilteringAdapter,
        which owns this AdapterFilterChain. All filters will be reentered as well.
        """
        self.reset()
        return self.send_to_adapter(self.__filtering_adapter)

    def send_to_adapter(self, adapter: BaseAdapter) -> Response:
        return adapter.send(self.request, self.stream, self.timeout, self.verify, self.cert, self.proxies)


class AdapterFilter(ABC):
    def __init__(self):
        self.next: AdapterFilter | None = None

    @abstractmethod
    def send(self, chain: AdapterFilterChain) -> Response:
        raise NotImplementedError()


class FilteringAdapter(BaseAdapterWrapper):
    def __init__(self, delegate: BaseAdapter, filters: [AdapterFilter] = None):
        """
        A Requests adapter, which applies configured filters to requests before delegating to an actual BaseAdapter.

        :param delegate: requests adapter to ultimately delegate the request to
        :param filters:  list of the filters to execute for each request, in order of precedence (left-most is first)
        """
        super().__init__(delegate)
        self.filters: [AdapterFilter] = filters or []
        self.__init_filters_next()

    def __init_filters_next(self):
        previous = None
        for current in self.filters:
            if previous:
                previous.next = current
            previous = current

    def send(self,
             request: PreparedRequest,
             stream: bool = False,
             timeout: None | float | tuple[float, float] | tuple[float, None] = None,
             verify: bool | str = True,
             cert: None | bytes | str | tuple[bytes | str, bytes | str] = None,
             proxies: Mapping[str, str] | None = None) -> Response:
        first_filter = self.filters[0] if self.filters else None
        chain = AdapterFilterChain(self, first_filter, request, stream, timeout, verify, cert, proxies)
        return chain.send()


class LoggingFilter(AdapterFilter):
    """
    A request filter, which generically logs the HTTP calls.
    """

    def send(self, chain: AdapterFilterChain):
        request = chain.request
        logger.trace("about to {method} {url}", method=request.method, url=request.url)

        exception: Exception | None = None
        start_time = time.time()
        response = None
        try:
            response = chain.send()
        except Exception as e:
            exception = e

        if exception is None or isinstance(exception, requests.exceptions.HTTPError):
            status_code = str(response.status_code) if response is not None else 'N/A'
            duration = round((time.time() - start_time) * 1000)
            logger.debug(
                "{method} {url} {status_code} ({duration}ms)",
                method=request.method, url=request.url, status_code=status_code, duration=duration)
        else:
            logger.error("{method} {url} FAILED with {error}",
                         method=request.method, url=request.url, error=str(exception))

        if exception:
            raise exception

        return response
