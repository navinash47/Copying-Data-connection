import re
import threading
import types
import urllib.parse
from datetime import datetime, timedelta
from http import cookiejar
from http.cookiejar import Cookie
from typing import Dict, List, Any
from urllib.parse import urljoin, urlencode, quote_plus

import jwt
import requests
from loguru import logger
from requests import RequestException, PreparedRequest, Response, HTTPError
from requests.adapters import HTTPAdapter

from config import Settings
from health.constants import HealthStatus
from health.models import Health, HealthIndicator
from helixplatform.constants import DATA_CONNECTION_BUNDLE
from helixplatform.models import Record, record_to_request_files, record_to_json_dict, Attachment, RecordDataPage, \
    ArJwtToken, ArError
from utils.collections_utils import copy_dict_without_none_values, join_int_iterable
from utils.file_types_utils import ContentType
from utils.http_utils import get_content_disposition_filename, parse_rfc_5322_datetime
from utils.requests_utils import FilteringAdapter, LoggingFilter, AdapterFilter, AdapterFilterChain


class NoArJwtCookiePolicy(cookiejar.DefaultCookiePolicy):
    """ A cookie policy that prevents storing the AR-JWT cookie. Used to prevent this cookie from interfering with
        the life-cycle of ``Authorization`` header we use in ``ArRestClient`` (AR servers may pick either to
        identify and authenticate a client's account). """
    def set_ok(self, cookie: Cookie, request) -> bool:
        return cookie.name != 'AR-JWT' and super().set_ok(cookie, request)


class ArAuthFilter(AdapterFilter):
    TOKEN_REFRESH_MARGIN_MINUTES = timedelta(minutes=1)
    DEFAULT_TIMEOUT_MINUTES = timedelta(minutes=2)
    AUTH_PATHS = {
        '/api/jwt/login',
        '/api/jwt/logout',
        '/api/rx/authentication/loginrequest',
        '/api/myit-sb/users/login'
    }

    def __init__(self, client: 'ArRestClient'):
        super().__init__()
        self.client = client
        self.jwt_token: ArJwtToken | None = None
        self.thread_local_storage = threading.local()

    def refresh_jwt_token(self):
        token = self.client.jwt_login()
        default_expiry = ArAuthFilter.parse_jwt_token_expiry(token)
        if default_expiry is None:
            # Apply a default expiry since /api/jwt/login doesn't return the timeout headers.
            # We assume that the token will nearly immediately be used with an end-point that does return these headers.
            default_expiry = datetime.utcnow() + ArAuthFilter.DEFAULT_TIMEOUT_MINUTES
        self.jwt_token = ArJwtToken(token, default_expiry)

    @staticmethod
    def parse_jwt_token_expiry(token):
        try:
            decoded_token = jwt.decode(token, options={'verify_signature': False})
            token_idle_expiry = ArAuthFilter._epoch_secs_to_datetime(decoded_token.get('exp'))
            token_absolute_expiry = ArAuthFilter._epoch_secs_to_datetime(decoded_token.get('_absoluteExpirationTime'))
            return ArAuthFilter._lenient_min(token_idle_expiry, token_absolute_expiry)

        except Exception as e:
            logger.warning('unable to decode AR JWT token: {exception}', exception=str(e))
            return None

    @staticmethod
    def _epoch_secs_to_datetime(epoch_secs) -> datetime | None:
        if isinstance(epoch_secs, int) or isinstance(epoch_secs, float):
            return datetime.utcfromtimestamp(epoch_secs)
        else:
            return None

    @staticmethod
    def _lenient_min(a: datetime | None, b: datetime | None) -> datetime | None:
        if a is None:
            return b
        elif b is None:
            return a
        else:
            return min(a, b)

    @staticmethod
    def is_auth_request(request: PreparedRequest) -> bool:
        url = request.url
        path = urllib.parse.urlparse(url).path
        return path in ArAuthFilter.AUTH_PATHS

    def _needs_token_refresh(self, at: datetime):
        return self.jwt_token is None or (at + ArAuthFilter.TOKEN_REFRESH_MARGIN_MINUTES) >= self.jwt_token.expiry

    @property
    def resending_on_401(self) -> bool:
        return hasattr(self.thread_local_storage, 'resending_on_401') \
            and self.thread_local_storage.resending_on_401

    @resending_on_401.setter
    def resending_on_401(self, b: bool):
        self.thread_local_storage.resending_on_401 = b

    def send(self, chain: AdapterFilterChain):
        # Don't try to log in prior to dealing with logins and logouts
        if ArAuthFilter.is_auth_request(chain.request):
            return chain.send()

        if self._needs_token_refresh(datetime.utcnow()):
            self.refresh_jwt_token()

        if self.jwt_token:
            chain.request.headers['Authorization'] = f"AR-JWT {self.jwt_token.jwt_token}"
        response = chain.send()

        if response.status_code == 401 and not self.resending_on_401:
            self.resending_on_401 = True
            self.refresh_jwt_token()
            response = chain.resend()
        else:
            self.resending_on_401 = False

        # check headers for new expiry
        self.update_jwt_token_expiry(response)

        return response

    def update_jwt_token_expiry(self, response: Response):
        raw_idle_expiry = response.headers.get('Session-Expiration')
        idle_expiry = parse_rfc_5322_datetime(raw_idle_expiry, offset_naive=True)
        raw_absolute_expiry = response.headers.get('Absolute-Session-Expiration')
        absolute_expiry = parse_rfc_5322_datetime(raw_absolute_expiry, offset_naive=True)

        expiry = ArAuthFilter._lenient_min(idle_expiry, absolute_expiry)
        if expiry is not None:
            # changing the reference should be atomic according to the literature
            self.jwt_token = self.jwt_token.with_expiry(expiry)


class ImpersonateFilter(AdapterFilter):

    def __init__(self, impersonated_user: str | None):
        super().__init__()
        self.impersonated_user = impersonated_user

    def send(self, chain: AdapterFilterChain) -> Response:
        if self.impersonated_user and not ArAuthFilter.is_auth_request(chain.request):
            chain.request.headers['impersonated-user-id'] = self.impersonated_user
        return chain.send()


class ArErrorFilter(AdapterFilter):
    """ Allows to report more than the HTTP status on errors by customizing ``Response.raise_for_status()``. """
    MAX_CHARS_IN_LOGGED_RESPONSE = 512

    def send(self, chain: AdapterFilterChain) -> Response:
        response = chain.send()
        if 400 <= response.status_code < 600:
            self._monkey_patch_raise_for_status(response)
        return response

    @staticmethod
    def _monkey_patch_raise_for_status(to_patch: Response):
        """ Replaces the default implementation of ``raise_for_status()`` on ``self`` by one that parses the
            response and includes it in the raised ``HTTPError``. Also, logs the response body on errors. """
        def custom_raise_for_status(self):
            try:
                self._old_raise_for_status()
            except HTTPError as http_error:
                response_text = self.text
                logger.error('HTTP error {status} with response body:\n{response}',
                             status=str(http_error),
                             response=response_text[:ArErrorFilter.MAX_CHARS_IN_LOGGED_RESPONSE])
                ar_errors = ArError.parse_leniently(response_text)
                if ar_errors:
                    extra_message = '\n'.join(str(ar_error) for ar_error in ar_errors)
                    raise HTTPError(str(http_error) + ' - ' + extra_message, response=self)
                else:
                    raise http_error

        to_patch._old_raise_for_status = to_patch.raise_for_status
        to_patch.raise_for_status = types.MethodType(custom_raise_for_status, to_patch)


# Terminology used with this code:
#   `record_id`: field "ID" (379)
#   `display_id`: field "Request ID" (1)
#   `guid`: field "GUID" (179), which is not the same as field 379 (when both exists)
class ArRestClient:

    def __init__(self, base_url: str, username: str, password: str, impersonated_user: str = None):
        """
        :param base_url: base URL of the REST API of the AR server
        :param username: login username
        :param password: login password
        :param impersonated_user: user to impersonate in all calls except the ones related to authentication
        """
        self.base_url = base_url
        self.username = username
        self.password = password
        self.impersonated_user = impersonated_user
        self.ar_auth_filter = ArAuthFilter(self)
        self._init_session()

    def _init_session(self):
        self.session = requests.Session()
        self.session.cookies.set_policy(NoArJwtCookiePolicy())

        adapter_filters = [ArErrorFilter(), self.ar_auth_filter, LoggingFilter()]
        if self.impersonated_user:
            logger.info(
                '{client} session will impersonate user "{user}"',
                client=type(self).__name__, user=self.impersonated_user)
            adapter_filters.append(ImpersonateFilter(self.impersonated_user))

        adapter = FilteringAdapter(HTTPAdapter(), adapter_filters)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def build_url(self, path: str, query_params: Dict | None = None) -> str:
        return urljoin(self.base_url, path + (query_params and ('?' + urlencode(query_params)) or ''))

    def jwt_login(self, username: str | None = None, password: str | None = None) -> str:
        """ Performs a JWT login and returns the JWT token if successful. """
        username = username or self.username
        password = password or self.password
        # using a dict the data is automatically encoded
        # https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
        data = {'username': username, 'password': password}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        url = self.build_url('/api/jwt/login')

        response = self.session.post(url,
                                     data=data,
                                     headers=headers)
        response.raise_for_status()
        return response.text

    def jwt_logout(self, token: str) -> None:
        url = self.build_url('/api/jwt/logout')
        headers = {'Authorization': f"AR-JWT {token}"}
        requests.post(url, headers=headers)

    @staticmethod
    def _get_entries_query(qualification: str | None,
                           fields: List | None,
                           offset: int | None,
                           limit: int | None) -> Dict:
        raw_query = {'q': qualification, 'fields': fields, 'offset': offset, 'limit': limit}
        return copy_dict_without_none_values(raw_query)

    def get_entries(self,
                    form: str,
                    qualification: str | None = None,
                    fields: List[int] | None = None,
                    offset: int | None = None,
                    limit: int | None = None):
        """ Retrieves AR entries for the specified AR form and matching the specified qualification. """
        headers = {'Content-Type': ContentType.APPLICATION_JSON}
        if fields:
            fields = f"values({','.join([str(field) for field in fields])})"
        query = ArRestClient._get_entries_query(qualification, fields, offset, limit)

        url = self.build_url(f"/api/arsys/v1/entry/{quote_plus(form)}", query)

        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def enumerate_all_entries(self,
                              form: str,
                              qualification: str | None = None,
                              fields: List[int] | None = None):
        """
        Returns a generator on all the entries of the specified form regardless of max-entries-by-query.
        You iterate on that iterator and paging will be handled transparently.

        :param form:  name of the form to query
        :param qualification: qualification to apply to the query
        :param fields: list of the fields to return
        :return: a generator providing the values of each of the entries returned by the AR form query endpoint.
                 The full response payloads aren't accessible via this function.
        """
        offset = 0
        while True:  # Looping on the pages
            # Because the AR response doesn't contain any indication of whether there is more,
            # we shift the offset until we get an empty result.
            response = self.get_entries(form, qualification, fields, offset=offset)
            entries = response['entries']
            if entries:
                offset += len(entries)
            else:
                break
            for entry in response['entries']:
                yield entry['values']


class InnovationSuite(ArRestClient):
    """ An Innovation Suite REST client. Also supports end-points more specific to IS. """

    def __init__(self, base_url: str = None, username: str = None, password: str = None, impersonated_user: str = None):
        super().__init__(
            base_url or Settings.INNOVATION_SUITE_URL,
            username or Settings.INNOVATION_SUITE_USER,
            password or Settings.INNOVATION_SUITE_PASSWORD,
            impersonated_user)

    @staticmethod
    def build_rx_headers(default_bundle_scope: str | None = None,
                         content_type: str | None = ContentType.APPLICATION_JSON,
                         accept: str | None = ContentType.APPLICATION_JSON,
                         override_optimistic_locking: bool = False,
                         should_query_all_locales: bool = True):
        headers = {
            'X-Requested-By': 'Helix-GPT/data-connection'
        }

        if content_type:
            headers['Content-Type'] = content_type
        if accept:
            headers['Accept'] = accept
        if default_bundle_scope:
            headers['default-bundle-scope'] = default_bundle_scope
        if override_optimistic_locking is not None:
            headers['Override-Optimistic-Lock'] = str(override_optimistic_locking)
        if should_query_all_locales is not None:
            headers['Should-Query-All-Locales'] = str(should_query_all_locales)

        return headers

    def get_current_user(self) -> Dict:
        """ Returns information about the user associated with the current AR session. """
        headers = self.build_rx_headers(None)
        url = self.build_url("/api/rx/application/user/$USER$")

        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_record(
            self,
            record_definition: str,
            record_id: str,
            default_bundle_scope: str | None = DATA_CONNECTION_BUNDLE) -> Record:
        """ Retrieves a single record by record ID (379). """
        headers = self.build_rx_headers(default_bundle_scope)

        url = self.build_url(
            f"/api/rx/application/record/recordinstance/{quote_plus(record_definition)}/{quote_plus(record_id)}")

        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return Record.parse_raw(response.text)

    def get_all_records(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """
        Retrieves all the specified records (using the IS data page end-point).

        :param record_definition: scoped name of the record definition
        :param property_selection: list of the field IDs to fetch. `None` to get the default list of fields.
        :param sort_by: list of the field IDs to order the result. Negative IDs indicate a descending order.
        :param query_expression: IS record qualification. `None` to ignore.
        :param include_total_size: indicates whether the data page should be returned with the "total size" property.
                                   This typically makes the call slower.
        :param default_bundle_scope: contextualizes the query to this bundle scope
                                     (not sure if it has an effect on this interaction).
        """
        start_index = 0
        results: List[Dict[str, Any]] = []
        while True:
            kwargs['start_index'] = start_index
            page = self.get_records(*args, **kwargs)
            if page.data:
                results.extend(page.data)
                start_index += len(page.data)
            else:
                return results

    def get_records(
            self,
            record_definition: str,
            property_selection: List[int] = None,
            sort_by: List[int] = None,
            start_index: int = 0,
            page_size: int = -1,
            query_expression: str = None,
            include_total_size: bool = False,
            default_bundle_scope: str | None = DATA_CONNECTION_BUNDLE) -> RecordDataPage:
        """
        Retrieves one page of the specified records (using the IS data page end-point).

        :param record_definition: scoped name of the record definition
        :param property_selection: list of the field IDs to fetch. `None` to get the default list of fields.
        :param sort_by: list of the field IDs to order the result. Negative IDs indicate a descending order.
        :param start_index: index of the first record to return
        :param page_size: size of the page to fetch. -1 indicates IS to use the default page size (2000 by default)
        :param query_expression: IS record qualification. `None` to ignore.
        :param include_total_size: indicates whether the data page should be returned with the "total size" property.
                                   This typically makes the call slower.
        :param default_bundle_scope: contextualizes the query to this bundle scope
                                     (not sure if it has an effect on this interaction).
        """
        headers = self.build_rx_headers(default_bundle_scope)

        raw_query = {
            'dataPageType': 'com.bmc.arsys.rx.application.record.datapage.RecordInstanceDataPageQuery',
            'recorddefinition': record_definition,
            'propertySelection': join_int_iterable(property_selection, ','),
            'sortBy': join_int_iterable(sort_by, ','),
            'startIndex': start_index,
            'pageSize': page_size,
            'queryExpression': query_expression,
            'shouldIncludeTotalSize': str(include_total_size).lower()
        }
        query = copy_dict_without_none_values(raw_query)

        url = self.build_url('/api/rx/application/datapage', query)

        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return RecordDataPage.parse_raw(response.text)

    def get_attachment(
            self,
            record_definition: str,
            record_id: str,
            field_id: int,
            default_bundle_scope: str | None = DATA_CONNECTION_BUNDLE) -> Attachment:
        """
        Returns an attachment field value as a `(content_bytes, filename)` tuple. The filename may be `None`.
        """
        headers = self.build_rx_headers(default_bundle_scope, accept='*/*')

        url = self.build_url(
            f"/api/rx/application/record/attachment/"
            f"{quote_plus(record_definition)}/{quote_plus(record_id)}/{quote_plus(str(field_id))}")

        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        response_content_type = response.headers.get('Content-Type')
        response_content_disposition = response.headers.get('Content-Disposition')
        response_filename = get_content_disposition_filename(response_content_disposition)
        return Attachment(
            content=response.content,
            filename=response_filename,
            content_type=response_content_type)

    def create_record(
            self,
            record: Record,
            default_bundle_scope: str | None = DATA_CONNECTION_BUNDLE):
        """ Creates a new IS record. """
        files = record_to_request_files(record)
        json = record_to_json_dict(record) if not files else None
        if files:
            headers = self.build_rx_headers(default_bundle_scope, content_type=None)
        else:
            headers = self.build_rx_headers(default_bundle_scope)

        url = self.build_url('/api/rx/application/record/recordinstance')

        response = self.session.post(url, headers=headers, json=json, files=files)
        response.raise_for_status()
        response_location = response.headers['Location']
        if response_location:
            record_id = re.sub('^.*/([^/]+)$', '\g<1>', response_location)
            return record_id
        else:
            # In theory, this shouldn't happen on a successful execution.
            logger.warning("no ID returned on '{record}' record creation", record=record.recordDefinitionName)
            return None

    def update_record(
            self,
            record: Record,
            default_bundle_scope: str | None = DATA_CONNECTION_BUNDLE,
            override_optimistic_locking=False):
        """ Creates a new IS record. """
        if not record.id:
            raise ValueError('cannot update a record without ID value')
        if not record.recordDefinitionName:
            raise ValueError('cannot update a record without record definition name')

        headers = self.build_rx_headers(
            default_bundle_scope, override_optimistic_locking=override_optimistic_locking)

        url = self.build_url(
            f"/api/rx/application/record/recordinstance/"
            f"{quote_plus(record.recordDefinitionName)}/{quote_plus(record.id)}")

        response = self.session.put(url, headers=headers, json=record.dict(exclude_unset=True))
        response.raise_for_status()


class HelixPlatformHealthIndicator(ArRestClient, HealthIndicator):

    def __init__(self, ):
        super().__init__(
            Settings.INNOVATION_SUITE_URL,
            Settings.INNOVATION_SUITE_USER,
            Settings.INNOVATION_SUITE_PASSWORD
        )

    def get_health(self) -> Health:
        try:
            token = self.jwt_login()
            self.jwt_logout(token)
            return Health(name='helixplatform', status=HealthStatus.UP)
        except RequestException as e:
            logger.error('platform service is unavailable: {cause}', cause=e)

        return Health(name='helixplatform', status=HealthStatus.DOWN)
