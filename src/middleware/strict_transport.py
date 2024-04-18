import typing

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class StrictTransportSecurity:
    """ Sets the HTTP Strict-Transport-Security header in the HTTP response with the given max-age directive

        Usage::
            app.add_middleware(StrictTransportSecurityMiddleware, max_age=30)
    """
    def __init__(
            self,
            app: ASGIApp,
            max_age: typing.Optional[int] = 5 * 24 * 60 * 60  # 5 days
    ) -> None:
        """
        :param max_age: The time, in seconds, that the browser should remember that a site is only to be accessed
        using HTTPS. Must be a positive integer.  Default value is 432000 (5 days)
        """
        self.app = app
        if max_age > 0:
            self.max_age = max_age
        else:
            raise SyntaxError('max-age must be a positive integer')

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        async def add_hsts_header(message: Message) -> None:
            if message['type'] == 'http.response.start':
                headers = MutableHeaders(scope=message)
                hsts_value = 'max-age=' + str(self.max_age)
                headers['Strict-Transport-Security'] = hsts_value

            await send(message)

        await self.app(scope, receive, add_hsts_header)
