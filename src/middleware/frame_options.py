import typing

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class XFrameOptions:
    """ Sets the X-Frame-Options header in the HTTP response with the directive

        Usage::
            app.add_middleware(XFrameOptions, directive="SAMEORIGIN")
    """
    def __init__(
            self,
            app: ASGIApp,
            directive: str = 'DENY'
    ) -> None:
        """
        :param directive: X-Frame-Options directive value, must be one of "DENY" or "SAMEORIGIN"
        """
        self.app = app
        if directive in ('DENY', 'SAMEORIGIN'):
            self.directive = directive
        else:
            raise SyntaxError('X-Frame-Options directive must be one of "DENY" or "SAMEORIGIN"')

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        async def add_xfo_header(message: Message) -> None:
            if message['type'] == 'http.response.start':
                headers = MutableHeaders(scope=message)
                headers['X-Frame-Options'] = self.directive

            await send(message)

        await self.app(scope, receive, add_xfo_header)