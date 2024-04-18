import typing

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class XContentTypeOptions:
    """ Sets the X-Content-Type-Options in the HTTP response. The header has no parameter options.

        Usage:
            app.add_middleware(XContentTypeOptions)
    """
    def __init__(
            self,
            app: ASGIApp
    ) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        async def add_xcto_header(message: Message) -> None:
            if message['type'] == 'http.response.start':
                headers = MutableHeaders(scope=message)
                headers['X-Content-Type-Options'] = 'nosniff'

            await send(message)

        await self.app(scope, receive, add_xcto_header)