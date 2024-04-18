from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class CacheControl:
    """ Sets the Cache-Control header in the HTTP response with the given policy directive. If the route has already
        set a header then this default header will not be set.

        Usage::
            app.add_middleware(ContentSecurityPolicy, policy="max-age=31536000, immutable")
    """
    def __init__(
            self,
            app: ASGIApp,
            directive: str = 'no-store'
    ) -> None:
        """
        :param directive: The full directive to assign to the header (optional).
                       default value: "no-store"
        """
        self.app = app
        self.directive = directive

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        async def add_cache_control_header(message: Message) -> None:
            if message['type'] == 'http.response.start':
                headers = MutableHeaders(scope=message)
                if 'Cache-Control' not in headers:
                    headers['Cache-Control'] = self.directive

            await send(message)

        await self.app(scope, receive, add_cache_control_header)
