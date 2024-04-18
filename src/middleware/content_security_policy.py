from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class ContentSecurityPolicy:
    """ Sets the Content-Security-Policy header in the HTTP response with the given policy directive

        Usage::
            app.add_middleware(ContentSecurityPolicyMiddleware, policy="frame-ancestors 'none'; script-src 'self'")
    """
    def __init__(
            self,
            app: ASGIApp,
            policy: str = "frame-ancestors 'none'"
    ) -> None:
        """
        :param policy: The full policy directive to assign to the header (optional).
                       default value: "frame-ancestors 'none'"
        """
        self.app = app
        self.policy = policy

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        async def add_csp_header(message: Message) -> None:
            if message['type'] == 'http.response.start':
                headers = MutableHeaders(scope=message)
                headers['Content-Security-Policy'] = self.policy

            await send(message)

        await self.app(scope, receive, add_csp_header)
