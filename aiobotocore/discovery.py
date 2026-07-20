import inspect

from botocore.discovery import (
    EndpointDiscoveryHandler,
    EndpointDiscoveryManager,
    EndpointDiscoveryRefreshFailed,
    HTTPClientError,
    logger,
)


class AioEndpointDiscoveryManager(EndpointDiscoveryManager):
    async def _refresh_current_endpoints(self, **kwargs):
        pass

    async def describe_endpoint(self, **kwargs):
        pass


class AioEndpointDiscoveryHandler(EndpointDiscoveryHandler):
    async def discover_endpoint(self, request, operation_name, **kwargs):
        pass
