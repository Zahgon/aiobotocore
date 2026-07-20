import asyncio
import contextlib
import io
import os
import socket
from concurrent.futures import CancelledError

import aiohttp  # lgtm [py/import-and-import-from]
from aiohttp import (
    ClientConnectionError,
    ClientConnectorError,
    ClientHttpProxyError,
    ClientProxyConnectionError,
    ClientSSLError,
    ServerDisconnectedError,
    ServerTimeoutError,
)
from aiohttp.client import URL
from botocore.httpsession import (
    MAX_POOL_CONNECTIONS,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    InvalidProxiesConfigError,
    LocationParseError,
    ProxyConfiguration,
    ProxyConnectionError,
    ReadTimeoutError,
    SSLError,
    _is_ipaddress,
    create_urllib3_context,
    ensure_boolean,
    get_cert_path,
    logger,
    mask_proxy_url,
    parse_url,
    urlparse,
)
from multidict import CIMultiDict

import aiobotocore.awsrequest

from ._constants import DEFAULT_KEEPALIVE_TIMEOUT
from ._endpoint_helpers import _IOBaseWrapper, _text


class AIOHTTPSession:
    def __init__(
        self,
        verify: bool = True,
        proxies: dict[str, str] = None,  # {scheme: url}
        timeout: float = None,
        max_pool_connections: int = MAX_POOL_CONNECTIONS,
        socket_options=None,
        client_cert=None,
        proxies_config=None,
        connector_args=None,
    ):
        self._exit_stack = contextlib.AsyncExitStack()

        self._sessions: dict[str | None, aiohttp.ClientSession] | None = None
        self._verify = verify
        self._proxy_config = ProxyConfiguration(
            proxies=proxies, proxies_settings=proxies_config
        )
        if isinstance(timeout, (list, tuple)):
            conn_timeout, read_timeout = timeout
        else:
            conn_timeout = read_timeout = timeout

        timeout = aiohttp.ClientTimeout(
            sock_connect=conn_timeout, sock_read=read_timeout
        )

        self._cert_file = None
        self._key_file = None
        if isinstance(client_cert, str):
            self._cert_file = client_cert
        elif isinstance(client_cert, tuple):
            self._cert_file, self._key_file = client_cert

        self._timeout = timeout
        self._connector_args = connector_args
        if self._connector_args is None:
            self._connector_args = dict(
                keepalive_timeout=DEFAULT_KEEPALIVE_TIMEOUT
            )

        self._max_pool_connections = max_pool_connections
        self._socket_options = socket_options
        if socket_options is None:
            self._socket_options = []


    async def __aenter__(self):
        assert self._sessions is None
        self._sessions = {}

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        assert self._sessions is not None, 'Session was never entered'
        self._sessions.clear()
        await self._exit_stack.aclose()
        self._sessions = None

    def _get_ssl_context(self):
        pass

    def _setup_proxy_ssl_context(self, proxy_url):
        pass

    def _chunked(self, headers):
        transfer_encoding = headers.get('Transfer-Encoding', '')
        if chunked := transfer_encoding.lower() == 'chunked':
            del headers['Transfer-Encoding']
        return chunked or None

    def _build_ssl_context(self, proxy_url):
        pass

    async def _create_connector(self, proxy_url):
        ssl_context = (
            await asyncio.to_thread(self._build_ssl_context, proxy_url)
            if bool(self._verify)
            else None
        )
        return aiohttp.TCPConnector(
            limit=self._max_pool_connections,
            ssl=ssl_context or False,
            **self._connector_args,
        )

    async def _get_session(self, proxy_url):
        if not (session := self._sessions.get(proxy_url)):
            connector = await self._create_connector(proxy_url)
            self._sessions[proxy_url] = (
                session
            ) = await self._exit_stack.enter_async_context(
                aiohttp.ClientSession(
                    connector=connector,
                    timeout=self._timeout,
                    skip_auto_headers={'CONTENT-TYPE'},
                    auto_decompress=False,
                ),
            )

        return session

    async def close(self):
        await self.__aexit__(None, None, None)

    async def send(self, request):
        try:
            proxy_url = self._proxy_config.proxy_url_for(request.url)
            proxy_headers = self._proxy_config.proxy_headers_for(request.url)
            url = request.url
            headers = request.headers
            data = request.body

            if ensure_boolean(
                os.environ.get('BOTO_EXPERIMENTAL__ADD_PROXY_HOST_HEADER', '')
            ):
                host = urlparse(request.url).hostname
                proxy_headers['host'] = host

            headers_ = CIMultiDict(
                (z[0], _text(z[1], encoding='utf-8')) for z in headers.items()
            )

            headers_['Accept-Encoding'] = 'identity'

            if isinstance(data, io.IOBase):
                data = _IOBaseWrapper(data)

            url = URL(url, encoded=True)
            session = await self._get_session(proxy_url)
            response = await session.request(
                request.method,
                url=url,
                chunked=self._chunked(headers_),
                headers=headers_,
                data=data,
                proxy=proxy_url,
                proxy_headers=proxy_headers,
            )

            headers = {
                k.decode('utf-8').lower(): v.decode('utf-8')
                for k, v in response.raw_headers
            }

            http_response = aiobotocore.awsrequest.AioAWSResponse(
                str(response.url), response.status, headers, response
            )

            if not request.stream_output:
                await http_response.content

            return http_response
        except ClientSSLError as e:
            raise SSLError(endpoint_url=request.url, error=e)
        except (ClientProxyConnectionError, ClientHttpProxyError) as e:
            raise ProxyConnectionError(
                proxy_url=mask_proxy_url(proxy_url), error=e
            )
        except (
            ServerDisconnectedError,
            aiohttp.ClientPayloadError,
            aiohttp.http_exceptions.BadStatusLine,
        ) as e:
            raise ConnectionClosedError(
                error=e, request=request, endpoint_url=request.url
            )
        except ServerTimeoutError as e:
            if str(e).lower().startswith('connect'):
                raise ConnectTimeoutError(endpoint_url=request.url, error=e)
            else:
                raise ReadTimeoutError(endpoint_url=request.url, error=e)
        except (
            ClientConnectorError,
            ClientConnectionError,
            socket.gaierror,
        ) as e:
            raise EndpointConnectionError(endpoint_url=request.url, error=e)
        except asyncio.TimeoutError as e:
            raise ReadTimeoutError(endpoint_url=request.url, error=e)
        except CancelledError:
            raise
        except Exception as e:
            message = 'Exception received when sending urllib3 HTTP request'
            logger.debug(message, exc_info=True)
            raise HTTPClientError(error=e)
