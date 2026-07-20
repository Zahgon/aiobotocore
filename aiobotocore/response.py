import asyncio
import contextlib

import aiohttp
import aiohttp.client_exceptions
import botocore.response
from botocore.response import (
    ReadTimeoutError,
    ResponseStreamingError,
)

from aiobotocore import parsers


class AioReadTimeoutError(ReadTimeoutError, asyncio.TimeoutError):
    pass


class AioStreamingBodyBase(
    botocore.response.StreamingBody, contextlib.AbstractAsyncContextManager
):

    _DEFAULT_CHUNK_SIZE = 1024

    @property
    def raw_stream(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    async def readlines(self):
        pass

    def __aiter__(self):
        """Return an async iterator to yield 1k chunks from the raw stream."""
        return self.iter_chunks(self._DEFAULT_CHUNK_SIZE)

    async def __anext__(self):
        """Return the next 1k chunk from the raw stream."""
        current_chunk = await self.read(self._DEFAULT_CHUNK_SIZE)
        if current_chunk:
            return current_chunk
        raise StopAsyncIteration

    anext = __anext__

    async def iter_lines(self, chunk_size=_DEFAULT_CHUNK_SIZE, keepends=False):
        pass

    async def iter_chunks(self, chunk_size=_DEFAULT_CHUNK_SIZE):
        pass

    def tell(self):
        pass

    def __iter__(self):
        raise TypeError(
            f"{type(self).__name__} is async; use 'async for' or __aiter__"
        )

    def __next__(self):
        raise TypeError(f"{type(self).__name__} is async; use __anext__")

    next = __next__

    def __enter__(self):
        raise TypeError(f"{type(self).__name__} is async; use 'async with'")

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise TypeError(f"{type(self).__name__} is async; use 'async with'")

    def set_socket_timeout(self, timeout):
        raise NotImplementedError(
            "set_socket_timeout is not supported for async streaming bodies; "
            "configure timeouts on the aiobotocore Config or HTTP session "
            "instead."
        )


class AioStreamingBody(AioStreamingBodyBase):

    def readable(self):
        pass

    async def read(self, amt=None):
        pass

    async def readinto(self, b: bytearray):
        pass

    def close(self):
        """Close the underlying response stream synchronously."""
        self._raw_stream.close()

    async def aclose(self):
        """Close the underlying response stream asynchronously."""
        self.close()


class AioHttpxStreamingBody(AioStreamingBodyBase):

    def __init__(self, raw_stream, content_length=None):
        super().__init__(raw_stream, content_length)
        self._buffer = b''
        self._stream_iter = None
        self._stream_exhausted = False

    def _ensure_stream(self):
        pass

    async def _fill_buffer(self, min_bytes):
        pass

    async def read(self, amt=None):
        pass

    async def readinto(self, b: bytearray):
        pass

    def readable(self):
        pass

    async def close(self):
        """Close the underlying httpx response (async-only — httpx has no
        synchronous close).
        """
        await self._raw_stream.aclose()

    aclose = close


StreamingBody = AioStreamingBody
HttpxStreamingBody = AioHttpxStreamingBody


async def get_response(operation_model, http_response):
    protocol = operation_model.service_model.resolved_protocol
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status_code,
    }
    if response_dict['status_code'] >= 300:
        response_dict['body'] = await http_response.content
    elif operation_model.has_streaming_output:
        response_dict['body'] = AioStreamingBody(
            http_response.raw, response_dict['headers'].get('content-length')
        )
    else:
        response_dict['body'] = await http_response.content

    parser = parsers.create_parser(protocol)
    parsed = await parser.parse(response_dict, operation_model.output_shape)
    return http_response, parsed
