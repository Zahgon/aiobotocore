import io

from botocore.httpchecksum import (
    _CHECKSUM_CLS,
    AwsChunkedWrapper,
    FlexibleChecksumError,
    _apply_request_header_checksum,
    base64,
    conditionally_calculate_md5,
    determine_content_length,
    logger,
)

from aiobotocore._helpers import resolve_awaitable
from aiobotocore.response import AioHttpxStreamingBody, AioStreamingBody

try:
    import httpx
except ImportError:
    httpx = None


class AioAwsChunkedWrapper(AwsChunkedWrapper):
    async def read(self, size=None):
        pass

    async def _make_chunk(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        while not self._complete:
            return await self._make_chunk()
        raise StopAsyncIteration()


class _ChecksumMixin:

    def _init_checksum(self, checksum, expected):
        pass

    async def read(self, amt=None):
        pass

    async def readinto(self, b: bytearray):
        pass

    def _validate_checksum(self):
        pass


class AioStreamingChecksumBody(_ChecksumMixin, AioStreamingBody):

    def __init__(self, raw_stream, content_length, checksum, expected):
        super().__init__(raw_stream, content_length)
        self._init_checksum(checksum, expected)


class AioHttpxStreamingChecksumBody(_ChecksumMixin, AioHttpxStreamingBody):

    def __init__(self, raw_stream, content_length, checksum, expected):
        super().__init__(raw_stream, content_length)
        self._init_checksum(checksum, expected)


StreamingChecksumBody = AioStreamingChecksumBody
HttpxStreamingChecksumBody = AioHttpxStreamingChecksumBody


def _handle_streaming_response(http_response, response, algorithm):
    checksum_cls = _CHECKSUM_CLS.get(algorithm)
    header_name = f"x-amz-checksum-{algorithm}"
    if httpx is not None and isinstance(http_response.raw, httpx.Response):
        streaming_cls = AioHttpxStreamingChecksumBody
    else:
        streaming_cls = AioStreamingChecksumBody
    return streaming_cls(
        http_response.raw,
        response["headers"].get("content-length"),
        checksum_cls(),
        response["headers"][header_name],
    )


async def handle_checksum_body(
    http_response, response, context, operation_model
):
    headers = response["headers"]
    checksum_context = context.get("checksum", {})
    algorithms = checksum_context.get("response_algorithms")

    if not algorithms:
        return

    for algorithm in algorithms:
        header_name = f"x-amz-checksum-{algorithm}"
        if header_name not in headers:
            continue

        if "-" in headers[header_name]:
            continue

        if operation_model.has_streaming_output:
            response["body"] = _handle_streaming_response(
                http_response, response, algorithm
            )
        else:
            response["body"] = await _handle_bytes_response(
                http_response, response, algorithm
            )

        checksum_context = response["context"].get("checksum", {})
        checksum_context["response_algorithm"] = algorithm
        response["context"]["checksum"] = checksum_context
        return

    logger.debug(
        'Skipping checksum validation. Response did not contain one of the following algorithms: %s.',
        algorithms,
    )


async def _handle_bytes_response(http_response, response, algorithm):
    body = await http_response.content
    header_name = f"x-amz-checksum-{algorithm}"
    checksum_cls = _CHECKSUM_CLS.get(algorithm)
    checksum = checksum_cls()
    checksum.update(body)
    expected = response["headers"][header_name]
    if checksum.digest() != base64.b64decode(expected):
        error_msg = (
            f"Expected checksum {expected} did not match calculated "
            f"checksum: {checksum.b64digest()}"
        )
        raise FlexibleChecksumError(error_msg=error_msg)
    return body


def apply_request_checksum(request):
    checksum_context = request.get("context", {}).get("checksum", {})
    algorithm = checksum_context.get("request_algorithm")

    if not algorithm:
        return

    if algorithm == "conditional-md5":
        conditionally_calculate_md5(request)
    elif algorithm["in"] == "header":
        _apply_request_header_checksum(request)
    elif algorithm["in"] == "trailer":
        _apply_request_trailer_checksum(request)
    else:
        raise FlexibleChecksumError(
            error_msg="Unknown checksum variant: {}".format(algorithm["in"])
        )
    if "request_algorithm_header" in checksum_context:
        request_algorithm_header = checksum_context["request_algorithm_header"]
        request["headers"][request_algorithm_header["name"]] = (
            request_algorithm_header["value"]
        )


def _apply_request_trailer_checksum(request):
    checksum_context = request.get("context", {}).get("checksum", {})
    algorithm = checksum_context.get("request_algorithm")
    location_name = algorithm["name"]
    checksum_cls = _CHECKSUM_CLS.get(algorithm["algorithm"])

    headers = request["headers"]
    body = request["body"]

    if location_name in headers:
        return

    headers["Transfer-Encoding"] = "chunked"
    if "Content-Encoding" in headers:
        headers["Content-Encoding"] += ",aws-chunked"
    else:
        headers["Content-Encoding"] = "aws-chunked"
    headers["X-Amz-Trailer"] = location_name

    content_length = determine_content_length(body)
    if content_length is None and "Content-Length" in headers:
        content_length = int(headers["Content-Length"])
    if content_length is not None:
        headers["X-Amz-Decoded-Content-Length"] = str(content_length)

    if "Content-Length" in headers:
        del headers["Content-Length"]
        logger.debug(
            "Removing the Content-Length header since 'chunked' is specified for Transfer-Encoding."
        )

    if isinstance(body, (bytes, bytearray)):
        body = io.BytesIO(body)

    request["body"] = AioAwsChunkedWrapper(
        body,
        checksum_cls=checksum_cls,
        checksum_name=location_name,
    )
