from botocore.retries.special import RetryDDBChecksumError, crc32, logger


class AioRetryDDBChecksumError(RetryDDBChecksumError):
    async def is_retryable(self, context):
        pass
