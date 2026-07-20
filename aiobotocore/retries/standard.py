import asyncio

from botocore.retries.standard import (
    _SERVICE_MAX_ATTEMPTS,
    DEFAULT_MAX_ATTEMPTS,
    NEW_RETRIES_ENABLED,
    ExponentialBackoff,
    MaxAttemptsChecker,
    ModeledRetryableChecker,
    OrRetryChecker,
    RetryEventAdapter,
    RetryHandler,
    RetryPolicy,
    RetryQuotaChecker,
    StandardRetryConditions,
    ThrottledRetryableChecker,
    ThrottlingErrorDetector,
    TransientRetryableChecker,
    logger,
    quota,
    special,
)

from .._helpers import async_any, resolve_awaitable
from .special import AioRetryDDBChecksumError


def register_retry_handler(client, max_attempts=None):
    service_id = client.meta.service_model.service_id
    service_event_name = service_id.hyphenize()
    retry_event_adapter = RetryEventAdapter()

    if NEW_RETRIES_ENABLED:
        if (
            max_attempts is None
            and service_event_name in _SERVICE_MAX_ATTEMPTS
        ):
            max_attempts = _SERVICE_MAX_ATTEMPTS[service_event_name]
        elif max_attempts is None:
            max_attempts = DEFAULT_MAX_ATTEMPTS
        throttling_detector = ThrottlingErrorDetector(retry_event_adapter)
        retry_quota = RetryQuotaChecker(
            quota.RetryQuota(), throttling_detector
        )
        handler = AioRetryHandler(
            retry_policy=AioRetryPolicy(
                retry_checker=AioStandardRetryConditions(
                    max_attempts=max_attempts
                ),
                retry_backoff=ExponentialBackoff(
                    service_name=service_event_name,
                    throttling_detector=throttling_detector,
                ),
            ),
            retry_event_adapter=retry_event_adapter,
            retry_quota=retry_quota,
            service_name=service_event_name,
        )
    else:
        retry_quota = RetryQuotaChecker(quota.RetryQuota())
        handler = AioRetryHandler(
            retry_policy=AioRetryPolicy(
                retry_checker=AioStandardRetryConditions(
                    max_attempts=max_attempts or DEFAULT_MAX_ATTEMPTS
                ),
                retry_backoff=ExponentialBackoff(),
            ),
            retry_event_adapter=retry_event_adapter,
            retry_quota=retry_quota,
        )

    client.meta.events.register(
        f'after-call.{service_event_name}', retry_quota.release_retry_quota
    )
    unique_id = f'retry-config-{service_event_name}'
    client.meta.events.register(
        f'needs-retry.{service_event_name}',
        handler.needs_retry,
        unique_id=unique_id,
    )
    return handler


class AioRetryHandler(RetryHandler):
    async def needs_retry(self, **kwargs):
        pass


class AioRetryPolicy(RetryPolicy):
    async def should_retry(self, context):
        pass


class AioStandardRetryConditions(StandardRetryConditions):
    def __init__(self, max_attempts=DEFAULT_MAX_ATTEMPTS):  # noqa: E501, lgtm [py/missing-call-to-init]
        self._max_attempts_checker = MaxAttemptsChecker(max_attempts)
        self._additional_checkers = AioOrRetryChecker(
            [
                TransientRetryableChecker(),
                ThrottledRetryableChecker(),
                ModeledRetryableChecker(),
                AioOrRetryChecker(
                    [
                        special.RetryIDPCommunicationError(),
                        AioRetryDDBChecksumError(),
                    ]
                ),
            ]
        )

    async def is_retryable(self, context):
        pass


class AioOrRetryChecker(OrRetryChecker):
    async def is_retryable(self, context):
        pass
