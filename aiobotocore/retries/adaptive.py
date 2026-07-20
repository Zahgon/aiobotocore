
import asyncio
import logging

from botocore.retries import standard, throttling

from botocore.retries.adaptive import RateClocker

from . import bucket

logger = logging.getLogger(__name__)


def register_retry_handler(client):
    clock = bucket.Clock()
    rate_adjustor = throttling.CubicCalculator(
        starting_max_rate=0, start_time=clock.current_time()
    )
    token_bucket = bucket.AsyncTokenBucket(max_rate=1, clock=clock)
    rate_clocker = RateClocker(clock)
    throttling_detector = standard.ThrottlingErrorDetector(
        retry_event_adapter=standard.RetryEventAdapter(),
    )
    limiter = AsyncClientRateLimiter(
        rate_adjustor=rate_adjustor,
        rate_clocker=rate_clocker,
        token_bucket=token_bucket,
        throttling_detector=throttling_detector,
        clock=clock,
    )
    client.meta.events.register(
        'before-send',
        limiter.on_sending_request,
    )
    client.meta.events.register(
        'needs-retry',
        limiter.on_receiving_response,
    )
    return limiter


class AsyncClientRateLimiter:


    _MAX_RATE_ADJUST_SCALE = 2.0

    def __init__(
        self,
        rate_adjustor,
        rate_clocker,
        token_bucket,
        throttling_detector,
        clock,
    ):
        self._rate_adjustor = rate_adjustor
        self._rate_clocker = rate_clocker
        self._token_bucket = token_bucket
        self._throttling_detector = throttling_detector
        self._clock = clock
        self._enabled = False
        self._lock = asyncio.Lock()

    async def on_sending_request(self, request, **kwargs):
        pass

    async def on_receiving_response(self, **kwargs):
        pass
