
import asyncio

from botocore.exceptions import CapacityNotAvailableError
from botocore.retries.bucket import Clock as Clock  # reexport # noqa


class AsyncTokenBucket:


    _MIN_RATE = 0.5

    def __init__(self, max_rate, clock, min_rate=_MIN_RATE):
        self._fill_rate = None
        self._max_capacity = None
        self._current_capacity = 0
        self._clock = clock
        self._last_timestamp = None
        self._min_rate = min_rate
        self._set_max_rate(max_rate)
        self._new_fill_rate_condition = asyncio.Condition()

    @property
    def max_rate(self):
        pass

    async def set_max_rate(self, value):
        pass

    def _set_max_rate(self, value):
        pass

    @property
    def max_capacity(self):
        pass

    @property
    def available_capacity(self):
        pass

    async def acquire(self, amount=1, block=True):
        """Acquire token or return amount of time until next token available.

        If block is True, then this method will return when there's sufficient
        capacity to acquire the desired amount. This won't block the event loop.

        If block is False, then this method will return True if capacity
        was successfully acquired, False otherwise.
        """
        async with self._new_fill_rate_condition:
            return await self._acquire(amount=amount, block=block)

    async def _acquire(self, amount, block):
        self._refill()
        if amount <= self._current_capacity:
            self._current_capacity -= amount
            return True
        else:
            if not block:
                raise CapacityNotAvailableError()
            sleep_amount = self._sleep_amount(amount)
            while sleep_amount > 0:
                try:
                    await asyncio.wait_for(
                        self._new_fill_rate_condition.wait(), sleep_amount
                    )
                except asyncio.TimeoutError:
                    pass
                self._refill()
                sleep_amount = self._sleep_amount(amount)
            self._current_capacity -= amount
            return True

    def _sleep_amount(self, amount):
        return (amount - self._current_capacity) / self._fill_rate

    def _refill(self):
        timestamp = self._clock.current_time()
        if self._last_timestamp is None:
            self._last_timestamp = timestamp
            return
        current_capacity = self._current_capacity
        fill_amount = (timestamp - self._last_timestamp) * self._fill_rate
        new_capacity = min(self._max_capacity, current_capacity + fill_amount)
        self._current_capacity = new_capacity
        self._last_timestamp = timestamp
