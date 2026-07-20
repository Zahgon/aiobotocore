import asyncio
import logging
from datetime import timedelta

import dateutil.parser
from botocore import UNSIGNED
from botocore.compat import total_seconds
from botocore.exceptions import ClientError, TokenRetrievalError
from botocore.tokens import (
    DeferredRefreshableToken,
    FrozenAuthToken,
    ScopedEnvTokenProvider,
    SSOTokenProvider,
    TokenProviderChain,
    _utc_now,
)

from aiobotocore.config import AioConfig
from aiobotocore.utils import create_nested_client

logger = logging.getLogger(__name__)


def create_token_resolver(session):
    providers = [
        ScopedEnvTokenProvider(session),
        AioSSOTokenProvider(session),
    ]
    return TokenProviderChain(providers=providers)


class AioDeferredRefreshableToken(DeferredRefreshableToken):
    def __init__(self, method, refresh_using, time_fetcher=_utc_now):  # noqa: E501, lgtm [py/missing-call-to-init]
        self._time_fetcher = time_fetcher
        self._refresh_using = refresh_using
        self.method = method

        self._refresh_lock = asyncio.Lock()
        self._frozen_token = None
        self._next_refresh = None

    async def get_frozen_token(self):
        await self._refresh()
        return self._frozen_token

    async def _refresh(self):
        refresh_type = self._should_refresh()
        if not refresh_type:
            return None

        block_for_refresh = refresh_type == "mandatory"
        if block_for_refresh or not self._refresh_lock.locked():
            async with self._refresh_lock:
                await self._protected_refresh()

    async def _protected_refresh(self):
        refresh_type = self._should_refresh()
        if not refresh_type:
            return None

        try:
            now = self._time_fetcher()
            self._next_refresh = now + timedelta(seconds=self._attempt_timeout)
            self._frozen_token = await self._refresh_using()
        except Exception:
            logger.warning(
                "Refreshing token failed during the %s refresh period.",
                refresh_type,
                exc_info=True,
            )
            if refresh_type == "mandatory":
                raise

        if self._is_expired():
            raise TokenRetrievalError(
                provider=self.method,
                error_msg="Token has expired and refresh failed",
            )


class AioSSOTokenProvider(SSOTokenProvider):
    async def _attempt_create_token(self, token):
        pass

    async def _refresh_access_token(self, token):
        pass

    async def _refresher(self):
        pass

    @property
    def _client(self):
        pass

    def load_token(self, **kwargs):
        if self._sso_config is None:
            return None

        return AioDeferredRefreshableToken(
            self.METHOD, self._refresher, time_fetcher=self._now
        )
