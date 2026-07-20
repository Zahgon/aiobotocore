import asyncio
import contextlib
import functools
import inspect
import json
import logging

import botocore.awsrequest
from botocore.exceptions import (
    InvalidIMDSEndpointError,
    MetadataRetrievalError,
)
from botocore.utils import (
    DEFAULT_METADATA_SERVICE_TIMEOUT,
    METADATA_BASE_URL,
    RETRYABLE_HTTP_ERRORS,
    ArnParser,
    BadIMDSRequestError,
    ClientError,
    ContainerMetadataFetcher,
    HTTPClientError,
    IdentityCache,
    IMDSFetcher,
    IMDSRegionProvider,
    InstanceMetadataFetcher,
    InstanceMetadataRegionFetcher,
    PluginContext,
    ReadTimeoutError,
    S3ExpressIdentityCache,
    S3ExpressIdentityResolver,
    S3RegionRedirector,
    S3RegionRedirectorv2,
    get_environ_proxies,
    os,
    reset_plugin_context,
    resolve_imds_endpoint_mode,
    set_plugin_context,
    validate_region_name,
)

import aiobotocore.httpsession

logger = logging.getLogger(__name__)


class _RefCountedSession(aiobotocore.httpsession.AIOHTTPSession):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__ref_count = 0
        self.__lock = None

    @contextlib.asynccontextmanager
    async def acquire(self):
        if not self.__lock:
            self.__lock = asyncio.Lock()

        async with self.__lock:
            self.__ref_count += 1

            try:
                if self.__ref_count == 1:
                    await self.__aenter__()
            except BaseException:
                self.__ref_count -= 1
                raise

        try:
            yield self
        finally:
            async with self.__lock:
                if self.__ref_count == 1:
                    await self.__aexit__(None, None, None)

                self.__ref_count -= 1


class AioIMDSFetcher(IMDSFetcher):
    def __init__(
        self,
        timeout=DEFAULT_METADATA_SERVICE_TIMEOUT,  # noqa: E501, lgtm [py/missing-call-to-init]
        num_attempts=1,
        base_url=METADATA_BASE_URL,
        env=None,
        user_agent=None,
        config=None,
        session=None,
    ):
        self._timeout = timeout
        self._num_attempts = num_attempts
        if config is None:
            config = {}
        self._base_url = self._select_base_url(base_url, config)
        self._config = config

        if env is None:
            env = os.environ.copy()
        self._disabled = (
            env.get('AWS_EC2_METADATA_DISABLED', 'false').lower() == 'true'
        )
        self._imds_v1_disabled = config.get('ec2_metadata_v1_disabled')
        self._user_agent = user_agent

        self._session = session or _RefCountedSession(
            timeout=self._timeout,
            proxies=get_environ_proxies(self._base_url),
        )

    async def _fetch_metadata_token(self):
        self._assert_enabled()
        url = self._construct_url(self._TOKEN_PATH)
        headers = {
            'x-aws-ec2-metadata-token-ttl-seconds': self._TOKEN_TTL,
        }
        self._add_user_agent(headers)

        request = botocore.awsrequest.AWSRequest(
            method='PUT', url=url, headers=headers
        )

        async with self._session.acquire() as session:
            for i in range(self._num_attempts):
                try:
                    response = await session.send(request.prepare())
                    if response.status_code == 200:
                        return await response.text
                    elif response.status_code in (404, 403, 405):
                        return None
                    elif response.status_code in (400,):
                        raise BadIMDSRequestError(request)
                except ReadTimeoutError:
                    return None
                except RETRYABLE_HTTP_ERRORS as e:
                    logger.debug(
                        "Caught retryable HTTP exception while making metadata "
                        "service request to %s: %s",
                        url,
                        e,
                        exc_info=True,
                    )
                except HTTPClientError as e:
                    error = e.kwargs.get('error')
                    if (
                        error
                        and getattr(error, 'errno', None) == 8
                        or str(getattr(error, 'os_error', None))
                        == 'Domain name not found'
                    ):  # threaded vs async resolver
                        raise InvalidIMDSEndpointError(endpoint=url, error=e)
                    else:
                        raise

        return None

    async def _get_request(self, url_path, retry_func, token=None):
        self._assert_enabled()
        if not token:
            self._assert_v1_enabled()
        if retry_func is None:
            retry_func = self._default_retry
        url = self._construct_url(url_path)
        headers = {}
        if token is not None:
            headers['x-aws-ec2-metadata-token'] = token
        self._add_user_agent(headers)

        async with self._session.acquire() as session:
            for i in range(self._num_attempts):
                try:
                    request = botocore.awsrequest.AWSRequest(
                        method='GET', url=url, headers=headers
                    )
                    response = await session.send(request.prepare())
                    should_retry = retry_func(response)
                    if inspect.isawaitable(should_retry):
                        should_retry = await should_retry

                    if not should_retry:
                        return response
                except RETRYABLE_HTTP_ERRORS as e:
                    logger.debug(
                        "Caught retryable HTTP exception while making metadata "
                        "service request to %s: %s",
                        url,
                        e,
                        exc_info=True,
                    )
        raise self._RETRIES_EXCEEDED_ERROR_CLS()

    async def _default_retry(self, response):
        pass

    async def _is_non_ok_response(self, response):
        pass

    async def _is_empty(self, response):
        pass

    async def _log_imds_response(
        self, response, reason_to_log, log_body=False
    ):
        pass


class AioInstanceMetadataFetcher(AioIMDSFetcher, InstanceMetadataFetcher):
    async def retrieve_iam_role_credentials(self):
        try:
            token = await self._fetch_metadata_token()
            role_name = await self._get_iam_role(token)
            credentials = await self._get_credentials(role_name, token)
            if self._contains_all_credential_fields(credentials):
                credentials = {
                    'role_name': role_name,
                    'access_key': credentials['AccessKeyId'],
                    'secret_key': credentials['SecretAccessKey'],
                    'token': credentials['Token'],
                    'expiry_time': credentials['Expiration'],
                }
                self._evaluate_expiration(credentials)
                return credentials
            else:
                if 'Code' in credentials and 'Message' in credentials:
                    logger.debug(
                        'Error response received when retrieving'
                        'credentials: %s.',
                        credentials,
                    )
                return {}
        except self._RETRIES_EXCEEDED_ERROR_CLS:
            logger.debug(
                "Max number of attempts exceeded (%s) when "
                "attempting to retrieve data from metadata service.",
                self._num_attempts,
            )
        except BadIMDSRequestError as e:
            logger.debug("Bad IMDS request: %s", e.request)
        return {}

    async def _get_iam_role(self, token=None):
        return await (
            await self._get_request(
                url_path=self._URL_PATH,
                retry_func=self._needs_retry_for_role_name,
                token=token,
            )
        ).text

    async def _get_credentials(self, role_name, token=None):
        r = await self._get_request(
            url_path=self._URL_PATH + role_name,
            retry_func=self._needs_retry_for_credentials,
            token=token,
        )
        return json.loads(await r.text)

    async def _is_invalid_json(self, response):
        pass

    async def _needs_retry_for_role_name(self, response):
        pass

    async def _needs_retry_for_credentials(self, response):
        pass


class AioIMDSRegionProvider(IMDSRegionProvider):
    async def provide(self):
        """Provide the region value from IMDS."""
        instance_region = await self._get_instance_metadata_region()
        return instance_region

    async def _get_instance_metadata_region(self):
        fetcher = self._get_fetcher()
        region = await fetcher.retrieve_region()
        return region

    def _create_fetcher(self):
        metadata_timeout = self._session.get_config_variable(
            'metadata_service_timeout'
        )
        metadata_num_attempts = self._session.get_config_variable(
            'metadata_service_num_attempts'
        )
        imds_config = {
            'ec2_metadata_service_endpoint': self._session.get_config_variable(
                'ec2_metadata_service_endpoint'
            ),
            'ec2_metadata_service_endpoint_mode': resolve_imds_endpoint_mode(
                self._session
            ),
            'ec2_metadata_v1_disabled': self._session.get_config_variable(
                'ec2_metadata_v1_disabled'
            ),
        }
        fetcher = AioInstanceMetadataRegionFetcher(
            timeout=metadata_timeout,
            num_attempts=metadata_num_attempts,
            env=self._environ,
            user_agent=self._session.user_agent(),
            config=imds_config,
        )
        return fetcher


class AioInstanceMetadataRegionFetcher(
    AioIMDSFetcher, InstanceMetadataRegionFetcher
):
    async def retrieve_region(self):
        try:
            region = await self._get_region()
            return region
        except self._RETRIES_EXCEEDED_ERROR_CLS:
            logger.debug(
                "Max number of attempts exceeded (%s) when "
                "attempting to retrieve data from metadata service.",
                self._num_attempts,
            )
        return None

    async def _get_region(self):
        token = await self._fetch_metadata_token()
        response = await self._get_request(
            url_path=self._URL_PATH,
            retry_func=self._default_retry,
            token=token,
        )
        availability_zone = await response.text
        region = availability_zone[:-1]
        return region


class AioIdentityCache(IdentityCache):
    async def get_credentials(self, **kwargs):
        callback = self.build_refresh_callback(**kwargs)
        metadata = await callback()
        credential_entry = self._credential_cls.create_from_metadata(
            metadata=metadata,
            refresh_using=callback,
            method=self.METHOD,
            advisory_timeout=45,
            mandatory_timeout=10,
        )
        return credential_entry


class AioS3ExpressIdentityCache(AioIdentityCache, S3ExpressIdentityCache):
    @functools.lru_cache(maxsize=100)
    def _get_credentials(self, bucket):
        return asyncio.create_task(super().get_credentials(bucket=bucket))

    async def get_credentials(self, bucket):

        return await self._get_credentials(bucket=bucket)

    def build_refresh_callback(self, bucket):
        async def refresher():
            pass

        return refresher


class AioS3ExpressIdentityResolver(S3ExpressIdentityResolver):
    def __init__(self, client, credential_cls, cache=None):
        super().__init__(client, credential_cls, cache)

        if cache is None:
            cache = AioS3ExpressIdentityCache(self._client, credential_cls)
        self._cache = cache


class AioS3RegionRedirectorv2(S3RegionRedirectorv2):
    async def redirect_from_error(
        self,
        request_dict,
        response,
        operation,
        **kwargs,
    ):
        pass

    async def get_bucket_region(self, bucket, response):
        pass


class AioS3RegionRedirector(S3RegionRedirector):
    async def redirect_from_error(
        self, request_dict, response, operation, **kwargs
    ):
        pass

    async def get_bucket_region(self, bucket, response):
        pass


class AioContainerMetadataFetcher(ContainerMetadataFetcher):
    def __init__(self, session=None, sleep=asyncio.sleep):  # noqa: E501, lgtm [py/missing-call-to-init]
        if session is None:
            session = _RefCountedSession(timeout=self.TIMEOUT_SECONDS)
        self._session = session
        self._sleep = sleep

    async def retrieve_full_uri(self, full_url, headers=None):
        self._validate_allowed_url(full_url)
        return await self._retrieve_credentials(full_url, headers)

    async def retrieve_uri(self, relative_uri):
        pass

    async def _retrieve_credentials(self, full_url, extra_headers=None):
        headers = {'Accept': 'application/json'}
        if extra_headers is not None:
            headers.update(extra_headers)
        attempts = 0
        while True:
            try:
                return await self._get_response(
                    full_url, headers, self.TIMEOUT_SECONDS
                )
            except MetadataRetrievalError as e:
                logger.debug(
                    "Received error when attempting to retrieve "
                    "container metadata: %s",
                    e,
                    exc_info=True,
                )
                await self._sleep(self.SLEEP_TIME)
                attempts += 1
                if attempts >= self.RETRY_ATTEMPTS:
                    raise

    async def _get_response(self, full_url, headers, timeout):
        try:
            async with self._session.acquire() as session:
                AWSRequest = botocore.awsrequest.AWSRequest
                request = AWSRequest(
                    method='GET', url=full_url, headers=headers
                )
                response = await session.send(request.prepare())
                response_text = (await response.content).decode('utf-8')

                if response.status_code != 200:
                    raise MetadataRetrievalError(
                        error_msg=(
                            f"Received non 200 response {response.status_code} "
                            f"from container metadata: {response_text}"
                        )
                    )
                try:
                    return json.loads(response_text)
                except ValueError:
                    error_msg = "Unable to parse JSON returned from container metadata services"
                    logger.debug('%s:%s', error_msg, response_text)
                    raise MetadataRetrievalError(error_msg=error_msg)

        except RETRYABLE_HTTP_ERRORS as e:
            error_msg = (
                "Received error when attempting to retrieve "
                f"container metadata: {e}"
            )
            raise MetadataRetrievalError(error_msg=error_msg)


@contextlib.asynccontextmanager
async def create_nested_client(session, service_name, **kwargs):
    """Create a nested client with plugin context disabled.

    If a client is created from within a plugin based on the environment variable,
    an infinite loop could arise. Any clients created from within another client
    must use this method to prevent infinite loops.

    This is the async version of botocore.utils.create_nested_client that works
    with aiobotocore's async session.

    Usage:
        async with create_nested_client(session, 'sts', region_name='us-east-1') as client:
            response = await client.assume_role(...)
    """
    ctx = PluginContext(plugins="DISABLED")
    token = set_plugin_context(ctx)

    try:
        async with session.create_client(service_name, **kwargs) as client:
            reset_plugin_context(token)
            token = None

            yield client
    finally:
        if token:
            reset_plugin_context(token)
