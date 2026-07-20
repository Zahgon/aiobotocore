import asyncio
import contextlib

from botocore import UNSIGNED, translate
from botocore import __version__ as botocore_version
from botocore.context import get_context
from botocore.exceptions import PartialCredentialsError
from botocore.session import (
    EVENT_ALIASES,
    ServiceModel,
    UnknownServiceError,
    copy,
    logger,
)
from botocore.session import Session as _SyncSession
from botocore.useragent import register_feature_id

from . import __version__, retryhandler
from .client import AioBaseClient, AioClientCreator
from .configprovider import AioSmartDefaultsConfigStoreFactory
from .context import with_current_context
from .credentials import AioCredentials, create_credential_resolver
from .hooks import AioHierarchicalEmitter
from .parsers import AioResponseParserFactory
from .tokens import create_token_resolver
from .utils import AioIMDSRegionProvider


class ClientCreatorContext:
    def __init__(self, coro):
        self._coro = coro
        self._client = None

    async def __aenter__(self) -> AioBaseClient:
        self._client = await self._coro
        return await self._client.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)


class AioSession(_SyncSession):
    def __init__(
        self,
        session_vars=None,
        event_hooks=None,
        include_builtin_handlers=True,
        profile=None,
    ):
        if event_hooks is None:
            event_hooks = AioHierarchicalEmitter()

        super().__init__(
            session_vars, event_hooks, include_builtin_handlers, profile
        )

        self._set_user_agent_for_session()

    def _set_user_agent_for_session(self):
        pass

    def _create_token_resolver(self):
        pass

    def _create_credential_resolver(self):
        pass

    def _register_smart_defaults_factory(self):
        pass

    def _register_response_parser_factory(self):
        pass

    def set_credentials(
        self, access_key, secret_key, token=None, account_id=None
    ):
        pass

    async def get_credentials(self):
        if self._credentials is None:
            self._credentials = await self._components.get_component(
                'credential_provider'
            ).load_credentials()
        return self._credentials

    async def get_service_model(self, service_name, api_version=None):
        pass

    async def get_service_data(self, service_name, api_version=None):
        pass

    def warm_up_loader_caches(
        self,
        service_name: str | None = None,
        api_version: str | None = None,
    ):
        pass

    def create_client(self, *args, **kwargs):
        return ClientCreatorContext(self._create_client(*args, **kwargs))

    @with_current_context()
    async def _create_client(
        self,
        service_name,
        region_name=None,
        api_version=None,
        use_ssl=True,
        verify=None,
        endpoint_url=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        config=None,
        aws_account_id=None,
    ):
        default_client_config = self.get_default_client_config()
        if config is not None and default_client_config is not None:
            config = default_client_config.merge(config)
        elif default_client_config is not None:
            config = default_client_config

        region_name = self._resolve_region_name(region_name, config)

        if verify is None:
            verify = self.get_config_variable('ca_bundle')

        if api_version is None:
            api_version = self.get_config_variable('api_versions').get(
                service_name, None
            )

        loader = self.get_component('data_loader')

        if getattr(config, 'warm_up_loader_caches', False):
            await asyncio.to_thread(
                self.warm_up_loader_caches, service_name, api_version
            )

        event_emitter = self.get_component('event_emitter')
        response_parser_factory = self.get_component('response_parser_factory')
        if config is not None and config.signature_version is UNSIGNED:
            credentials = None
        elif (
            aws_access_key_id is not None and aws_secret_access_key is not None
        ):
            credentials = AioCredentials(
                access_key=aws_access_key_id,
                secret_key=aws_secret_access_key,
                token=aws_session_token,
                account_id=aws_account_id,
            )
        elif self._missing_cred_vars(aws_access_key_id, aws_secret_access_key):
            raise PartialCredentialsError(
                provider='explicit',
                cred_var=self._missing_cred_vars(
                    aws_access_key_id, aws_secret_access_key
                ),
            )
        else:
            if ignored_credentials := self._get_ignored_credentials(
                aws_session_token, aws_account_id
            ):
                logger.debug(
                    "Ignoring the following credential-related values which were set without "
                    "an access key id and secret key on the session or client: %s",
                    ignored_credentials,
                )
            credentials = await self.get_credentials()
        if getattr(credentials, 'method', None) == 'explicit':
            register_feature_id('CREDENTIALS_CODE')
        auth_token = self.get_auth_token()
        endpoint_resolver = self._get_internal_component('endpoint_resolver')
        exceptions_factory = self._get_internal_component('exceptions_factory')
        config_store = copy.copy(self.get_component('config_store'))
        user_agent_creator = self.get_component('user_agent_creator')
        user_agent_creator.set_session_config(
            session_user_agent_name=self.user_agent_name,
            session_user_agent_version=self.user_agent_version,
            session_user_agent_extra=self.user_agent_extra,
        )
        defaults_mode = self._resolve_defaults_mode(config, config_store)
        if defaults_mode != 'legacy':
            smart_defaults_factory = self._get_internal_component(
                'smart_defaults_factory'
            )
            await smart_defaults_factory.merge_smart_defaults(
                config_store, defaults_mode, region_name
            )
        self._add_configured_endpoint_provider(
            client_name=service_name,
            config_store=config_store,
        )

        user_agent_creator.set_client_features(get_context().features)

        client_creator = AioClientCreator(
            loader,
            endpoint_resolver,
            self.user_agent(),
            event_emitter,
            retryhandler,
            translate,
            response_parser_factory,
            exceptions_factory,
            config_store,
            user_agent_creator=user_agent_creator,
            auth_token_resolver=self.get_auth_token,
        )
        client = await client_creator.create_client(
            service_name=service_name,
            region_name=region_name,
            is_secure=use_ssl,
            endpoint_url=endpoint_url,
            verify=verify,
            credentials=credentials,
            scoped_config=self.get_scoped_config(),
            client_config=config,
            api_version=api_version,
            auth_token=auth_token,
        )
        monitor = self._get_internal_component('monitor')
        if monitor is not None:
            monitor.register(client.meta.events)
        self._register_client_plugins(client)
        return client

    async def get_available_regions(
        self, service_name, partition_name='aws', allow_non_regional=False
    ):
        pass


def get_session(env_vars=None):
    """
    Return a new session object.
    """
    return AioSession(env_vars)
