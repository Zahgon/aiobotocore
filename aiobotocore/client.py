from botocore.auth import resolve_auth_scheme_preference, resolve_auth_type
from botocore.awsrequest import prepare_request_dict
from botocore.client import (
    BaseClient,
    ClientCreator,
    ClientEndpointBridge,
    PaginatorDocstring,
    logger,
    resolve_checksum_context,
)
from botocore.compress import maybe_compress_request
from botocore.discovery import block_endpoint_discovery_required_operations
from botocore.exceptions import OperationNotPageableError, UnknownServiceError
from botocore.history import get_global_history_recorder
from botocore.hooks import first_non_none_response
from botocore.useragent import register_feature_id
from botocore.utils import get_service_module_name
from botocore.waiter import xform_name

from . import waiter
from .args import AioClientArgsCreator
from .context import with_current_context
from .credentials import AioRefreshableCredentials
from .discovery import AioEndpointDiscoveryHandler, AioEndpointDiscoveryManager
from .httpchecksum import apply_request_checksum
from .paginate import AioPaginator
from .retries import adaptive, standard
from .utils import AioS3ExpressIdentityResolver, AioS3RegionRedirectorv2

history_recorder = get_global_history_recorder()


class AioClientCreator(ClientCreator):
    async def create_client(
        self,
        service_name,
        region_name,
        is_secure=True,
        endpoint_url=None,
        verify=None,
        credentials=None,
        scoped_config=None,
        api_version=None,
        client_config=None,
        auth_token=None,
    ):
        responses = await self._event_emitter.emit(
            'choose-service-name', service_name=service_name
        )
        service_name = first_non_none_response(responses, default=service_name)
        service_model = self._load_service_model(service_name, api_version)
        try:
            endpoints_ruleset_data = self._load_service_endpoints_ruleset(
                service_name, api_version
            )
            partition_data = self._loader.load_data('partitions')
        except UnknownServiceError:
            endpoints_ruleset_data = None
            partition_data = None
            logger.info(
                'No endpoints ruleset found for service %s, falling back to '
                'legacy endpoint routing.',
                service_name,
            )

        cls = await self._create_client_class(service_name, service_model)
        region_name, client_config = self._normalize_fips_region(
            region_name, client_config
        )
        if auth := service_model.metadata.get('auth'):
            service_signature_version = resolve_auth_type(auth)
        else:
            service_signature_version = service_model.metadata.get(
                'signatureVersion'
            )
        endpoint_bridge = ClientEndpointBridge(
            self._endpoint_resolver,
            scoped_config,
            client_config,
            service_signing_name=service_model.metadata.get('signingName'),
            config_store=self._config_store,
            service_signature_version=service_signature_version,
        )
        if token := self._evaluate_client_specific_token(
            service_model.signing_name
        ):
            auth_token = token
        client_args = await self._get_client_args(
            service_model,
            region_name,
            is_secure,
            endpoint_url,
            verify,
            credentials,
            scoped_config,
            client_config,
            endpoint_bridge,
            auth_token,
            endpoints_ruleset_data,
            partition_data,
        )
        service_client = cls(**client_args)
        self._register_retries(service_client)
        self._register_s3_events(
            client=service_client,
            endpoint_bridge=None,
            endpoint_url=None,
            client_config=client_config,
            scoped_config=scoped_config,
        )
        self._register_s3express_events(client=service_client)
        self._register_s3_control_events(client=service_client)
        self._register_importexport_events(client=service_client)
        self._register_endpoint_discovery(
            service_client, endpoint_url, client_config
        )
        return service_client

    async def _create_client_class(self, service_name, service_model):
        class_attributes = self._create_methods(service_model)
        py_name_to_operation_name = self._create_name_mapping(service_model)
        class_attributes['_PY_TO_OP_NAME'] = py_name_to_operation_name
        bases = [AioBaseClient]
        service_id = service_model.service_id.hyphenize()
        await self._event_emitter.emit(
            f'creating-client-class.{service_id}',
            class_attributes=class_attributes,
            base_classes=bases,
        )
        class_name = get_service_module_name(service_model)
        cls = type(str(class_name), tuple(bases), class_attributes)
        return cls

    def _register_retries(self, client):
        retry_mode = client.meta.config.retries['mode']
        if retry_mode == 'standard':
            self._register_v2_standard_retries(client)
        elif retry_mode == 'adaptive':
            self._register_v2_standard_retries(client)
            self._register_v2_adaptive_retries(client)
        elif retry_mode == 'legacy':
            self._register_legacy_retries(client)
        else:
            return
        register_feature_id(f'RETRY_MODE_{retry_mode.upper()}')

    def _register_v2_standard_retries(self, client):
        max_attempts = client.meta.config.retries.get('total_max_attempts')
        kwargs = {'client': client}
        if max_attempts is not None:
            kwargs['max_attempts'] = max_attempts
        standard.register_retry_handler(**kwargs)

    def _register_v2_adaptive_retries(self, client):
        adaptive.register_retry_handler(client)

    def _register_legacy_retries(self, client):
        endpoint_prefix = client.meta.service_model.endpoint_prefix
        service_id = client.meta.service_model.service_id
        service_event_name = service_id.hyphenize()

        original_config = self._loader.load_data('_retry')
        if not original_config:
            return

        retries = self._transform_legacy_retries(client.meta.config.retries)
        retry_config = self._retry_config_translator.build_retry_config(
            endpoint_prefix,
            original_config.get('retry', {}),
            original_config.get('definitions', {}),
            retries,
        )

        logger.debug(
            "Registering retry handlers for service: %s",
            client.meta.service_model.service_name,
        )
        handler = self._retry_handler_factory.create_retry_handler(
            retry_config, endpoint_prefix
        )
        unique_id = f'retry-config-{service_event_name}'
        client.meta.events.register(
            f"needs-retry.{service_event_name}", handler, unique_id=unique_id
        )

    def _register_endpoint_discovery(self, client, endpoint_url, config):
        if endpoint_url is not None:
            return
        if client.meta.service_model.endpoint_discovery_operation is None:
            return
        events = client.meta.events
        service_id = client.meta.service_model.service_id.hyphenize()
        enabled = False
        if config and config.endpoint_discovery_enabled is not None:
            enabled = config.endpoint_discovery_enabled
        elif self._config_store:
            enabled = self._config_store.get_config_variable(
                'endpoint_discovery_enabled'
            )

        enabled = self._normalize_endpoint_discovery_config(enabled)
        if enabled and self._requires_endpoint_discovery(client, enabled):
            discover = enabled is True
            manager = AioEndpointDiscoveryManager(
                client, always_discover=discover
            )
            handler = AioEndpointDiscoveryHandler(manager)
            handler.register(events, service_id)
        else:
            events.register(
                'before-parameter-build',
                block_endpoint_discovery_required_operations,
            )

    def _register_s3express_events(
        self,
        client,
        endpoint_bridge=None,
        endpoint_url=None,
        client_config=None,
        scoped_config=None,
    ):
        if client.meta.service_model.service_name != 's3':
            return
        AioS3ExpressIdentityResolver(
            client, AioRefreshableCredentials
        ).register()

    def _register_s3_events(
        self,
        client,
        endpoint_bridge,
        endpoint_url,
        client_config,
        scoped_config,
    ):
        if client.meta.service_model.service_name != 's3':
            return
        AioS3RegionRedirectorv2(None, client).register()
        self._set_s3_presign_signature_version(
            client.meta, client_config, scoped_config
        )
        client.meta.events.register(
            'before-parameter-build.s3', self._inject_s3_input_parameters
        )

    async def _get_client_args(
        self,
        service_model,
        region_name,
        is_secure,
        endpoint_url,
        verify,
        credentials,
        scoped_config,
        client_config,
        endpoint_bridge,
        auth_token,
        endpoints_ruleset_data,
        partition_data,
    ):
        args_creator = AioClientArgsCreator(
            self._event_emitter,
            self._user_agent,
            self._response_parser_factory,
            self._loader,
            self._exceptions_factory,
            config_store=self._config_store,
            user_agent_creator=self._user_agent_creator,
        )
        return await args_creator.get_client_args(
            service_model,
            region_name,
            is_secure,
            endpoint_url,
            verify,
            credentials,
            scoped_config,
            client_config,
            endpoint_bridge,
            auth_token,
            endpoints_ruleset_data,
            partition_data,
        )


class AioBaseClient(BaseClient):
    async def _async_getattr(self, item):
        pass

    def __getattr__(self, item):
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{item}'"
        )

    async def close(self):
        """Closes underlying endpoint connections."""
        await self._endpoint.close()

    @with_current_context()
    async def _make_api_call(self, operation_name, api_params):
        pass

    async def _make_request(
        self, operation_model, request_dict, request_context
    ):
        pass

    async def _convert_to_request_dict(
        self,
        api_params,
        operation_model,
        endpoint_url,
        context=None,
        headers=None,
        set_user_agent_header=True,
    ):
        request_dict = self._serializer.serialize_to_request(
            api_params, operation_model
        )
        if not self._client_config.inject_host_prefix:
            request_dict.pop('host_prefix', None)
        if headers is not None:
            request_dict['headers'].update(headers)
        if set_user_agent_header:
            user_agent = self._user_agent_creator.to_string()
        else:
            user_agent = None
        prepare_request_dict(
            request_dict,
            endpoint_url=endpoint_url,
            user_agent=user_agent,
            context=context,
        )
        return request_dict

    async def _emit_api_params(self, api_params, operation_model, context):
        operation_name = operation_model.name

        service_id = self._service_model.service_id.hyphenize()
        responses = await self.meta.events.emit(
            f'provide-client-params.{service_id}.{operation_name}',
            params=api_params,
            model=operation_model,
            context=context,
        )
        api_params = first_non_none_response(responses, default=api_params)

        await self.meta.events.emit(
            f'before-parameter-build.{service_id}.{operation_name}',
            params=api_params,
            model=operation_model,
            context=context,
        )
        return api_params

    async def _resolve_endpoint_ruleset(
        self,
        operation_model,
        params,
        request_context,
        ignore_signing_region=False,
    ):
        """Returns endpoint URL and list of additional headers returned from
        EndpointRulesetResolver for the given operation and params. If the
        ruleset resolver is not available, for example because the service has
        no endpoints ruleset file, the legacy endpoint resolver's value is
        returned.

        Use ignore_signing_region for generating presigned URLs or any other
        situation where the signing region information from the ruleset
        resolver should be ignored.

        Returns tuple of URL and headers dictionary. Additionally, the
        request_context dict is modified in place with any signing information
        returned from the ruleset resolver.
        """
        if self._ruleset_resolver is None:
            endpoint_url = self.meta.endpoint_url
            additional_headers = {}
            endpoint_properties = {}
        else:
            endpoint_info = await self._ruleset_resolver.construct_endpoint(
                operation_model=operation_model,
                call_args=params,
                request_context=request_context,
            )
            endpoint_url = endpoint_info.url
            additional_headers = endpoint_info.headers
            endpoint_properties = endpoint_info.properties
            auth_schemes = endpoint_info.properties.get('authSchemes')
            if auth_schemes is not None:
                auth_info = self._ruleset_resolver.auth_schemes_to_signing_ctx(
                    auth_schemes
                )
                auth_type, signing_context = auth_info
                request_context['auth_type'] = auth_type
                if 'region' in signing_context and ignore_signing_region:
                    del signing_context['region']
                if 'signing' in request_context:
                    request_context['signing'].update(signing_context)
                else:
                    request_context['signing'] = signing_context

        return endpoint_url, additional_headers, endpoint_properties

    def get_paginator(self, operation_name):
        pass

    def get_waiter(self, waiter_name):
        """Returns an object that can wait for some condition.

        :type waiter_name: str
        :param waiter_name: The name of the waiter to get. See the waiters
            section of the service docs for a list of available waiters.

        :returns: The specified waiter object.
        :rtype: ``botocore.waiter.Waiter``
        """
        config = self._get_waiter_config()
        if not config:
            raise ValueError(f"Waiter does not exist: {waiter_name}")
        model = waiter.WaiterModel(config)
        mapping = {}
        for name in model.waiter_names:
            mapping[xform_name(name)] = name
        if waiter_name not in mapping:
            raise ValueError(f"Waiter does not exist: {waiter_name}")

        return waiter.create_waiter_with_client(
            mapping[waiter_name], model, self
        )

    async def __aenter__(self):
        await self._endpoint.http_session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._endpoint.http_session.__aexit__(exc_type, exc_val, exc_tb)
