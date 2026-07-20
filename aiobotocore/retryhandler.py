from botocore.retryhandler import (
    ChecksumError,
    CRC32Checker,
    ExceptionRaiser,
    HTTPStatusCodeChecker,
    MaxAttemptsDecorator,
    MultiChecker,
    RetryHandler,
    ServiceErrorCodeChecker,
    _extract_retryable_exception,
    crc32,
    create_retry_action_from_config,
    logger,
)

from ._helpers import resolve_awaitable


def create_retry_handler(config, operation_name=None):
    checker = create_checker_from_retry_config(
        config, operation_name=operation_name
    )
    action = create_retry_action_from_config(
        config, operation_name=operation_name
    )
    return AioRetryHandler(checker=checker, action=action)


def create_checker_from_retry_config(config, operation_name=None):
    checkers = []
    max_attempts = None
    retryable_exceptions = []
    if '__default__' in config:
        policies = config['__default__'].get('policies', [])
        max_attempts = config['__default__']['max_attempts']
        for key in policies:
            current_config = policies[key]
            checkers.append(_create_single_checker(current_config))
            retry_exception = _extract_retryable_exception(current_config)
            if retry_exception is not None:
                retryable_exceptions.extend(retry_exception)
    if operation_name is not None and config.get(operation_name) is not None:
        operation_policies = config[operation_name]['policies']
        for key in operation_policies:
            checkers.append(_create_single_checker(operation_policies[key]))
            retry_exception = _extract_retryable_exception(
                operation_policies[key]
            )
            if retry_exception is not None:
                retryable_exceptions.extend(retry_exception)
    if len(checkers) == 1:
        return AioMaxAttemptsDecorator(checkers[0], max_attempts=max_attempts)
    else:
        multi_checker = AioMultiChecker(checkers)
        return AioMaxAttemptsDecorator(
            multi_checker,
            max_attempts=max_attempts,
            retryable_exceptions=tuple(retryable_exceptions),
        )


def _create_single_checker(config):
    if 'response' in config['applies_when']:
        return _create_single_response_checker(
            config['applies_when']['response']
        )
    elif 'socket_errors' in config['applies_when']:
        return ExceptionRaiser()


def _create_single_response_checker(response):
    if 'service_error_code' in response:
        checker = ServiceErrorCodeChecker(
            status_code=response['http_status_code'],
            error_code=response['service_error_code'],
        )
    elif 'http_status_code' in response:
        checker = HTTPStatusCodeChecker(
            status_code=response['http_status_code']
        )
    elif 'crc32body' in response:
        checker = AioCRC32Checker(header=response['crc32body'])
    else:
        raise ValueError("Unknown retry policy")
    return checker


class AioRetryHandler(RetryHandler):
    async def _call(self, attempts, response, caught_exception, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self._call(*args, **kwargs)  # return awaitable


class AioMaxAttemptsDecorator(MaxAttemptsDecorator):
    async def _call(
        self, attempt_number, response, caught_exception, retries_context
    ):
        pass

    def __call__(self, *args, **kwargs):
        return self._call(*args, **kwargs)

    async def _should_retry(self, attempt_number, response, caught_exception):
        pass


class AioMultiChecker(MultiChecker):
    async def _call(self, attempt_number, response, caught_exception):
        pass

    def __call__(self, *args, **kwargs):
        return self._call(*args, **kwargs)


class AioCRC32Checker(CRC32Checker):
    async def _call(self, attempt_number, response, caught_exception):
        pass

    def __call__(self, *args, **kwargs):
        return self._call(*args, **kwargs)

    async def _check_response(self, attempt_number, response):
        pass
