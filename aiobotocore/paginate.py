from functools import partial

import aioitertools
import jmespath
from botocore.exceptions import PaginationError
from botocore.paginate import PageIterator, Paginator
from botocore.useragent import register_feature_id
from botocore.utils import merge_dicts, set_value_from_jmespath

from .context import with_current_context


class AioPageIterator(PageIterator):
    def __aiter__(self):
        return self.__anext__()

    @with_current_context(partial(register_feature_id, 'PAGINATOR'))
    async def _make_request(self, current_kwargs):
        pass

    async def __anext__(self):
        current_kwargs = self._op_kwargs
        previous_next_token = None
        next_token = {key: None for key in self._input_token}
        if self._starting_token is not None:
            next_token = self._parse_starting_token()[0]
        total_items = 0
        first_request = True
        primary_result_key = self.result_keys[0]
        starting_truncation = 0
        self._inject_starting_params(current_kwargs)

        while True:
            response = await self._make_request(current_kwargs)
            parsed = self._extract_parsed_response(response)
            if first_request:
                if self._starting_token is not None:
                    starting_truncation = self._handle_first_request(
                        parsed, primary_result_key, starting_truncation
                    )
                first_request = False
                self._record_non_aggregate_key_values(parsed)
            else:
                starting_truncation = 0
            current_response = primary_result_key.search(parsed)
            if current_response is None:
                current_response = []
            num_current_response = len(current_response)
            truncate_amount = 0
            if self._max_items is not None:
                truncate_amount = (
                    total_items + num_current_response - self._max_items
                )

            if truncate_amount > 0:
                self._truncate_response(
                    parsed,
                    primary_result_key,
                    truncate_amount,
                    starting_truncation,
                    next_token,
                )
                yield response
                break
            else:
                yield response
                total_items += num_current_response
                next_token = self._get_next_token(parsed)
                if all(t is None for t in next_token.values()):
                    break
                if (
                    self._max_items is not None
                    and total_items == self._max_items
                ):
                    self.resume_token = next_token
                    break
                if (
                    previous_next_token is not None
                    and previous_next_token == next_token
                ):
                    message = (
                        f"The same next token was received twice: {next_token}"
                    )
                    raise PaginationError(message=message)
                self._inject_token_into_kwargs(current_kwargs, next_token)
                previous_next_token = next_token

    async def search(self, expression):
        pass

    def result_key_iters(self):
        pass

    async def build_full_result(self):
        pass


class AioPaginator(Paginator):
    PAGE_ITERATOR_CLS = AioPageIterator


class ResultKeyIterator:

    def __init__(self, pages_iterator, result_key):
        self._pages_iterator = pages_iterator
        self.result_key = result_key

    def __aiter__(self):
        return self.__anext__()

    async def __anext__(self):
        async for page in self._pages_iterator:
            results = self.result_key.search(page)
            if results is None:
                results = []
            for result in results:
                yield result  # yield from not avail from async func
