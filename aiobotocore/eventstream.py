from botocore.eventstream import (
    EventStream,
    EventStreamBuffer,
    NoInitialResponseError,
)
from botocore.exceptions import EventStreamError


class AioEventStream(EventStream):
    def __iter__(self):
        raise NotImplementedError('Use async-for instead')

    def __aiter__(self):
        return self.__anext__()

    async def __anext__(self):
        async for event in self._event_generator:
            parsed_event = await self._parse_event(event)
            if parsed_event:
                yield parsed_event

    async def _create_raw_event_generator(self):
        pass

    async def _parse_event(self, event):
        pass

    async def get_initial_response(self):
        try:
            async for event in self._event_generator:
                event_type = event.headers.get(':event-type')
                if event_type == 'initial-response':
                    return event

                break
        except StopIteration:
            pass
        raise NoInitialResponseError()

