import botocore.utils
from botocore.awsrequest import AWSResponse


class AioAWSResponse(AWSResponse):

    async def _content_prop(self):
        pass

    @property
    def content(self):
        pass

    async def _text_prop(self):
        pass

    @property
    def text(self):
        pass


class HttpxAWSResponse(AioAWSResponse):
    async def _content_prop(self):
        pass
