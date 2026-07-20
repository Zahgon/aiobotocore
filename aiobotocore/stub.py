from botocore.stub import Stubber

from .awsrequest import AioAWSResponse


class AioStubber(Stubber):
    def _add_response(self, method, service_response, expected_params):
        pass

    def add_client_error(
        self,
        method,
        service_error_code='',
        service_message='',
        http_status_code=400,
        service_error_meta=None,
        expected_params=None,
        response_meta=None,
        modeled_fields=None,
    ):
        pass
