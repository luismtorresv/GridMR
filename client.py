import argparse
import mr.openapi_client as openapi_client
from mr.openapi_client.models import SubmitJobRequest


class Client:
    def __init__(self, ip_address: str):
        self.configuration = openapi_client.Configuration(host=ip_address)

    def submit(self, code_url: str, data_url: str):
        with openapi_client.ApiClient(self.configuration) as api_client:
            api_instance = openapi_client.DefaultApi(api_client)
            try:
                api_response = api_instance.submit_job(
                    submit_job_request=SubmitJobRequest(
                        code_url=code_url,
                        data_url=data_url,
                        job_name="Just testing it out",
                    )
                )

                return api_response

            except openapi_client.rest.ApiException as e:
                print(e)


def handle_client(args: argparse.Namespace):
    client = Client(args.ip_address)
    response = client.submit(args.code_url, args.data_url)
    print(response)
