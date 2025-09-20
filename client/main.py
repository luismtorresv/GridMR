import argparse

import client.openapi_client as openapi_client
from client.openapi_client.models import SubmitJobRequest


class Client:
    def __init__(self, master_ip_address: str):
        self.configuration = openapi_client.Configuration(host=master_ip_address)
        self.api_client = openapi_client.ApiClient(self.configuration)
        self.api = openapi_client.DefaultApi(self.api_client)

    def submit(self, code_url: str, data_url: str):
        try:
            api_response = self.api.submit_job(
                submit_job_request=SubmitJobRequest(
                    code_url=code_url,
                    data_url=data_url,
                )
            )
            return api_response

        except openapi_client.rest.ApiException as e:
            print(e)


def handle_client(args: argparse.Namespace):
    client = Client(args.ip_address)
    response = client.submit(args.code_url, args.data_url)
