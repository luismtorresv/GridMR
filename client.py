import argparse

import requests


class Client:
    def __init__(self, ip_address: str):
        self.master_ip_address = ip_address

    def submit(self, code_url: str, data_url: str):
        payload = {
            "code_url": code_url,
            "data_url": data_url,
        }

        requests.get(
            self.master_ip_address,
            json=payload,
            timeout=10,
        )


def handle_client(args: argparse.Namespace):
    raise NotImplementedError


if __name__ == "__main__":
    import openapi_client
    from openapi_client.rest import ApiException
    from pprint import pprint

    # Defining the host is optional and defaults to http://localhost
    # See configuration.py for a list of all supported configuration parameters.
    configuration = openapi_client.Configuration(host="http://localhost")

    # Enter a context with an instance of the API client
    with openapi_client.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = openapi_client.DefaultApi(api_client)
        job_id = "job_id_example"  # str |

        try:
            # Cancel a running job
            api_response = api_instance.cancel_job(job_id)
            print("The response of DefaultApi->cancel_job:\n")
            pprint(api_response)
        except ApiException as e:
            print("Exception when calling DefaultApi->cancel_job: %s\n" % e)
