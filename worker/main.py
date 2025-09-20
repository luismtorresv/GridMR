import argparse

import client.openapi_client as openapi_client
from client.openapi_client.models import RegisterWorkerRequest


class Worker:

    def __init__(self, master_ip: str, master_port: int, worker_type: str):
        # Master connection setup
        master_url = f"http://{master_ip}:{master_port}"
        self.configuration = openapi_client.Configuration(host=master_url)
        self.api_client = openapi_client.ApiClient(self.configuration)
        self.api = openapi_client.DefaultApi(self.api_client)
        self.worker_type = worker_type
        
    def start_worker(self):
        """Register this worker with the master"""
        try:
            # Send registration request
            api_response = self.api.register_worker(
                RegisterWorkerRequest(
                    worker_type=self.worker_type,
            ))

            return api_response
            
        except openapi_client.ApiException as e:
            raise e


def handle_worker(args: argparse.Namespace):
    worker = Worker(
        master_ip=args.master_ip,
        master_port=args.master_port,
        worker_type=args.worker_type
    )
    worker.start_worker()
