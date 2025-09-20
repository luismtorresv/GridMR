import argparse

import client.openapi_client as openapi_client
from client.openapi_client.models import SubmitJobRequest


class Worker:

    def __init__(self, master_ip_adress: str):
        self.configuration = openapi_client.Configuration(host=master_ip_adress)

    def start_worker(self):
        pass


def handle_worker(args: argparse.Namespace):
    worker = Worker(args.master_ip)

    raise NotImplementedError
