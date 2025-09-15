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
