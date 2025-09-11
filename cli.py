import requests
import argparse


class Client:
    def __init__(self, ip_address: str):
        self.master_ip_address = ip_address

    def submit(self, code_url: str, data_url: str):
        payload = {
            "code_url": code_url,
            "data_url": data_url,
        }

        requests.get(self.master_ip_address, json=payload)


def main():
    parser = argparse.ArgumentParser(
        prog="GridMR-client", description="Client version of GridMR."
    )
    parser.add_argument("ip_address")
    parser.add_argument("code_url")
    parser.add_argument("data_url")
    args = parser.parse_args()
    print(args)

    client = Client(args.ip_address)
    client.submit(args.code_url, args.data_url)


if __name__ == "__main__":
    main()
