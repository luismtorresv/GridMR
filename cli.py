"""
Command Line Interface for MapReduce.
This is the single entrypoint.
"""

import argparse

from client import handle_client
# from master import handle_master
from worker import handle_worker


def build_parser() -> argparse.ArgumentParser:
    """
    Builds and returns the parser for the GridMR CLI.
    """
    parser = argparse.ArgumentParser(
        prog="GridMR",
        description="Distributed grid processing system based MapReduce.",
    )

    subparsers = parser.add_subparsers(
        dest="role",
        required=True,
        title="roles",
        description="Available roles for GridMR",
    )

    # Client
    client_parser = subparsers.add_parser(
        "client", help="Run as client (to submit jobs)"
    )
    client_parser.add_argument("ip_address", help="IP address of the master node")
    client_parser.add_argument("data_url", help="URL to the input data")
    client_parser.add_argument("code_url", help="URL to the MapReduce code")

    # Master
    master_parser = subparsers.add_parser(
        "master", help="Run as master (to control jobs)"
    )
    master_parser.add_argument(
        "--port", type=int, default=8000, help="Port for master node (default: 8000)"
    )

    # Worker
    worker_parser = subparsers.add_parser(
        "worker", help="Run as worker (to process jobs)"
    )
    worker_parser.add_argument(
        "worker_type", choices=["mapper", "reducer"], help="Type of worker"
    )
    worker_parser.add_argument("master_ip", help="IP address of the master node")
    worker_parser.add_argument("master_port", type=int, help="Port of the master node")

    return parser


def main() -> None:
    """
    Dispatch based on the worker type.
    """
    parser = build_parser()
    args = parser.parse_args()
    match args.role:
        case "client":
            handle_client(args)
        case "master":
            handle_master(args)
        case "worker":
            handle_worker(args)


if __name__ == "__main__":
    main()
