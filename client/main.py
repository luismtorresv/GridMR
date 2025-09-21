import argparse
import time
import sys

import client.openapi_client as openapi_client
from client.openapi_client.models import SubmitJobRequest


class Client:
    def __init__(self, master_ip_address: str):
        # Ensure the URL has the correct format
        if not master_ip_address.startswith("http"):
            master_ip_address = f"http://{master_ip_address}:8000"

        self.configuration = openapi_client.Configuration(host=master_ip_address)
        self.api_client = openapi_client.ApiClient(self.configuration)
        self.api = openapi_client.DefaultApi(self.api_client)

    def submit(self, code_url: str, data_url: str, job_name: str = None):
        try:
            api_response = self.api.submit_job(
                submit_job_request=SubmitJobRequest(
                    code_url=code_url,
                    data_url=data_url,
                    job_name=job_name or f"job_{int(time.time())}",
                )
            )
            return api_response

        except openapi_client.rest.ApiException as e:
            print(f"Error submitting job: {e}")
            return None

    def monitor_job(self, job_id: str, timeout: int = 300):
        """Monitor job progress until completion or timeout"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                status_response = self.api.get_job_status(job_id)
                status = status_response.status
                progress = getattr(status_response, "progress", 0)

                print(f"Job {job_id}: {status} - {progress:.1f}%")

                if status in ["completed", "failed"]:
                    return status == "completed"

                time.sleep(5)

            except openapi_client.rest.ApiException as e:
                print(f"Error checking job status: {e}")
                return False

        print(f"Job monitoring timed out after {timeout} seconds")
        return False

    def get_results(self, job_id: str):
        """Get and display job results"""
        try:
            result_response = self.api.get_job_result(job_id)
            result_url = result_response.result_url

            print(f"\nJob {job_id} completed successfully!")
            print(f"Results available at: {result_url}")

            # If it's a local file URL, try to display some results
            if result_url and result_url.startswith("file://"):
                from pathlib import Path

                result_path = Path(result_url[7:])  # Remove "file://" prefix

                if result_path.exists():
                    print("\nSample results:")
                    print("=" * 50)

                    for result_file in result_path.glob("*.txt"):
                        print(f"\nFile: {result_file.name}")
                        print("-" * 30)
                        try:
                            with open(result_file, "r") as f:
                                lines = f.readlines()
                                for line in lines[:10]:  # Show first 10 lines
                                    print(line.strip())
                                if len(lines) > 10:
                                    print("... (truncated)")
                        except Exception as e:
                            print(f"Error reading file: {e}")

        except openapi_client.rest.ApiException as e:
            print(f"Error getting job results: {e}")


def handle_client(args: argparse.Namespace):
    client = Client(args.ip_address)

    print(f"Submitting job to master at {args.ip_address}")
    print(f"Data URL: {args.data_url}")
    print(f"Code URL: {args.code_url}")

    response = client.submit(args.code_url, args.data_url)

    if response:
        job_id = response.job_id
        print(f"Job submitted successfully! Job ID: {job_id}")

        # Monitor the job
        print("Monitoring job progress...")
        success = client.monitor_job(job_id)

        if success:
            client.get_results(job_id)
        else:
            print("Job failed or timed out")
            sys.exit(1)
    else:
        print("Failed to submit job")
        sys.exit(1)
