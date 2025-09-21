import argparse
import time
import sys
import requests
from pathlib import Path
from urllib.parse import urlunparse

# We'll bypass the OpenAPI client validation by using direct HTTP requests
# import client.openapi_client as openapi_client
# from client.openapi_client.models import SubmitJobRequest


class Client:
    def __init__(self, master_ip_address: str):
        # Ensure the URL has the correct format
        if not master_ip_address.startswith("http"):
            master_ip_address = f"http://{master_ip_address}:8000"

        self.base_url = master_ip_address

    def _make_file_url(self, path: str) -> str:
        """Convert a relative or absolute path to a proper file:// URL"""
        # If it's already a URL, return it
        if path.startswith(("http://", "https://", "file://")):
            return path

        # If it's a special command like 'wordcount', return as is
        if "/" not in path and "\\" not in path:
            return path

        # Convert relative path to absolute
        abs_path = str(Path(path).resolve())

        # Convert to file:// URL
        return urlunparse(("file", "", abs_path, "", "", ""))

    def submit(self, code_url: str, data_url: str, job_name: str = None):
        """Submit job using direct HTTP requests to bypass OpenAPI validation"""
        try:
            # Convert paths to proper URLs
            code_url = self._make_file_url(code_url)
            data_url = self._make_file_url(data_url)

            print("Converted URLs:")
            print(f"Code URL: {code_url}")
            print(f"Data URL: {data_url}")

            # Construct the job submission data
            job_data = {
                "code_url": code_url,
                "data_url": data_url,
                "job_name": job_name or f"job_{int(time.time())}",
            }

            # Make direct HTTP request
            response = requests.post(
                f"{self.base_url}/job/submit",
                json=job_data,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code == 201:
                return response.json()
            else:
                print(f"Job submission failed with status {response.status_code}")
                print(f"Response: {response.text}")
                return None

        except Exception as e:
            print(f"Error submitting job: {e}")
            return None

    def monitor_job(self, job_id: str, timeout: int = 300):
        """Monitor job progress until completion or timeout"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.base_url}/job/status/{job_id}")

                if response.status_code == 200:
                    status_data = response.json()
                    status = status_data.get("status")
                    progress = status_data.get("progress", 0)

                    print(f"Job {job_id}: {status} - {progress:.1f}%")

                    if status in ["completed", "failed"]:
                        return status == "completed"
                else:
                    print(f"Error checking job status: {response.text}")

                time.sleep(5)

            except Exception as e:
                print(f"Error monitoring job: {e}")
                return False

        print(f"Job monitoring timed out after {timeout} seconds")
        return False

    def get_results(self, job_id: str):
        """Get and display job results"""
        try:
            response = requests.get(f"{self.base_url}/job/result/{job_id}")

            if response.status_code == 200:
                result_data = response.json()
                result_url = result_data.get("result_url")

                print(f"\nJob {job_id} completed successfully!")
                print(f"Results available at: {result_url}")

                # If it's a local file URL, try to display results
                if result_url and result_url.startswith("file://"):
                    from pathlib import Path

                    result_dir = Path(result_url[7:])  # Remove "file://" prefix

                    # First try to show the consolidated result.txt file
                    consolidated_file = result_dir.parent / "result.txt"
                    if consolidated_file.exists():
                        print(f"\n🎯 Consolidated Results ({consolidated_file}):")
                        print("=" * 60)
                        try:
                            with open(consolidated_file, "r") as f:
                                lines = f.readlines()
                                total_lines = len(lines)
                                display_lines = min(
                                    20, total_lines
                                )  # Show up to 20 lines

                                for line in lines[:display_lines]:
                                    print(line.strip())

                                if total_lines > display_lines:
                                    print(
                                        f"... ({total_lines - display_lines} more lines)"
                                    )

                                print(f"\n📊 Total results: {total_lines} entries")

                        except Exception as e:
                            print(f"Error reading consolidated file: {e}")

                    # Also show individual partition files for reference
                    if result_dir.exists():
                        print("\n📁 Individual partition files:")
                        partition_files = list(result_dir.glob("part-*.txt"))
                        if partition_files:
                            for i, result_file in enumerate(
                                sorted(partition_files)[:3]
                            ):  # Show first 3
                                print(f"\nFile: {result_file.name} (sample)")
                                print("-" * 30)
                                try:
                                    with open(result_file, "r") as f:
                                        sample_lines = f.readlines()[
                                            :3
                                        ]  # Just show 3 lines per file
                                        for line in sample_lines:
                                            print(line.strip())
                                        if len(f.readlines()) > 3:
                                            print("...")
                                except Exception as e:
                                    print(f"Error reading file: {e}")

                            if len(partition_files) > 3:
                                print(
                                    f"... and {len(partition_files) - 3} more partition files"
                                )
                    else:
                        print(f"Result path not found: {result_dir}")
                else:
                    print(f"Result URL: {result_url}")

            elif response.status_code == 202:
                print(f"Job {job_id} is still running, results not ready yet")
            else:
                print(f"Error getting job results: {response.text}")

        except Exception as e:
            print(f"Error retrieving job results: {e}")


def handle_client(args: argparse.Namespace):
    client = Client(args.ip_address)

    print(f"Submitting job to master at {args.ip_address}")
    print(f"Data URL: {args.data_url}")
    print(f"Code URL: {args.code_url}")

    response = client.submit(args.code_url, args.data_url)

    if response:
        job_id = response.get("job_id")
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
