#!/usr/bin/env python3
"""
Comprehensive test script for the GridMR MapReduce system.
This script demonstrates how to set up and test the entire distributed system.
"""

import sys
import time
import requests
import subprocess
from pathlib import Path


class GridMRTester:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.test_data_dir = self.base_dir / "test_folder"
        self.master_process = None
        self.worker_processes = []
        self.master_url = "http://localhost:8000"

    def setup_test_data(self):
        """Create test data files if they don't exist"""
        self.test_data_dir.mkdir(exist_ok=True)

        # Sample text files for testing
        test_files = {
            "test01.txt": """
            The quick brown fox jumps over the lazy dog.
            Pack my box with five dozen liquor jugs.
            How vexingly quick daft zebras jump!
            """,
            "test02.txt": """
            MapReduce is a programming model and an associated implementation
            for processing and generating large data sets with a parallel,
            distributed algorithm on a cluster.
            """,
            "test03.txt": """
            Python is a high-level, interpreted programming language with
            dynamic semantics. Its high-level built in data structures,
            combined with dynamic typing make it very attractive.
            """,
        }

        for filename, content in test_files.items():
            file_path = self.test_data_dir / filename
            if not file_path.exists():
                with open(file_path, "w") as f:
                    f.write(content.strip())
                print(f"Created test file: {file_path}")

    def start_master(self):
        """Start the master node"""
        print("Starting master node...")
        cmd = [sys.executable, "cli.py", "master", "--port", "8000"]
        self.master_process = subprocess.Popen(
            cmd, cwd=self.base_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for master to start
        for _ in range(30):  # Wait up to 30 seconds
            try:
                response = requests.get(f"{self.master_url}/")
                if response.status_code == 200:
                    print("Master node started successfully")
                    return True
            except requests.exceptions.ConnectionError:
                time.sleep(1)

        print("Failed to start master node")
        return False

    def start_workers(self, num_workers=2):
        """Start worker nodes"""
        print(f"Starting {num_workers} worker nodes...")

        for i in range(num_workers):
            port = 8001 + i
            cmd = [
                sys.executable,
                "cli.py",
                "worker",
                "localhost",
                "8000",
                "--port",
                str(port),
            ]

            worker_process = subprocess.Popen(
                cmd, cwd=self.base_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            self.worker_processes.append(worker_process)

            # Wait a bit for worker to start and register
            time.sleep(2)

        print(f"Started {len(self.worker_processes)} workers")

    def submit_job(self, job_type="wordcount"):
        """Submit a MapReduce job"""
        print(f"Submitting {job_type} job...")

        job_data = {
            "data_url": f"file://{self.test_data_dir}",
            "code_url": job_type,  # This maps to our example jobs
            "job_name": f"test_{job_type}_job",
        }

        try:
            response = requests.post(
                f"{self.master_url}/job/submit", json=job_data, timeout=30
            )

            if response.status_code == 201:
                result = response.json()
                job_id = result.get("job_id")
                print(f"Job submitted successfully. Job ID: {job_id}")
                return job_id
            else:
                print(f"Failed to submit job: {response.text}")
                return None

        except Exception as e:
            print(f"Error submitting job: {e}")
            return None

    def monitor_job(self, job_id, timeout=120):
        """Monitor job progress until completion"""
        print(f"Monitoring job {job_id}...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.master_url}/job/status/{job_id}")

                if response.status_code == 200:
                    status_data = response.json()
                    status = status_data.get("status")
                    progress = status_data.get("progress", 0)

                    print(f"Job {job_id}: {status} - {progress:.1f}%")

                    if status == "completed":
                        print(f"Job {job_id} completed successfully!")
                        return True
                    elif status == "failed":
                        print(f"Job {job_id} failed!")
                        return False

                else:
                    print(f"Error checking job status: {response.text}")

            except Exception as e:
                print(f"Error monitoring job: {e}")

            time.sleep(5)

        print(f"Job {job_id} monitoring timed out")
        return False

    def get_job_results(self, job_id):
        """Get and display job results"""
        try:
            response = requests.get(f"{self.master_url}/job/result/{job_id}")

            if response.status_code == 200:
                result_data = response.json()
                result_url = result_data.get("result_url")

                if result_url and result_url.startswith("file://"):
                    result_path = Path(result_url[7:])  # Remove "file://" prefix

                    if result_path.exists():
                        print(f"\nJob Results for {job_id}:")
                        print("=" * 50)

                        # Read all result files
                        for result_file in result_path.glob("*.txt"):
                            print(f"\nFile: {result_file.name}")
                            print("-" * 30)
                            with open(result_file, "r") as f:
                                content = f.read().strip()
                                if content:
                                    for line in content.split("\n")[
                                        :10
                                    ]:  # Show first 10 lines
                                        print(line)
                                    if len(content.split("\n")) > 10:
                                        print("... (truncated)")
                                else:
                                    print("(empty file)")
                    else:
                        print(f"Result path not found: {result_path}")
                else:
                    print(f"Invalid result URL: {result_url}")
            else:
                print(f"Error getting job results: {response.text}")

        except Exception as e:
            print(f"Error retrieving job results: {e}")

    def cleanup(self):
        """Clean up processes"""
        print("Cleaning up processes...")

        # Stop workers
        for process in self.worker_processes:
            if process.poll() is None:
                process.terminate()
                process.wait()

        # Stop master
        if self.master_process and self.master_process.poll() is None:
            self.master_process.terminate()
            self.master_process.wait()

        print("Cleanup completed")

    def run_test(self, job_type="wordcount"):
        """Run a complete test of the MapReduce system"""
        try:
            print("=" * 60)
            print("GridMR MapReduce System Test")
            print("=" * 60)

            # Setup
            self.setup_test_data()

            # Start services
            if not self.start_master():
                return False

            self.start_workers(2)

            # Submit and monitor job
            job_id = self.submit_job(job_type)
            if not job_id:
                return False

            success = self.monitor_job(job_id)
            if success:
                self.get_job_results(job_id)

            return success

        except KeyboardInterrupt:
            print("\nTest interrupted by user")
            return False
        finally:
            self.cleanup()


def main():
    """Main test function"""
    import argparse

    parser = argparse.ArgumentParser(description="Test GridMR MapReduce System")
    parser.add_argument(
        "--job-type",
        default="wordcount",
        choices=["wordcount", "charcount", "linelength"],
        help="Type of MapReduce job to test",
    )

    args = parser.parse_args()

    tester = GridMRTester()
    success = tester.run_test(args.job_type)

    if success:
        print("\n✅ Test completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Test failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
