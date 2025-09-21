#!/usr/bin/env python3
"""
Simple demonstration script for the GridMR MapReduce system.
This script shows how to use the system with a basic word count example.
"""

import sys
import time
import subprocess
from pathlib import Path


def create_demo_data():
    """Create demonstration text files for testing"""
    demo_dir = Path("demo_data")
    demo_dir.mkdir(exist_ok=True)

    # Sample text files
    texts = {
        "shakespeare.txt": """
        To be or not to be, that is the question:
        Whether 'tis nobler in the mind to suffer
        The slings and arrows of outrageous fortune,
        Or to take arms against a sea of troubles
        And by opposing end them.
        """,
        "declaration.txt": """
        We hold these truths to be self-evident, that all men are created equal,
        that they are endowed by their Creator with certain unalienable Rights,
        that among these are Life, Liberty and the pursuit of Happiness.
        """,
        "gettysburg.txt": """
        Four score and seven years ago our fathers brought forth on this continent,
        a new nation, conceived in Liberty, and dedicated to the proposition that
        all men are created equal.
        """,
    }

    for filename, content in texts.items():
        file_path = demo_dir / filename
        with open(file_path, "w") as f:
            f.write(content.strip())

    print(f"Created demo data in {demo_dir}")
    return demo_dir


def run_demo():
    """Run a complete demonstration of the MapReduce system"""
    print("=" * 60)
    print("GridMR MapReduce System Demo")
    print("=" * 60)

    # Create demo data
    data_dir = create_demo_data()

    print("\n1. Starting Master Node...")
    master_cmd = [sys.executable, "cli.py", "master", "--port", "8000"]
    master_process = subprocess.Popen(
        master_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Wait for master to start
    time.sleep(3)

    print("2. Starting Worker Nodes...")
    worker1_cmd = [
        sys.executable,
        "cli.py",
        "worker",
        "localhost",
        "8000",
        "--port",
        "8001",
    ]
    worker1_process = subprocess.Popen(
        worker1_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    worker2_cmd = [
        sys.executable,
        "cli.py",
        "worker",
        "localhost",
        "8000",
        "--port",
        "8002",
    ]
    worker2_process = subprocess.Popen(
        worker2_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Wait for workers to register
    time.sleep(5)

    print("3. Submitting Word Count Job...")
    try:
        # Submit job using the client
        client_cmd = [
            sys.executable,
            "cli.py",
            "client",
            "localhost",
            f"file://{data_dir.absolute()}",
            "wordcount",
        ]

        client_process = subprocess.run(
            client_cmd, capture_output=True, text=True, timeout=120
        )

        if client_process.returncode == 0:
            print("✅ Job completed successfully!")
            print("\nClient output:")
            print(client_process.stdout)
        else:
            print("❌ Job failed!")
            print("Error:", client_process.stderr)

    except subprocess.TimeoutExpired:
        print("⏰ Job timed out")
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    finally:
        print("\n4. Cleaning up processes...")

        # Clean up processes
        for process in [master_process, worker1_process, worker2_process]:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

        print("Demo completed!")


if __name__ == "__main__":
    try:
        run_demo()
    except Exception as e:
        print(f"Demo failed with error: {e}")
        sys.exit(1)
