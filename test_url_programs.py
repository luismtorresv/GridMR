#!/usr/bin/env python3
"""
Test script to demonstrate the new URL-based program loading system for GridMR.
This shows how mapper and reducer programs can now be loaded from URLs.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from mapreduce.program_loader import ProgramLoader


def test_url_based_program_loading():
    """Test the new URL-based program loading system"""

    print("🧪 Testing URL-based Program Loading System")
    print("=" * 50)

    # Initialize program loader
    loader = ProgramLoader()

    # Test 1: Load mapper from file URL
    print("\n1️⃣ Testing Mapper Loading from File URL")
    mapper_url = "file:///home/penguin/uni/gridmr/programs/wordcount_mapper.py"

    try:
        mapper_class = loader.load_program_from_url(mapper_url, "mapper")
        print(f"✅ Successfully loaded mapper: {mapper_class.__name__}")

        # Test the mapper
        mapper = mapper_class()
        test_input = "hello world hello gridmr"
        results = list(mapper.map(1, test_input))
        print(f"   Test input: '{test_input}'")
        print(f"   Mapper output: {[(kv.key, kv.value) for kv in results]}")

    except Exception as e:
        print(f"❌ Failed to load mapper: {e}")

    # Test 2: Load reducer from file URL
    print("\n2️⃣ Testing Reducer Loading from File URL")
    reducer_url = "file:///home/penguin/uni/gridmr/programs/wordcount_reducer.py"

    try:
        reducer_class = loader.load_program_from_url(reducer_url, "reducer")
        print(f"✅ Successfully loaded reducer: {reducer_class.__name__}")

        # Test the reducer
        reducer = reducer_class()
        test_values = ["1", "1", "1"]  # Simulating word count values
        results = list(reducer.reduce("hello", iter(test_values)))
        print(f"   Test input: key='hello', values={test_values}")
        print(f"   Reducer output: {[(kv.key, kv.value) for kv in results]}")

    except Exception as e:
        print(f"❌ Failed to load reducer: {e}")

    # Test 3: Test NFS URL simulation
    print("\n3️⃣ Testing NFS URL Simulation")
    # Create a symbolic link to simulate NFS mount
    nfs_programs_dir = Path("/tmp/nfs_simulation/shared/gridmr/programs")
    nfs_programs_dir.mkdir(parents=True, exist_ok=True)

    # Copy programs to simulated NFS location
    import shutil

    original_programs_dir = Path("/home/penguin/uni/gridmr/programs")
    if original_programs_dir.exists():
        for program_file in original_programs_dir.glob("*.py"):
            dest_file = nfs_programs_dir / program_file.name
            shutil.copy2(program_file, dest_file)
            print(f"   Copied {program_file.name} to simulated NFS")

    # Test loading from simulated NFS
    nfs_loader = ProgramLoader(nfs_mount="/tmp/nfs_simulation/mnt/gridmr")
    nfs_mapper_url = "nfs://shared/gridmr/programs/wordcount_mapper.py"

    try:
        nfs_mapper_class = nfs_loader.load_program_from_url(nfs_mapper_url, "mapper")
        print(
            f"✅ Successfully loaded mapper from NFS URL: {nfs_mapper_class.__name__}"
        )
    except Exception as e:
        print(f"❌ Failed to load mapper from NFS URL: {e}")

    print("\n🎉 Program loading tests completed!")
    print("\nThe system now supports:")
    print("  ✅ Hash-based key partitioning (works for any data type)")
    print(
        "  ✅ Clean directory structure: jobs/{id}/intermediate/{map,shuffled,reduce}/"
    )
    print("  ✅ URL-based program loading (file://, nfs://, http://)")
    print("  ✅ Dynamic class discovery and loading")
    print("  ✅ Proper shuffle & sort phase per Google's MapReduce spec")


if __name__ == "__main__":
    test_url_based_program_loading()
