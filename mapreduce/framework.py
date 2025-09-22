import abc
import os
import time
from typing import Iterator, List, Dict, Any
from .types import KeyValue, MapTask, ReduceTask, TaskResult, TaskStatus, TaskType


class Mapper(abc.ABC):
    """Abstract base class for Map functions following Hadoop MapReduce pattern"""

    @abc.abstractmethod
    def map(self, key: Any, value: Any) -> Iterator[KeyValue]:
        """
        Map function that takes input key-value pairs and emits intermediate key-value pairs

        Args:
            key: Input key (typically line number or offset)
            value: Input value (typically line content)

        Yields:
            KeyValue pairs representing intermediate results
        """
        pass


class Reducer(abc.ABC):
    """Abstract base class for Reduce functions following Hadoop MapReduce pattern"""

    @abc.abstractmethod
    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[KeyValue]:
        """
        Reduce function that takes a key and all values for that key

        Args:
            key: The key to reduce
            values: Iterator over all values for this key

        Yields:
            KeyValue pairs representing final results
        """
        pass


class OutputCollector:
    """Collects output from map and reduce operations"""

    def __init__(self, output_file: str):
        self.output_file = output_file
        self.buffer = []

    def collect(self, key: Any, value: Any):
        """Collect a key-value pair for output"""
        self.buffer.append(KeyValue(key=key, value=value))

    def flush(self):
        """Write buffered output to file"""
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, "w") as f:
            for kv in self.buffer:
                f.write(f"{kv.key}\t{kv.value}\n")
        self.buffer.clear()


class ShuffleSorter:
    """
    Handles the Shuffle and Sort phase between Map and Reduce phases.
    According to Google's MapReduce specification, this phase:
    1. Groups intermediate key-value pairs by key
    2. Sorts them by key
    3. Prepares input for reducers in format (k2, list(v2))
    """

    def __init__(self, use_nfs: bool = False, nfs_mount: str = "/mnt/gridmr"):
        self.use_nfs = use_nfs
        self.nfs_mount = nfs_mount

    def shuffle_and_sort(
        self, intermediate_files: List[str], partition_id: int, output_file: str
    ) -> str:
        """
        Perform shuffle and sort for a specific partition.

        Args:
            intermediate_files: List of intermediate files from map tasks for this partition
            partition_id: The partition ID being processed
            output_file: Where to write the shuffled and sorted output

        Returns:
            Path to the shuffled and sorted file ready for reducer input
        """
        print(f"🔀 Starting shuffle and sort for partition {partition_id}")

        # CRITICAL NFS FIX: Map server paths to worker mount paths if needed
        if self.use_nfs:
            mapped_files = [
                f.replace("/shared/gridmr", self.nfs_mount) for f in intermediate_files
            ]
            mapped_output = output_file.replace("/shared/gridmr", self.nfs_mount)
        else:
            mapped_files = intermediate_files
            mapped_output = output_file

        # Phase 1: Collect all key-value pairs from intermediate files
        all_kvs: List[KeyValue] = []

        print(
            f"📖 Reading {len(mapped_files)} intermediate files for partition {partition_id}"
        )

        for input_file in mapped_files:
            if os.path.exists(input_file):
                print(f"   Processing intermediate file: {input_file}")
                with open(input_file, "r") as f:
                    line_count = 0
                    for line in f:
                        parts = line.strip().split("\t", 1)
                        if len(parts) == 2:
                            key, value = parts
                            all_kvs.append(KeyValue(key=key, value=value))
                            line_count += 1
                    print(f"     Read {line_count} key-value pairs")
            else:
                print(f"⚠️  Intermediate file not found: {input_file}")

        # Phase 2: Sort all key-value pairs by key (SORT step)
        print(f"🔧 Sorting {len(all_kvs)} key-value pairs by key")
        all_kvs.sort(key=lambda kv: str(kv.key))

        # Phase 3: Group by key (SHUFFLE step)
        # This creates the format (k2, list(v2)) that reducers expect
        key_groups: Dict[str, List[Any]] = {}
        for kv in all_kvs:
            if kv.key not in key_groups:
                key_groups[kv.key] = []
            key_groups[kv.key].append(kv.value)

        print(f"🔑 Grouped into {len(key_groups)} unique keys")

        # Phase 4: Write shuffled and sorted output in the format expected by reducers
        # Format: key\tvalue1,value2,value3... (comma-separated values for same key)
        os.makedirs(os.path.dirname(mapped_output), exist_ok=True)

        with open(mapped_output, "w") as out_f:
            for key in sorted(key_groups.keys()):  # Ensure keys are sorted
                values = key_groups[key]
                # Write as: key\tvalue1,value2,value3...
                values_str = ",".join(str(v) for v in values)
                out_f.write(f"{key}\t{values_str}\n")

        print(f"✅ Shuffle and sort completed for partition {partition_id}")
        print(f"   Output file: {mapped_output}")

        # Return the appropriate path (server path for NFS, local path otherwise)
        return output_file if self.use_nfs else mapped_output


class TaskTracker:
    """Manages execution of individual map and reduce tasks on worker nodes"""

    def __init__(
        self, worker_id: str, use_nfs: bool = False, nfs_mount: str = "/mnt/gridmr"
    ):
        self.worker_id = worker_id
        self.use_nfs = use_nfs
        self.nfs_mount = nfs_mount
        self.current_tasks: Dict[str, TaskResult] = {}
        self.shuffle_sorter = ShuffleSorter(use_nfs, nfs_mount)

    def execute_map_task(self, task: MapTask, mapper_class: type) -> TaskResult:
        """Execute a map task using the provided mapper"""
        start_time = time.time()
        result = TaskResult(
            task_id=task.task_id,
            task_type=TaskType.MAP,
            status=TaskStatus.RUNNING,
            output_files=[],
            worker_id=self.worker_id,
        )

        try:
            # Create mapper instance
            mapper = mapper_class()

            # CRITICAL NFS FIX: Map server paths to worker mount paths
            if self.use_nfs:
                # Convert server paths (/shared/gridmr/...) to worker mount paths (/mnt/gridmr/...)
                input_file = task.input_file.replace("/shared/gridmr", self.nfs_mount)
                output_dir = task.output_dir.replace("/shared/gridmr", self.nfs_mount)
                print("🔧 NFS path mapping:")
                print(f"   Original input: {task.input_file}")
                print(f"   Mapped input: {input_file}")
                print(f"   Original output: {task.output_dir}")
                print(f"   Mapped output: {output_dir}")
            else:
                input_file = task.input_file
                output_dir = task.output_dir

            # Set up output collector
            intermediate_dir = os.path.join(output_dir, "intermediate")
            os.makedirs(intermediate_dir, exist_ok=True)

            print(f"📁 Reading input file: {input_file}")
            print(f"📁 Output directory: {intermediate_dir}")

            # Read input file and process
            with open(input_file, "r") as f:
                lines = f.readlines()

                # Apply split boundaries if specified
                start = task.split_start
                end = task.split_end or len(lines)
                lines = lines[start:end]

                print(f"📊 Processing {len(lines)} lines from {input_file}")

                # SHUFFLE STEP: Group output by partition (hash of key)
                # Use consistent number of reducers for proper key distribution
                num_reducers = 4  # Standard number of reduce partitions
                partitions: Dict[int, List[KeyValue]] = {}

                for line_num, line in enumerate(lines, start):
                    # Call mapper to get key-value pairs
                    for kv in mapper.map(line_num, line.strip()):
                        # CRITICAL: Partition based on key hash (shuffle step)
                        # This ensures same keys go to same reducer
                        key_hash = hash(str(kv.key))
                        partition = key_hash % num_reducers

                        if partition not in partitions:
                            partitions[partition] = []
                        partitions[partition].append(kv)

                # Write partitioned output (sorted by key within each partition)
                output_files = []
                for partition_id, kvs in partitions.items():
                    # SORT STEP: Sort by key within each partition
                    kvs.sort(key=lambda x: str(x.key))

                    output_file = os.path.join(
                        intermediate_dir, f"map_{task.task_id}_part_{partition_id}.txt"
                    )

                    print(
                        f"💾 Writing partition {partition_id}: {len(kvs)} key-value pairs to {output_file}"
                    )

                    with open(output_file, "w") as out_f:
                        for kv in kvs:
                            out_f.write(f"{kv.key}\t{kv.value}\n")

                    # CRITICAL NFS FIX: Return server paths so master can find the files
                    if self.use_nfs:
                        # Convert worker mount path back to server path for master
                        server_output_file = output_file.replace(
                            self.nfs_mount, "/shared/gridmr"
                        )
                        output_files.append(server_output_file)
                    else:
                        output_files.append(output_file)

                result.output_files = output_files
                result.status = TaskStatus.COMPLETED
                result.execution_time = time.time() - start_time

                print(f"✅ Map task {task.task_id} completed successfully")
                print(f"   Processed {len(lines)} lines")
                print(f"   Generated {len(output_files)} partition files")
                print(f"   Output files: {output_files}")

        except Exception as e:
            print(f"❌ Map task {task.task_id} failed: {e}")
            result.status = TaskStatus.FAILED
            result.error_message = str(e)
            result.execution_time = time.time() - start_time

        return result

    def execute_reduce_task(self, task: ReduceTask, reducer_class: type) -> TaskResult:
        """
        Execute a reduce task using the provided reducer.

        IMPORTANT: This now expects the input files to already be shuffled and sorted
        by the ShuffleSorter in the correct format (k2, list(v2)) as per Google's specification.
        """
        start_time = time.time()
        result = TaskResult(
            task_id=task.task_id,
            task_type=TaskType.REDUCE,
            status=TaskStatus.RUNNING,
            output_files=[],
            worker_id=self.worker_id,
        )

        try:
            # Create reducer instance
            reducer = reducer_class()

            # CRITICAL NFS FIX: Map server paths to worker mount paths
            if self.use_nfs:
                # Convert server paths to worker mount paths
                input_files = [
                    f.replace("/shared/gridmr", self.nfs_mount)
                    for f in task.input_files
                ]
                output_file = task.output_file.replace("/shared/gridmr", self.nfs_mount)
                print("🔧 NFS path mapping for reduce task:")
                print(f"   Original input files: {task.input_files}")
                print(f"   Mapped input files: {input_files}")
                print(f"   Original output: {task.output_file}")
                print(f"   Mapped output: {output_file}")
            else:
                input_files = task.input_files
                output_file = task.output_file

            # STEP 1: Perform shuffle and sort on intermediate files
            # This creates the proper input format for the reducer: (k2, list(v2))
            shuffle_output_dir = os.path.join(os.path.dirname(output_file), "shuffled")
            shuffle_output_file = os.path.join(
                shuffle_output_dir, f"shuffled_part_{task.partition_id}.txt"
            )

            print(f"🔀 Performing shuffle and sort for reduce task {task.task_id}")
            shuffled_file = self.shuffle_sorter.shuffle_and_sort(
                input_files, task.partition_id, shuffle_output_file
            )

            # STEP 2: Execute reduce function on shuffled data
            # Now the input is in the correct format: each line is "key\tvalue1,value2,value3..."
            print(f"🔧 Executing reducer on shuffled data from: {shuffled_file}")

            # Map to local path if using NFS
            local_shuffled_file = (
                shuffled_file.replace("/shared/gridmr", self.nfs_mount)
                if self.use_nfs
                else shuffled_file
            )

            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, "w") as out_f:
                with open(local_shuffled_file, "r") as shuffled_f:
                    for line in shuffled_f:
                        parts = line.strip().split("\t", 1)
                        if len(parts) == 2:
                            key, values_str = parts
                            # Parse comma-separated values back to list
                            values = values_str.split(",") if values_str else []

                            # Call reducer with (k2, list(v2)) format - EXACTLY per Google spec!
                            for output_kv in reducer.reduce(key, iter(values)):
                                out_f.write(f"{output_kv.key}\t{output_kv.value}\n")

            # Return server path for master coordination
            if self.use_nfs:
                server_output_file = output_file.replace(
                    self.nfs_mount, "/shared/gridmr"
                )
                result.output_files = [server_output_file]
            else:
                result.output_files = [output_file]

            result.status = TaskStatus.COMPLETED
            result.execution_time = time.time() - start_time

            print(f"✅ Reduce task {task.task_id} completed successfully")
            print(f"   Output file: {result.output_files[0]}")

        except Exception as e:
            print(f"❌ Reduce task {task.task_id} failed: {e}")
            result.status = TaskStatus.FAILED
            result.error_message = str(e)
            result.execution_time = time.time() - start_time

        return result


class MapReduceJob:
    """Orchestrates a complete MapReduce job"""

    def __init__(
        self,
        job_id: str,
        job_name: str,
        input_files: List[str],
        output_dir: str,
        mapper_class: type,
        reducer_class: type,
    ):
        self.job_id = job_id
        self.job_name = job_name
        self.input_files = input_files
        self.output_dir = output_dir
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        self.map_tasks: List[MapTask] = []
        self.reduce_tasks: List[ReduceTask] = []
        self.completed_map_tasks: List[TaskResult] = []
        self.completed_reduce_tasks: List[TaskResult] = []

    def create_map_tasks(self, num_splits: int | None = None) -> List[MapTask]:
        """Create map tasks by splitting input files"""
        if num_splits is None:
            num_splits = len(self.input_files)

        tasks = []
        for i, input_file in enumerate(self.input_files):
            task_id = f"{self.job_id}_map_{i}"
            task = MapTask(
                task_id=task_id,
                input_file=input_file,
                output_dir=os.path.join(self.output_dir, "map_output"),
                mapper_code="",  # Would contain serialized mapper code for remote execution
            )
            tasks.append(task)

        self.map_tasks = tasks
        return tasks

    def create_reduce_tasks(self, num_reducers: int = 4) -> List[ReduceTask]:
        """Create reduce tasks based on map task outputs"""
        tasks = []

        # Collect all intermediate files from completed map tasks
        intermediate_files = []
        for map_result in self.completed_map_tasks:
            intermediate_files.extend(map_result.output_files)

        # Group intermediate files by partition
        partitions: Dict[int, List[str]] = {}
        for file_path in intermediate_files:
            # Extract partition ID from filename
            filename = os.path.basename(file_path)
            if "_part_" in filename:
                partition_id = int(filename.split("_part_")[1].split(".")[0])
                if partition_id not in partitions:
                    partitions[partition_id] = []
                partitions[partition_id].append(file_path)

        # Create reduce task for each partition
        for partition_id, files in partitions.items():
            task_id = f"{self.job_id}_reduce_{partition_id}"
            output_file = os.path.join(
                self.output_dir, "reduce_output", f"part-{partition_id:05d}.txt"
            )
            task = ReduceTask(
                task_id=task_id,
                input_files=files,
                output_file=output_file,
                reducer_code="",  # Would contain serialized reducer code
                partition_id=partition_id,
            )
            tasks.append(task)

        self.reduce_tasks = tasks
        return tasks
