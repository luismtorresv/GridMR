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


class TaskTracker:
    """Manages execution of individual map and reduce tasks on worker nodes"""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.current_tasks: Dict[str, TaskResult] = {}

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

            # Set up output collector
            intermediate_dir = os.path.join(task.output_dir, "intermediate")
            os.makedirs(intermediate_dir, exist_ok=True)

            # Read input file and process
            with open(task.input_file, "r") as f:
                lines = f.readlines()

                # Apply split boundaries if specified
                start = task.split_start
                end = task.split_end or len(lines)
                lines = lines[start:end]

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
                    with open(output_file, "w") as out_f:
                        for kv in kvs:
                            out_f.write(f"{kv.key}\t{kv.value}\n")
                    output_files.append(output_file)

                result.output_files = output_files
                result.status = TaskStatus.COMPLETED
                result.execution_time = time.time() - start_time

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error_message = str(e)
            result.execution_time = time.time() - start_time

        return result

    def execute_reduce_task(self, task: ReduceTask, reducer_class: type) -> TaskResult:
        """Execute a reduce task using the provided reducer"""
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

            # Read and merge all input files for this partition
            key_values: Dict[str, List[Any]] = {}

            for input_file in task.input_files:
                if os.path.exists(input_file):
                    with open(input_file, "r") as f:
                        for line in f:
                            parts = line.strip().split("\t", 1)
                            if len(parts) == 2:
                                key, value = parts
                                if key not in key_values:
                                    key_values[key] = []
                                key_values[key].append(value)

            # Execute reduce for each key
            os.makedirs(os.path.dirname(task.output_file), exist_ok=True)
            with open(task.output_file, "w") as out_f:
                for key, values in key_values.items():
                    for output_kv in reducer.reduce(key, iter(values)):
                        out_f.write(f"{output_kv.key}\t{output_kv.value}\n")

            result.output_files = [task.output_file]
            result.status = TaskStatus.COMPLETED
            result.execution_time = time.time() - start_time

        except Exception as e:
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

    def create_map_tasks(self, num_splits: int = None) -> List[MapTask]:
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
