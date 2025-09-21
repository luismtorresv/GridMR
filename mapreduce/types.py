from typing import TypeVar, Generic, List, Optional
from pydantic import BaseModel
from enum import Enum

# Type definitions for MapReduce
K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type


class KeyValue(BaseModel, Generic[K, V]):
    """Generic key-value pair for MapReduce operations"""

    key: K
    value: V


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskType(str, Enum):
    MAP = "MAP"
    REDUCE = "REDUCE"


class MapTask(BaseModel):
    """Map task definition"""

    task_id: str
    input_file: str
    output_dir: str
    mapper_code: str
    split_start: int = 0
    split_end: Optional[int] = None


class ReduceTask(BaseModel):
    """Reduce task definition"""

    task_id: str
    input_files: List[str]  # Intermediate files from map phase
    output_file: str
    reducer_code: str
    partition_id: int


class TaskResult(BaseModel):
    """Result of a completed task"""

    task_id: str
    task_type: TaskType
    status: TaskStatus
    output_files: List[str]
    error_message: Optional[str] = None
    execution_time: Optional[float] = None
    worker_id: Optional[str] = None
