import argparse
import asyncio
import os
import sys
import uuid
from typing import Dict, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import requests

from mapreduce import TaskTracker, Mapper, Reducer
from mapreduce.types import MapTask, ReduceTask, TaskResult, TaskStatus

# Import example jobs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.mapreduce_jobs import MAPPER_REGISTRY, REDUCER_REGISTRY


class TaskRequest(BaseModel):
    """Request model for task assignment"""

    task_type: str  # "MAP" or "REDUCE"
    task_data: dict  # Serialized task data


class TaskResponse(BaseModel):
    """Response model for task completion"""

    task_id: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None


class HeartbeatRequest(BaseModel):
    """Heartbeat to maintain connection with master"""

    worker_id: str
    status: str
    current_tasks: list


class Worker:
    def __init__(self, worker_id: str, master_url: str, port: int):
        self.worker_id = worker_id
        self.master_url = master_url
        self.port = port
        self.task_tracker = TaskTracker(worker_id)
        self.current_tasks: Dict[str, TaskResult] = {}
        self.app = FastAPI(title=f"MapReduce Worker {worker_id}")
        self.setup_routes()

    def setup_routes(self):
        """Setup FastAPI routes for worker"""

        @self.app.get("/health")
        async def health_check():
            return {"status": "healthy", "worker_id": self.worker_id}

        @self.app.post("/task/execute")
        async def execute_task(request: TaskRequest) -> TaskResponse:
            """Execute a map or reduce task"""
            try:
                if request.task_type == "MAP":
                    task = MapTask(**request.task_data)
                    result = await self.execute_map_task(task)
                elif request.task_type == "REDUCE":
                    task = ReduceTask(**request.task_data)
                    result = await self.execute_reduce_task(task)
                else:
                    raise ValueError(f"Unknown task type: {request.task_type}")

                return TaskResponse(
                    task_id=result.task_id,
                    status=result.status.value,
                    result=result.model_dump()
                    if result.status == TaskStatus.COMPLETED
                    else None,
                    error=result.error_message,
                )

            except Exception as e:
                return TaskResponse(
                    task_id=request.task_data.get("task_id", "unknown"),
                    status=TaskStatus.FAILED.value,
                    error=str(e),
                )

        @self.app.get("/task/status/{task_id}")
        async def get_task_status(task_id: str):
            """Get status of a specific task"""
            if task_id in self.current_tasks:
                result = self.current_tasks[task_id]
                return {
                    "task_id": task_id,
                    "status": result.status.value,
                    "progress": 100 if result.status == TaskStatus.COMPLETED else 50,
                    "result": result.model_dump()
                    if result.status == TaskStatus.COMPLETED
                    else None,  # Include full result data
                }
            else:
                raise HTTPException(status_code=404, detail="Task not found")

    async def execute_map_task(self, task: MapTask) -> TaskResult:
        """Execute a map task asynchronously"""
        # Load mapper class from code_url (simplified - would need proper code loading)
        mapper_class = self.load_user_class(task.mapper_code, "Mapper")

        # Execute in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, self.task_tracker.execute_map_task, task, mapper_class
        )

        self.current_tasks[task.task_id] = result
        return result

    async def execute_reduce_task(self, task: ReduceTask) -> TaskResult:
        """Execute a reduce task asynchronously"""
        # Load reducer class from code_url (simplified)
        reducer_class = self.load_user_class(task.reducer_code, "Reducer")

        # Execute in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, self.task_tracker.execute_reduce_task, task, reducer_class
        )

        self.current_tasks[task.task_id] = result
        return result

    def load_user_class(self, code_content: str, class_type: str):
        """Dynamically load user-defined mapper or reducer class"""
        # For simplicity, we'll use the code_content as a job type identifier
        # In a real implementation, this would download and execute code from URL
        job_type = (
            code_content.lower()
            .replace("http://", "")
            .replace("https://", "")
            .split("/")[-1]
        )

        if class_type == "Mapper":
            return MAPPER_REGISTRY.get(job_type, MAPPER_REGISTRY["wordcount"])
        else:
            return REDUCER_REGISTRY.get(job_type, REDUCER_REGISTRY["wordcount"])

    def register_with_master(self):
        """Register this worker with the master node"""
        try:
            response = requests.post(
                f"{self.master_url}/worker/register",
                json={
                    "worker_id": self.worker_id,
                    "worker_url": f"http://localhost:{self.port}",
                    "worker_type": "compute",
                    "capabilities": ["map", "reduce"],
                },
            )
            if response.status_code == 200:
                print(f"Worker {self.worker_id} registered successfully")
                return True
            else:
                print(f"Failed to register worker: {response.text}")
                return False
        except Exception as e:
            print(f"Error registering with master: {e}")
            return False

    async def send_heartbeat(self):
        """Send periodic heartbeat to master"""
        while True:
            try:
                heartbeat_data = {
                    "worker_id": self.worker_id,
                    "status": "AVAILABLE",
                    "current_tasks": [
                        {"task_id": task_id, "status": result.status.value}
                        for task_id, result in self.current_tasks.items()
                    ],
                }

                response = requests.post(
                    f"{self.master_url}/worker/heartbeat", json=heartbeat_data
                )

                if response.status_code != 200:
                    print(f"Heartbeat failed: {response.text}")

            except Exception as e:
                print(f"Error sending heartbeat: {e}")

            await asyncio.sleep(30)  # Send heartbeat every 30 seconds

    async def start(self):
        """Start the worker server and register with master"""
        # Register with master
        if not self.register_with_master():
            print("Failed to register with master, exiting...")
            return

        # Start heartbeat task
        asyncio.create_task(self.send_heartbeat())

        # Start server
        config = uvicorn.Config(
            app=self.app, host="0.0.0.0", port=self.port, log_level="info"
        )
        server = uvicorn.Server(config)
        await server.serve()


# Example WordCount implementation for testing
class WordCountMapper(Mapper):
    """Example mapper for word count"""

    def map(self, key, value):
        """Emit each word with count 1"""
        from mapreduce.types import KeyValue

        words = value.split()
        for word in words:
            yield KeyValue(key=word.lower(), value=1)


class WordCountReducer(Reducer):
    """Example reducer for word count"""

    def reduce(self, key, values):
        """Sum up counts for each word"""
        from mapreduce.types import KeyValue

        total = sum(int(v) for v in values)
        yield KeyValue(key=key, value=total)


def handle_worker(args: argparse.Namespace):
    """Handle worker startup from CLI"""
    worker_id = f"worker_{uuid.uuid4().hex[:8]}"
    master_url = f"http://{args.master_ip}:{args.master_port}"

    worker = Worker(worker_id=worker_id, master_url=master_url, port=args.port)

    # Run the worker
    asyncio.run(worker.start())
