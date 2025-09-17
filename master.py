import argparse

from fastapi import FastAPI

app = FastAPI()


def handle_master(args: argparse.Namespace):
    raise NotImplementedError


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/job/submit")
def receive_job(data_url: str, code_url: str):
    return {"data_url": data_url, "code_url": code_url}


@app.get("/job/status/{job_id}")
def send_status(job_id: int):
    return {"job_id": job_id}


@app.get("/job/result/url/{job_id}")
def send_result_url(job_id: int):
    return {"result_url": job_id}
