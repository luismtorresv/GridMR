import argparse

from fastapi import FastAPI

app = FastAPI()


def handle_master(args: argparse.Namespace):
    raise NotImplementedError


@app.get("/")
def read_root():
    return {"Hello": "World"}
