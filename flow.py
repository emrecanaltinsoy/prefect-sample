"""Orchestration: Prefect flow that calls the extract and transform REST services.

Each run gets its own run_id (the Prefect flow run ID), used as the folder name in MinIO,
so parallel runs never overwrite each other's files.
"""

import os
import uuid

import httpx
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run, task_run

EXTRACT_URL = os.getenv("EXTRACT_URL", "http://localhost:8001")
TRANSFORM_URL = os.getenv("TRANSFORM_URL", "http://localhost:8002")
HTTP_TIMEOUT = httpx.Timeout(60.0)


def run_context() -> dict:
    """IDs the steps need to tag their logs with this flow run and task run."""
    return {
        "flow_run_id": str(flow_run.id) if flow_run.id else None,
        "task_run_id": str(task_run.id) if task_run.id else None,
    }


@task(name="call-extract-step", retries=2, retry_delay_seconds=3)
def call_extract_step(run_id: str, num_orders: int) -> dict:
    logger = get_run_logger()
    payload = {"num_orders": num_orders, "run_id": run_id, **run_context()}
    resp = httpx.post(f"{EXTRACT_URL}/run", json=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    result = resp.json()
    logger.info(
        "Extract step wrote %d orders to %s",
        result["order_count"],
        result["object_key"],
    )
    return result


@task(name="call-transform-step", retries=2, retry_delay_seconds=3)
def call_transform_step(run_id: str) -> dict:
    logger = get_run_logger()
    payload = {"run_id": run_id, **run_context()}
    resp = httpx.post(f"{TRANSFORM_URL}/run", json=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    result = resp.json()
    logger.info("Transform step wrote summary to %s", result["object_key"])
    return result


@flow(name="orders-pipeline", log_prints=True)
def orders_pipeline(num_orders: int = 100) -> dict:
    run_id = str(flow_run.id) if flow_run.id else str(uuid.uuid4())
    extracted = call_extract_step(run_id, num_orders)  # step 1 (REST)
    summary = call_transform_step(run_id)  # step 2 (REST)
    return {"run_id": run_id, "extract": extracted, "summary": summary}


if __name__ == "__main__":
    result = orders_pipeline()
    print(
        f"Pipeline finished: run {result['run_id']}, total revenue {result['summary']['total_revenue']}"
    )
