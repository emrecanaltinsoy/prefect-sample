"""Orchestration: Prefect flow that calls the extract and transform REST services."""

import os

import httpx
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run, task_run

EXTRACT_URL = os.getenv("EXTRACT_URL", "http://localhost:8001")
TRANSFORM_URL = os.getenv("TRANSFORM_URL", "http://localhost:8002")
HTTP_TIMEOUT = httpx.Timeout(60.0)


def run_context() -> dict:
    """IDs the steps need to attach their logs to this flow run and task run."""
    return {
        "flow_run_id": str(flow_run.id) if flow_run.id else None,
        "task_run_id": str(task_run.id) if task_run.id else None,
    }


@task(name="call-extract-step", retries=2, retry_delay_seconds=3)
def call_extract_step(num_orders: int) -> dict:
    logger = get_run_logger()
    payload = {"num_orders": num_orders, **run_context()}
    resp = httpx.post(f"{EXTRACT_URL}/run", json=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    result = resp.json()
    logger.info(
        "Extract step wrote %d orders to %s",
        result["order_count"],
        result["output_path"],
    )
    return result


@task(name="call-transform-step", retries=2, retry_delay_seconds=3)
def call_transform_step(input_path: str) -> dict:
    logger = get_run_logger()
    payload = {"input_path": input_path, **run_context()}
    resp = httpx.post(f"{TRANSFORM_URL}/run", json=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    result = resp.json()
    logger.info("Transform step wrote summary to %s", result["output_path"])
    return result


@flow(name="orders-pipeline", log_prints=True)
def orders_pipeline(num_orders: int = 100) -> dict:
    extracted = call_extract_step(num_orders)  # step 1 (REST)
    summary = call_transform_step(extracted["output_path"])  # step 2 (REST)
    return summary


if __name__ == "__main__":
    result = orders_pipeline()
    print(f"Pipeline finished. Total revenue: {result['total_revenue']}")
