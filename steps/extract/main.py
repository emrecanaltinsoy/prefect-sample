"""Step 1 - extract, served as a REST API.

Endpoints:
  GET  /health  -> liveness check
  POST /run     -> generate fake orders, store them as CSV in MinIO, return the object key

Output key: runs/<run_id>/raw/orders.csv (one folder per run, so parallel runs do not collide).
Logs go to stdout and to the Prefect API, tagged with flow_run_id / task_run_id when given.
"""

import csv
import io
import logging
import os
import random
import uuid
from datetime import date, timedelta

import boto3
from fastapi import FastAPI
from prefect_log_handler import setup_logger
from pydantic import BaseModel, Field

S3_BUCKET = os.getenv("S3_BUCKET", "pipeline-data")
S3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name="us-east-1",
)

PRODUCTS = {
    "keyboard": 45.0,
    "mouse": 20.0,
    "monitor": 220.0,
    "webcam": 65.0,
    "headset": 80.0,
}

logger = setup_logger("extract-step")
app = FastAPI(title="Extract step", version="2.0.0")


class ExtractRequest(BaseModel):
    num_orders: int = Field(100, ge=1, le=100_000)
    seed: int = 42  # same seed -> same output
    run_id: str | None = Field(
        None, pattern=r"^[A-Za-z0-9_-]{1,64}$"
    )  # generated if omitted
    flow_run_id: str | None = None  # set by the orchestrator, used to tag logs
    task_run_id: str | None = None


class ExtractResponse(BaseModel):
    run_id: str
    object_key: str
    order_count: int


def generate_orders(num_orders: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    start = date(2024, 1, 1)
    rows = []
    for order_id in range(1, num_orders + 1):
        product = rng.choice(list(PRODUCTS))
        rows.append(
            {
                "order_id": order_id,
                "customer_id": rng.randint(1, 25),
                "product": product,
                "quantity": rng.randint(1, 4),
                "unit_price": PRODUCTS[product],
                "order_date": (start + timedelta(days=rng.randint(0, 89))).isoformat(),
            }
        )
    return rows


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/run", response_model=ExtractResponse)
def run(req: ExtractRequest) -> ExtractResponse:
    run_id = req.run_id or str(uuid.uuid4())
    log = logging.LoggerAdapter(
        logger, {"flow_run_id": req.flow_run_id, "task_run_id": req.task_run_id}
    )
    log.info("Run %s: generating %d orders (seed=%d)", run_id, req.num_orders, req.seed)

    rows = generate_orders(req.num_orders, req.seed)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    key = f"runs/{run_id}/raw/orders.csv"
    S3.put_object(
        Bucket=S3_BUCKET, Key=key, Body=buf.getvalue().encode(), ContentType="text/csv"
    )

    log.info("Run %s: wrote %d orders to s3://%s/%s", run_id, len(rows), S3_BUCKET, key)
    return ExtractResponse(run_id=run_id, object_key=key, order_count=len(rows))
