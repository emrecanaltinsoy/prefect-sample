"""Step 2 - transform, served as a REST API.

Endpoints:
  GET  /health  -> liveness check
  POST /run     -> read runs/<run_id>/raw/orders.csv from MinIO, write
                   runs/<run_id>/processed/summary.json, return the summary

Logs go to stdout and to the Prefect API, tagged with flow_run_id / task_run_id when given.
"""

import csv
import io
import json
import logging
import os
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
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

logger = setup_logger("transform-step")
app = FastAPI(title="Transform step", version="2.0.0")


class TransformRequest(BaseModel):
    run_id: str = Field(..., pattern=r"^[A-Za-z0-9_-]{1,64}$")
    flow_run_id: str | None = None  # set by the orchestrator, used to tag logs
    task_run_id: str | None = None


class TransformResponse(BaseModel):
    run_id: str
    object_key: str
    order_count: int
    total_revenue: float


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/run", response_model=TransformResponse)
def run(req: TransformRequest) -> TransformResponse:
    log = logging.LoggerAdapter(
        logger, {"flow_run_id": req.flow_run_id, "task_run_id": req.task_run_id}
    )
    in_key = f"runs/{req.run_id}/raw/orders.csv"
    log.info("Run %s: reading s3://%s/%s", req.run_id, S3_BUCKET, in_key)

    try:
        body = S3.get_object(Bucket=S3_BUCKET, Key=in_key)["Body"].read().decode()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            log.error("Run %s: input not found: %s", req.run_id, in_key)
            raise HTTPException(status_code=404, detail=f"input not found: {in_key}")
        raise

    revenue_by_product: dict[str, float] = defaultdict(float)
    units_by_product: dict[str, int] = defaultdict(int)
    order_count = 0
    for row in csv.DictReader(io.StringIO(body)):
        qty = int(row["quantity"])
        price = float(row["unit_price"])
        revenue_by_product[row["product"]] += qty * price
        units_by_product[row["product"]] += qty
        order_count += 1

    total_revenue = round(sum(revenue_by_product.values()), 2)
    summary = {
        "order_count": order_count,
        "total_revenue": total_revenue,
        "products": {
            product: {"units": units_by_product[product], "revenue": round(revenue, 2)}
            for product, revenue in sorted(
                revenue_by_product.items(), key=lambda kv: kv[1], reverse=True
            )
        },
    }

    out_key = f"runs/{req.run_id}/processed/summary.json"
    S3.put_object(
        Bucket=S3_BUCKET,
        Key=out_key,
        Body=json.dumps(summary, indent=2).encode(),
        ContentType="application/json",
    )

    log.info(
        "Run %s: aggregated %d orders across %d products, total revenue %.2f",
        req.run_id,
        order_count,
        len(revenue_by_product),
        total_revenue,
    )
    log.info("Run %s: wrote summary to s3://%s/%s", req.run_id, S3_BUCKET, out_key)
    return TransformResponse(
        run_id=req.run_id,
        object_key=out_key,
        order_count=order_count,
        total_revenue=total_revenue,
    )
