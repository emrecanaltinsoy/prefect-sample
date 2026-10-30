"""Step 2 - transform, served as a REST API.

Endpoints:
  GET  /health  -> liveness check
  POST /run     -> read a raw orders CSV, write a revenue summary JSON, return its path

Logs go to stdout and to the Prefect API. If the caller passes flow_run_id and
task_run_id, the logs appear under that run in the Prefect UI.
"""

import csv
import json
import logging
import os
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, HTTPException
from prefect_log_handler import setup_logger
from pydantic import BaseModel

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()

logger = setup_logger("transform-step")
app = FastAPI(title="Transform step", version="1.0.0")


class TransformRequest(BaseModel):
    input_path: str  # must live under DATA_DIR, e.g. /data/raw/orders.csv
    flow_run_id: str | None = None  # set by the orchestrator, used to tag logs
    task_run_id: str | None = None


class TransformResponse(BaseModel):
    output_path: str
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
    input_path = Path(req.input_path).resolve()

    # Only allow reading files inside the shared data directory.
    if not input_path.is_relative_to(DATA_DIR):
        log.error("Rejected input_path outside data dir: %s", input_path)
        raise HTTPException(
            status_code=400, detail=f"input_path must be under {DATA_DIR}"
        )
    if not input_path.is_file():
        log.error("Input file not found: %s", input_path)
        raise HTTPException(
            status_code=404, detail=f"input file not found: {input_path}"
        )

    log.info("Reading orders from %s", input_path)
    revenue_by_product: dict[str, float] = defaultdict(float)
    units_by_product: dict[str, int] = defaultdict(int)
    order_count = 0

    with input_path.open(newline="") as f:
        for row in csv.DictReader(f):
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

    output_dir = DATA_DIR / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "summary.json"
    output_path.write_text(json.dumps(summary, indent=2))

    log.info(
        "Aggregated %d orders across %d products, total revenue %.2f",
        order_count,
        len(revenue_by_product),
        total_revenue,
    )
    log.info("Wrote summary to %s", output_path)
    return TransformResponse(
        output_path=str(output_path),
        order_count=order_count,
        total_revenue=total_revenue,
    )
