"""Step 1 - extract, served as a REST API.

Endpoints:
  GET  /health  -> liveness check
  POST /run     -> generate fake orders, write them to CSV, return the file path

Logs go to stdout and to the Prefect API. If the caller passes flow_run_id and
task_run_id, the logs appear under that run in the Prefect UI.
"""

import csv
import logging
import os
import random
from datetime import date, timedelta
from pathlib import Path

from fastapi import FastAPI
from prefect_log_handler import setup_logger
from pydantic import BaseModel, Field

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

PRODUCTS = {
    "keyboard": 45.0,
    "mouse": 20.0,
    "monitor": 220.0,
    "webcam": 65.0,
    "headset": 80.0,
}

logger = setup_logger("extract-step")
app = FastAPI(title="Extract step", version="1.0.0")


class ExtractRequest(BaseModel):
    num_orders: int = Field(100, ge=1, le=100_000)
    seed: int = 42  # same seed -> same output
    flow_run_id: str | None = None  # set by the orchestrator, used to tag logs
    task_run_id: str | None = None


class ExtractResponse(BaseModel):
    output_path: str
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
    # Attach run ids to every record logged for this request.
    log = logging.LoggerAdapter(
        logger, {"flow_run_id": req.flow_run_id, "task_run_id": req.task_run_id}
    )
    log.info("Generating %d orders (seed=%d)", req.num_orders, req.seed)

    rows = generate_orders(req.num_orders, req.seed)

    output_dir = DATA_DIR / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "orders.csv"

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    log.info("Wrote %d orders to %s", len(rows), output_path)
    return ExtractResponse(output_path=str(output_path), order_count=len(rows))
