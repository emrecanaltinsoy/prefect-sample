"""Orchestrator API: accepts pipeline requests and queues them as Prefect flow runs.

This API does not run the pipeline itself. Each POST creates a flow run from the
deployment, and the Prefect work pool starts it when a worker slot is free
(the worker's --limit caps how many run at once).
"""

import os
from uuid import UUID

from typing import Annotated

from fastapi import FastAPI, Header, HTTPException
from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectNotFound
from pydantic import BaseModel, Field

DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "orders-pipeline/orders-pipeline-docker")
PREFECT_UI_URL = os.getenv("PREFECT_UI_URL", "http://localhost:4200")

app = FastAPI(title="Orders pipeline orchestrator", version="1.0.0")


class PipelineRequest(BaseModel):
    num_orders: int = Field(
        100, ge=1, le=100_000, description="Orders to generate for this run"
    )


class PipelineCreated(BaseModel):
    flow_run_id: UUID
    state: str | None
    ui_url: str


class PipelineStatus(BaseModel):
    flow_run_id: UUID
    name: str
    state: str | None
    state_message: str | None
    created: str | None
    started: str | None
    ended: str | None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/pipelines", status_code=202, response_model=PipelineCreated)
async def create_pipeline(
    req: PipelineRequest,
    idempotency_key: Annotated[str | None, Header(max_length=128)] = None,
) -> PipelineCreated:
    """Queue a run. Send the same Idempotency-Key again (for example on a retry) to get the
    existing run back instead of starting a second one."""
    async with get_client() as client:
        try:
            deployment = await client.read_deployment_by_name(DEPLOYMENT_NAME)
        except ObjectNotFound:
            raise HTTPException(
                status_code=503,
                detail=f"deployment '{DEPLOYMENT_NAME}' not found; run the deployer first",
            )
        run = await client.create_flow_run_from_deployment(
            deployment.id,
            parameters={"num_orders": req.num_orders},
            idempotency_key=idempotency_key,
        )

    return PipelineCreated(
        flow_run_id=run.id,
        state=run.state.name if run.state else None,
        ui_url=f"{PREFECT_UI_URL}/runs/flow-run/{run.id}",
    )


@app.get("/pipelines/{flow_run_id}", response_model=PipelineStatus)
async def get_pipeline(flow_run_id: UUID) -> PipelineStatus:
    async with get_client() as client:
        try:
            run = await client.read_flow_run(flow_run_id)
        except ObjectNotFound:
            raise HTTPException(
                status_code=404, detail=f"flow run {flow_run_id} not found"
            )

    return PipelineStatus(
        flow_run_id=run.id,
        name=run.name,
        state=run.state.name if run.state else None,
        state_message=run.state.message if run.state else None,
        created=run.created.isoformat() if run.created else None,
        started=run.start_time.isoformat() if run.start_time else None,
        ended=run.end_time.isoformat() if run.end_time else None,
    )
