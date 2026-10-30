# Orchestrator API

**Code:** [`main.py`](main.py) · **Image:** built from [`Dockerfile`](Dockerfile)
**Port:** `8000` in the container, `8080` on the host (Compose)

## What it does

A small FastAPI service that accepts pipeline requests and queues them as Prefect flow runs.
It does not run the pipeline itself:

1. `POST /pipelines` looks up the `orders-pipeline/orders-pipeline-docker` deployment and creates
   a flow run from it. The call returns immediately with the run's ID (`202 Accepted`).
2. The run waits in the Docker work pool until the worker has a free slot. The worker's
   `--limit 10` decides how many run at once.
3. `GET /pipelines/{flow_run_id}` returns the run's current state.

So you can send many requests at once. Extra requests wait in the queue; nothing is rejected.

## API

| Method | Path                       | Description                                  |
|--------|----------------------------|----------------------------------------------|
| GET    | `/health`                  | Liveness check                               |
| POST   | `/pipelines`               | Queue a pipeline run, returns the flow run ID (optional `Idempotency-Key` header) |
| GET    | `/pipelines/{flow_run_id}` | State, timestamps, and message of one run     |

Interactive docs (with the stack running): http://localhost:8080/docs

**Request** (`POST /pipelines`):

```json
{ "num_orders": 100 }
```

`num_orders` is 1 to 100000 (default 100). Sample files are in [`../samples/`](../samples/).

**Idempotency:** send an `Idempotency-Key` header (up to 128 characters). Repeating a request with the
same key returns the existing run instead of creating a new one, so retries are safe.

**Response** (`202 Accepted`):

```json
{
  "flow_run_id": "2ff3ac16-0c6a-4f4d-a961-d4ceee49baa8",
  "state": "Scheduled",
  "ui_url": "http://localhost:4200/runs/flow-run/2ff3ac16-0c6a-4f4d-a961-d4ceee49baa8"
}
```

**Status** (`GET /pipelines/{flow_run_id}`):

```json
{
  "flow_run_id": "2ff3ac16-0c6a-4f4d-a961-d4ceee49baa8",
  "name": "agate-tamarin",
  "state": "Completed",
  "state_message": null,
  "created": "2026-10-30T10:45:57.781096+00:00",
  "started": "2026-10-30T10:46:05.920277+00:00",
  "ended": "2026-10-30T10:46:05.995400+00:00"
}
```

Errors: `503` if the deployment doesn't exist yet (the `deployer` service has not run), `404` for an
unknown run ID.

## Try it

With the stack running (`docker compose up -d`):

```bash
curl -X POST http://localhost:8080/pipelines \
     -H "Content-Type: application/json" \
     -d @../samples/create-pipeline.json

curl http://localhost:8080/pipelines/<flow_run_id>
```

## Configuration

| Variable           | Default                                       | Purpose                              |
|--------------------|-----------------------------------------------|--------------------------------------|
| `PREFECT_API_URL`  | from the Prefect settings (`http://localhost:4200/api`) | Prefect server to create runs on |
| `DEPLOYMENT_NAME`  | `orders-pipeline/orders-pipeline-docker`      | Deployment to run                    |
| `PREFECT_UI_URL`   | `http://localhost:4200`                       | Base URL for the `ui_url` links      |

## Run locally (without Docker)

```bash
cd orchestrator
uv sync
PREFECT_API_URL=http://localhost:4200/api uv run uvicorn main:app --port 8080
```

## Tests

```bash
cd orchestrator && uv run pytest
```

The tests use a fake Prefect client, so they don't need the server.

This needs the Prefect server and the deployment running. Start them with `docker compose up -d prefect-server postgres deployer`.
