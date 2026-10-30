# Prefect Sample: Orders Pipeline (Docker, REST, MinIO, Postgres)

An orchestrated pipeline with Prefect 3 where:

- an **orchestrator API** (FastAPI) accepts pipeline requests and queues them as Prefect flow runs,
- a **Docker work pool** runs each flow run in its own container, at most **10 at a time**,
- the flow calls two **step services** (FastAPI, one container each) over HTTP,
- step outputs are stored in **MinIO** (S3), one folder per run,
- Prefect's state and queue are stored in **PostgreSQL**.

```
client ──POST /pipelines──▶ orchestrator-api (FastAPI, :8080)
                                  │ create flow run (returns immediately)
                                  ▼
                            prefect-server (UI :4200) ── postgres
                                  │ queued runs
                                  ▼
                            prefect-worker (--limit 10)
                                  │ one container per flow run
                                  ▼
                    flow container (orders-pipeline)
                          │ POST /run                 │ POST /run
                          ▼                           ▼
             extract-step (:8000, internal)  transform-step (:8000, internal)
                          │ put runs/<id>/raw/orders.csv      ▲ get runs/<id>/raw/orders.csv
                          └──────────────► minio (S3 :9000, console :9001) ─┘
                                           put runs/<id>/processed/summary.json
```

Each flow run's `run_id` is its Prefect flow run ID, so parallel runs never overwrite each other.

## Project layout

```
.
├── flow.py                    # Prefect flow: calls the step APIs (runs in the flow container)
├── deploy.py                  # Registers the flow as a deployment on the Docker work pool
├── Dockerfile                 # Flow image (also used by the deployer and the worker)
├── infra/docker-job-template.json   # Work pool template: network, env, image, auto-remove
├── docker-compose.yml         # Everything below, wired together
├── orchestrator/              # Orchestrator API (FastAPI) - see its README
├── steps/
│   ├── extract/               # Step 1 service - see its README
│   └── transform/             # Step 2 service - see its README
├── packages/
│   └── prefect-log-handler/   # Shared library: sends logs to the Prefect API
├── samples/                   # Sample request payloads and a burst script
├── pyproject.toml / uv.lock   # Flow and deployer dependencies (uv)
└── .python-version            # 3.15
```

## Services

| Service          | URL                          | Purpose                                                  |
|------------------|------------------------------|----------------------------------------------------------|
| orchestrator-api | http://localhost:8080/docs   | Start and check pipeline runs                            |
| prefect-server   | http://localhost:4200        | Prefect UI and API                                       |
| prefect-worker   | —                            | Starts a container per flow run (max 10 at once)         |
| extract-step     | internal only                | Step 1 REST API (`http://extract-step:8000`)             |
| transform-step   | internal only                | Step 2 REST API (`http://transform-step:8000`)           |
| minio            | http://localhost:9001        | MinIO console (user and password from `.env`)            |
| reaper           | —                            | Marks runs stuck in `Submitting` as Crashed              |
| postgres         | —                            | Prefect database                                         |
| deployer         | —                            | One-shot: creates the work pool and deployment, then exits |

## Requirements

- Docker with Compose v2 (Docker Desktop, or Colima).
- **Memory:** each flow run is a Python container of roughly 150 to 250 MB. With the limit at
  10, plan for about 3 GB for flow runs plus about 1 GB for the services. On Colima, start it
  with `colima start --memory 8 --cpu 4`. At 2 GB the Prefect server was killed by the OOM killer
  during a burst.
- For local development without Docker: [uv](https://docs.astral.sh/uv/). All projects use
  **Python 3.15** (pinned in `.python-version`).

## Run it

Create your local settings first. `.env` holds the Postgres and MinIO credentials and is not committed:

```bash
cp .env.example .env     # then set real values for POSTGRES_PASSWORD, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
```

```bash
docker compose up --build -d
docker compose ps        # wait until the services are healthy
```

If you change `POSTGRES_PASSWORD` after the first start, run `docker compose down -v` once, because
Postgres only reads the password when its data volume is created.

On startup, `deployer` creates the `docker-pool` work pool and the
`orders-pipeline/orders-pipeline-docker` deployment, then exits. `prefect-worker` then
starts polling the pool.

Stop everything (data is kept in volumes):

```bash
docker compose down
```

Remove the data as well:

```bash
docker compose down -v
```

## Start a pipeline run

Requests take a JSON body. Sample payloads are in [`samples/`](samples/).

```bash
curl -X POST http://localhost:8080/pipelines \
     -H "Content-Type: application/json" \
     -d @samples/create-pipeline.json
```

Response (`202 Accepted`). The run is queued, and the worker starts it when a slot is free:

```json
{
  "flow_run_id": "2ff3ac16-0c6a-4f4d-a961-d4ceee49baa8",
  "state": "Scheduled",
  "ui_url": "http://localhost:4200/runs/flow-run/2ff3ac16-0c6a-4f4d-a961-d4ceee49baa8"
}
```

Check the run:

```bash
curl http://localhost:8080/pipelines/<flow_run_id>
```

Send an `Idempotency-Key` header to make retries safe. A second request with the same key returns
the existing run instead of starting another one:

```bash
curl -X POST http://localhost:8080/pipelines \
     -H "Content-Type: application/json" -H "Idempotency-Key: order-42" \
     -d @samples/create-pipeline.json
```

Payloads:

| File                                         | Body                  |
|----------------------------------------------|-----------------------|
| `samples/create-pipeline.json`               | `{"num_orders": 100}` |
| `samples/create-pipeline-large.json`         | `{"num_orders": 5000}` |

## Test parallel runs

```bash
./samples/burst.sh 25      # sends 25 requests as fast as possible
```

The orchestrator returns right away for each request. The worker runs at most 10 flow
containers at once, and the rest wait in the queue. Watch them in the UI at
http://localhost:4200, or check the worker log:

```bash
docker compose logs -f prefect-worker
```

Measured on this setup (Colima, 8 GB, 4 CPUs): 25 requests of 20,000 orders each. The host never ran more than 10 flow containers at once, and all 25 completed.

## See the results

Each run writes two objects to the `pipeline-data` bucket, under its run ID:

```
runs/<flow_run_id>/raw/orders.csv
runs/<flow_run_id>/processed/summary.json
```

Open the MinIO console at http://localhost:9001 (user and password from `.env`) and browse bucket
`pipeline-data`. To download an object from the command line, use any S3 client, for example the AWS CLI:

```bash
AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
  aws --endpoint-url http://localhost:9000 --region us-east-1 \
  s3 cp s3://pipeline-data/runs/<flow_run_id>/processed/summary.json -
```

Example summary (the fixed seed gives the same output for 100 orders on every run):

```json
{
  "order_count": 100,
  "total_revenue": 20220.0,
  "products": {
    "monitor": { "units": 45, "revenue": 9900.0 },
    "headset": { "units": 50, "revenue": 4000.0 },
    "keyboard": { "units": 61, "revenue": 2745.0 },
    "webcam": { "units": 39, "revenue": 2535.0 },
    "mouse": { "units": 52, "revenue": 1040.0 }
  }
}
```

## Tests and lint

Each project has its own tests (run with uv from the project folder):

```bash
(cd steps/extract && uv run pytest)
(cd steps/transform && uv run pytest)
(cd orchestrator && uv run pytest)
uv run pytest                              # flow-side tests (the reaper)
```

Lint and format, with the same ruff version as the pre-commit hooks:

```bash
uv tool run ruff@0.8.0 check .
uv tool run ruff@0.8.0 format --check .
```

## View logs in the Prefect UI

Each service sends its logs to the Prefect API as well as stdout, tagged with the flow run and
task run. Open a run in the UI and select the **Logs** tab. You will see the orchestrator's
messages, plus `extract-step` and `transform-step` messages, in one place.

Container output: `docker compose logs extract-step`.

## Local development without Docker

Use this for working on the Python code. The orchestrator and flow still need the Prefect server,
MinIO, and the work pool, so start the infrastructure with Compose and run the rest locally:

```bash
docker compose up -d postgres minio prefect-server
set -a; . ./.env; set +a                       # load the MinIO credentials
export PREFECT_API_URL=http://localhost:4200/api
export S3_ENDPOINT_URL=http://localhost:9000
export S3_ACCESS_KEY=$MINIO_ROOT_USER S3_SECRET_KEY=$MINIO_ROOT_PASSWORD

uv run --project steps/extract uvicorn --app-dir steps/extract main:app --port 8001
uv run --project steps/transform uvicorn --app-dir steps/transform main:app --port 8002
uv run python flow.py      # runs the flow in this terminal, no worker needed
```

`flow.py` run this way uses the default step URLs (`localhost:8001` and `localhost:8002`), which match the local `uvicorn` ports above.

## More details

- [Orchestrator API](orchestrator/README.md)
- [Step 1: extract](steps/extract/README.md)
- [Step 2: transform](steps/transform/README.md)
- [prefect-log-handler](packages/prefect-log-handler/README.md)
