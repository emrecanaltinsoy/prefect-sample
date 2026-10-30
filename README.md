# Prefect Sample: Two-Step Orders Pipeline (Docker + REST)

A [Prefect 3](https://docs.prefect.io/) orchestration where each step runs in its
own Docker container and exposes a REST API. The Prefect flow calls the steps over HTTP.

```
                       ┌──────────────────────────┐
                       │  pipeline (Prefect flow)  │  flow.py
                       └──────┬───────────────┬────┘
              POST /run       │               │       POST /run
                  ┌───────────▼──┐        ┌───▼──────────────┐
                  │ extract-step │        │ transform-step   │
                  │ :8001 → :8000│        │ :8002 → :8000    │
                  └──────┬───────┘        └────────▲─────────┘
                         │ writes orders.csv        │ reads orders.csv
                         └────────► ./data ─────────┘ writes summary.json
                                     ▲
                       prefect-server (UI/API) :4200
```

The flow runs two tasks in order: `call-extract-step`, then `call-transform-step`.
The extract response contains the file path, and the flow passes it to the transform step.

## Project layout

```
.
├── flow.py                    # Prefect flow: calls the step APIs over HTTP
├── Dockerfile                 # Orchestrator image
├── docker-compose.yml         # Prefect server + 2 step services + pipeline
├── pyproject.toml / uv.lock   # Orchestrator dependencies (uv)
├── packages/
│   └── prefect-log-handler/   # Shared uv package: sends logs to the Prefect API
│       ├── README.md
│       ├── pyproject.toml
│       └── src/prefect_log_handler/
├── steps/
│   ├── extract/               # Step 1 service
│   │   ├── README.md
│   │   ├── main.py            # FastAPI app: GET /health, POST /run
│   │   ├── Dockerfile
│   │   ├── pyproject.toml / uv.lock
│   └── transform/             # Step 2 service
│       ├── README.md
│       ├── main.py            # FastAPI app: GET /health, POST /run
│       ├── Dockerfile
│       ├── pyproject.toml / uv.lock
└── data/                      # Shared volume, created at runtime
    ├── raw/orders.csv
    └── processed/summary.json
```

Each step has its own `pyproject.toml` and `uv.lock`, so each image installs only the
dependencies it needs. Both steps depend on the shared `packages/prefect-log-handler`
library, so the step images are built from the repository root (`context: .` in
`docker-compose.yml`) to include it.

## Run everything with Docker Compose

Requires Docker (Docker Desktop, or Docker Engine + Compose v2).

```bash
docker compose up --build
```

Services:

| Service          | URL                          | Purpose                          |
|------------------|------------------------------|----------------------------------|
| prefect-server   | http://localhost:4200        | Prefect UI and API               |
| extract-step     | http://localhost:8001/docs   | Step 1 REST API (Swagger UI)     |
| transform-step   | http://localhost:8002/docs   | Step 2 REST API (Swagger UI)     |
| pipeline         | —                            | Runs the flow once, then exits   |

Open http://localhost:4200 to see the `orders-pipeline` run. Results are in `./data`.

Stop everything:

```bash
docker compose down
```

## Call the steps directly with curl

With the stack running:

```bash
curl -X POST http://localhost:8001/run -H "Content-Type: application/json" \
     -d '{"num_orders": 100, "seed": 42}'

curl -X POST http://localhost:8002/run -H "Content-Type: application/json" \
     -d '{"input_path": "/data/raw/orders.csv"}'
```

Note that the transform step sees the shared volume at `/data`, so the path is the
container path, not `./data/...` on your machine.

## Run locally without Docker

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/). All projects use **Python 3.15** (pinned in `.python-version`); `uv` installs it for you. Use three terminals,
all from the project root.

```bash
# Terminal 1: step 1 API
DATA_DIR=$PWD/data uv run --project steps/extract uvicorn --app-dir steps/extract main:app --port 8001

# Terminal 2: step 2 API
DATA_DIR=$PWD/data uv run --project steps/transform uvicorn --app-dir steps/transform main:app --port 8002

# Terminal 3: the orchestrator
uv sync
uv run python flow.py
```

For step logs to appear in the Prefect UI, run a Prefect server and point everything at it:

```bash
# Terminal 0: Prefect server (UI at http://localhost:4200)
uv run prefect server start

# In each terminal (1-3), before starting the process:
export PREFECT_API_URL=http://localhost:4200/api
```

Without `PREFECT_API_URL`, the orchestrator uses a temporary server and the steps only log to stdout.

The flow defaults to `http://localhost:8001` and `http://localhost:8002`. Override them
with `EXTRACT_URL` and `TRANSFORM_URL` if needed.

## View logs in the Prefect UI

Each step logs its work to the Prefect API as well as stdout. Logs from the orchestrator
and from the steps appear together under the same flow run.

1. Open http://localhost:4200 and go to **Runs**, then `orders-pipeline`.
2. Open the run and select the **Logs** tab. You will see:
   - `prefect.flow_runs` and `prefect.task_runs`: orchestrator messages
   - `extract-step`: messages from step 1
   - `transform-step`: messages from step 2

The orchestrator passes its flow run ID and task run ID to each step in the request body.
The steps tag their logs with those IDs, so each log line is attached to the correct task.

Step logs also appear in the container output: `docker compose logs extract-step`.

## Check the result

```bash
cat data/processed/summary.json
```

Example output (fixed seed, so it is the same on every run):

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

## More details

- [Step 1: extract](steps/extract/README.md)
- [Step 2: transform](steps/transform/README.md)
