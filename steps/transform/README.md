# Step 2: Transform (REST service)

**Code:** [`main.py`](main.py) · **Image:** built from [`Dockerfile`](Dockerfile)
**Port:** `8000` in the container, `8002` on the host (Compose)

## What it does

Reads a raw orders CSV from the shared volume and writes a revenue summary:

- total number of orders
- total revenue
- units sold and revenue per product, sorted by revenue (highest first)

Output goes to `/data/processed/summary.json`.

For safety, `input_path` must be inside the data directory. Requests for other paths
return `400`.

## API

| Method | Path      | Description                                         |
|--------|-----------|-----------------------------------------------------|
| GET    | `/health` | Liveness check, returns `{"status": "ok"}`         |
| POST   | `/run`    | Read the CSV, write the summary, return its path    |

Interactive docs: http://localhost:8002/docs (with the stack running).

**Request** (`POST /run`):

```json
{ "input_path": "/data/raw/orders.csv" }
```

**Response:**

```json
{ "output_path": "/data/processed/summary.json", "order_count": 100, "total_revenue": 20220.0 }
```

Errors: `400` if the path is outside the data directory, `404` if the file does not exist.

## Logs

The service logs to stdout

Logging uses the shared [`prefect-log-handler`](../../packages/prefect-log-handler/README.md) package.

 and sends each record to the Prefect API (`PREFECT_API_URL`).
Records are tagged with the `flow_run_id` and `task_run_id` from the request, so they appear
under that task in the Prefect UI. Logging is stdlib-only and sent in the background, so it
does not slow requests. Without `PREFECT_API_URL`, logs only go to stdout.

Typical messages: `Reading orders from ...`, `Aggregated N orders ...`, and `Wrote summary to ...`.
Rejected or missing input files are logged at ERROR level.

## Run

Run step 1 first, because step 2 needs its output.

With Docker Compose (from the project root):

```bash
docker compose up --build extract-step transform-step
curl -X POST http://localhost:8001/run -H "Content-Type: application/json" -d '{}'
curl -X POST http://localhost:8002/run -H "Content-Type: application/json" \
     -d '{"input_path": "/data/raw/orders.csv"}'
```

Locally (from the project root, with step 1 running on port 8001):

```bash
DATA_DIR=$PWD/data uv run --project steps/transform uvicorn --app-dir steps/transform main:app --port 8002
curl -X POST http://localhost:8002/run -H "Content-Type: application/json" \
     -d "{\"input_path\": \"$PWD/data/raw/orders.csv\"}"
```

## Previous step

[Step 1: extract](../extract/README.md) creates the input file.
