# Step 1: Extract (REST service)

**Code:** [`main.py`](main.py) · **Image:** built from [`Dockerfile`](Dockerfile)
**Port:** `8000` in the container, `8001` on the host (Compose)

## What it does

Generates fake order records and writes them to `/data/raw/orders.csv` on the shared
volume. In a real pipeline this is where you would call an API, read a database, or
pull from object storage.

- Creates `order_id`, `customer_id`, `product`, `quantity`, `unit_price`, `order_date`.
- Uses a fixed seed by default, so output is reproducible.

## API

| Method | Path      | Description                                   |
|--------|-----------|-----------------------------------------------|
| GET    | `/health` | Liveness check, returns `{"status": "ok"}`   |
| POST   | `/run`    | Generate orders and write the CSV             |

Interactive docs: http://localhost:8001/docs (with the stack running).

**Request** (`POST /run`):

```json
{ "num_orders": 100, "seed": 42 }
```

`num_orders` is 1 to 100000 (default 100). `seed` is optional (default 42).

**Response:**

```json
{ "output_path": "/data/raw/orders.csv", "order_count": 100 }
```

## Logs

The service logs to stdout

Logging uses the shared [`prefect-log-handler`](../../packages/prefect-log-handler/README.md) package.

 and sends each record to the Prefect API (`PREFECT_API_URL`).
Records are tagged with the `flow_run_id` and `task_run_id` from the request, so they appear
under that task in the Prefect UI. Logging is stdlib-only and sent in the background, so it
does not slow requests. Without `PREFECT_API_URL`, logs only go to stdout.

Typical messages: `Generating N orders (seed=S)` and `Wrote N orders to ...`.

## Run

With Docker Compose (from the project root):

```bash
docker compose up --build extract-step
curl -X POST http://localhost:8001/run -H "Content-Type: application/json" -d '{}'
```

Locally (from the project root):

```bash
DATA_DIR=$PWD/data uv run --project steps/extract uvicorn --app-dir steps/extract main:app --port 8001
```

## Next step

[Step 2: transform](../transform/README.md) reads the file this step writes.
