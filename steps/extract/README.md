# Step 1: Extract (REST service)

**Code:** [`main.py`](main.py) · **Image:** built from [`Dockerfile`](Dockerfile)
**Port:** `8000` inside the Compose network (no host port; run locally on `8001`/`8002`)

## What it does

Generates fake order records and stores them as CSV in MinIO, under a folder for the run:

```
s3://pipeline-data/runs/<run_id>/raw/orders.csv
```

In a real pipeline this is where you would call an API, read a database, or pull from storage.

- Columns: `order_id`, `customer_id`, `product`, `quantity`, `unit_price`, `order_date`.
- Uses a fixed seed by default, so the same input gives the same output.
- `run_id` comes from the request. If omitted, a new UUID is generated. The flow always passes its
  Prefect flow run ID, so parallel runs write to different folders.

## API

| Method | Path      | Description                                   |
|--------|-----------|-----------------------------------------------|
| GET    | `/health` | Liveness check, returns `{"status": "ok"}`   |
| POST   | `/run`    | Generate orders and store the CSV in MinIO    |

Interactive docs when running locally (see below): `http://localhost:8001/docs` (extract) or `http://localhost:8002/docs` (transform).

**Request** (`POST /run`):

```json
{ "num_orders": 100, "seed": 42, "run_id": "my-test-run" }
```

- `num_orders`: 1 to 100000 (default 100)
- `seed`: optional (default 42)
- `run_id`: optional, letters, digits, `-` and `_` only, up to 64 characters

**Response:**

```json
{ "run_id": "my-test-run", "object_key": "runs/my-test-run/raw/orders.csv", "order_count": 100 }
```

## Configuration

| Variable          | Default (Compose)                  | Purpose                       |
|-------------------|------------------------------------|-------------------------------|
| `S3_ENDPOINT_URL` | `http://minio:9000`                | MinIO / S3 endpoint           |
| `S3_ACCESS_KEY`   | from `.env` (`MINIO_ROOT_USER`)    | Access key, required          |
| `S3_SECRET_KEY`   | from `.env` (`MINIO_ROOT_PASSWORD`)| Secret key, required          |
| `S3_BUCKET`       | `pipeline-data`                    | Bucket to write to            |
| `PREFECT_API_URL` | `http://prefect-server:4200/api`   | Where logs are sent           |

## Logs

The service logs to stdout and sends each record to the Prefect API. Records are tagged with the
`flow_run_id` and `task_run_id` from the request, so they appear under that task in the Prefect UI.
Logging uses the shared [`prefect-log-handler`](../../packages/prefect-log-handler/README.md) package.

## Run

With Docker Compose (from the project root). MinIO and the Prefect server start as dependencies:

```bash
docker compose up --build -d extract-step
docker compose exec -T extract-step python -c "import urllib.request as u; r = u.Request('http://localhost:8000/run', data=b'{\"run_id\": \"manual-1\"}', headers={'Content-Type': 'application/json'}); print(u.urlopen(r).read().decode())"
```

Locally (from the project root, with MinIO on port 9000):

```bash
docker compose up -d minio
S3_ENDPOINT_URL=http://localhost:9000 S3_ACCESS_KEY=... S3_SECRET_KEY=... uv run --project steps/extract \
  uvicorn --app-dir steps/extract main:app --port 8001
curl -X POST http://localhost:8001/run -H "Content-Type: application/json" -d '{"run_id": "manual-1"}'
```

## Next step

[Step 2: transform](../transform/README.md) reads the object this step writes.
