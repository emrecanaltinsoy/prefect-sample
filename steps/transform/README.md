# Step 2: Transform (REST service)

**Code:** [`main.py`](main.py) · **Image:** built from [`Dockerfile`](Dockerfile)
**Port:** `8000` inside the Compose network (no host port; run locally on `8001`/`8002`)

## What it does

Reads the raw orders for a run from MinIO and writes a revenue summary:

```
reads   s3://pipeline-data/runs/<run_id>/raw/orders.csv
writes  s3://pipeline-data/runs/<run_id>/processed/summary.json
```

The summary contains:

- total number of orders
- total revenue
- units sold and revenue per product, sorted by revenue (highest first)

Because the input is addressed by `run_id`, a request can only read that run's folder. Path
tricks such as `../` are not possible: `run_id` is checked against a pattern (letters, digits,
`-` and `_`).

## API

| Method | Path      | Description                                              |
|--------|-----------|----------------------------------------------------------|
| GET    | `/health` | Liveness check, returns `{"status": "ok"}`              |
| POST   | `/run`    | Read the run's orders from MinIO, write and return the summary |

Interactive docs when running locally (see below): `http://localhost:8001/docs` (extract) or `http://localhost:8002/docs` (transform).

**Request** (`POST /run`):

```json
{ "run_id": "my-test-run" }
```

**Response:**

```json
{
  "run_id": "my-test-run",
  "object_key": "runs/my-test-run/processed/summary.json",
  "order_count": 100,
  "total_revenue": 20220.0
}
```

Errors: `404` if the run has no `raw/orders.csv` (run the extract step first), `422` if `run_id`
has invalid characters.

## Configuration

Same as the extract step: `S3_ENDPOINT_URL`, `S3_ACCESS_KEY` and `S3_SECRET_KEY` (both required, from `.env`), `S3_BUCKET`, and
`PREFECT_API_URL`.

## Logs

Logs go to stdout and to the Prefect API, tagged with the flow run and task run, using the shared
[`prefect-log-handler`](../../packages/prefect-log-handler/README.md) package.

## Run

Run step 1 first, because step 2 needs its output. With Docker Compose (from the project root):

```bash
docker compose up --build -d extract-step transform-step
docker compose exec -T extract-step python -c "import urllib.request as u; r = u.Request('http://localhost:8000/run', data=b'{\"run_id\": \"manual-1\"}', headers={'Content-Type': 'application/json'}); print(u.urlopen(r).read().decode())"
docker compose exec -T transform-step python -c "import urllib.request as u; r = u.Request('http://localhost:8000/run', data=b'{\"run_id\": \"manual-1\"}', headers={'Content-Type': 'application/json'}); print(u.urlopen(r).read().decode())"
```

Locally (from the project root, with MinIO on port 9000 and step 1 running on port 8001):

```bash
docker compose up -d minio
S3_ENDPOINT_URL=http://localhost:9000 S3_ACCESS_KEY=... S3_SECRET_KEY=... uv run --project steps/transform \
  uvicorn --app-dir steps/transform main:app --port 8002
curl -X POST http://localhost:8001/run -H "Content-Type: application/json" -d '{"run_id": "manual-1"}'
curl -X POST http://localhost:8002/run -H "Content-Type: application/json" -d '{"run_id": "manual-1"}'
```

## Previous step

[Step 1: extract](../extract/README.md) creates the input object.
