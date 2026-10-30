# prefect-log-handler

A small, stdlib-only Python package that sends log records to the
[Prefect](https://docs.prefect.io/) API, so service logs appear in the Prefect UI.

- **No dependencies.** Only the standard library.
- **Non-blocking.** Records are queued and sent in batches by a background thread.
- **Optional.** Without `PREFECT_API_URL`, logs only go to stdout.
- **Run-aware.** Records carrying `flow_run_id` / `task_run_id` attach to that run and task.

## Install

From another uv project in this repo:

```bash
uv add ../../packages/prefect-log-handler
```

Or from git, once the repo is pushed:

```bash
uv add "prefect-log-handler @ git+https://<your-repo-url>#subdirectory=packages/prefect-log-handler"
```

## Use

```python
import logging
from prefect_log_handler import setup_logger

logger = setup_logger(
    "extract-step"
)  # stdout + Prefect API (if PREFECT_API_URL is set)
logger.info("Service started")

# Attach records to a flow run / task run (IDs passed in by the caller)
log = logging.LoggerAdapter(
    logger, {"flow_run_id": flow_run_id, "task_run_id": task_run_id}
)
log.info("Processing request")
```

## Configuration

| Variable          | Effect                                                              |
|-------------------|---------------------------------------------------------------------|
| `PREFECT_API_URL` | Prefect API base URL, e.g. `http://prefect-server:4200/api`. If unset, nothing is sent. |

## API

- `setup_logger(name) -> logging.Logger`: returns a logger with a stdout handler and, if
  `PREFECT_API_URL` is set, a `PrefectLogHandler`. Calling it again for the same name does not add duplicate handlers.
- `PrefectLogHandler(api_url, batch_size=100, flush_interval=0.5)`: the `logging.Handler` that
  posts batches to `{api_url}/logs/`.
