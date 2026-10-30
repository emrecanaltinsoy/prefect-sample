"""Logging handler that ships records to the Prefect API.

Stdlib only. Records are queued and sent in batches by a background thread, so logging
never slows down a request. If PREFECT_API_URL is not set, nothing is sent.
"""

import json
import logging
import os
import queue
import sys
import threading
import time
import urllib.request
from datetime import UTC, datetime


class PrefectLogHandler(logging.Handler):
    def __init__(
        self, api_url: str, batch_size: int = 100, flush_interval: float = 0.5
    ):
        super().__init__()
        self.logs_url = f"{api_url.rstrip('/')}/logs/"
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._queue: queue.Queue = queue.Queue()
        threading.Thread(
            target=self._worker, daemon=True, name="prefect-log-shipper"
        ).start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._queue.put_nowait(
                {
                    "name": record.name,
                    "level": record.levelno,
                    "message": record.getMessage(),
                    "timestamp": datetime.fromtimestamp(
                        record.created, tz=UTC
                    ).isoformat(),
                    "flow_run_id": getattr(record, "flow_run_id", None),
                    "task_run_id": getattr(record, "task_run_id", None),
                }
            )
        except Exception:
            self.handleError(record)

    def _worker(self) -> None:
        while True:
            batch = [self._queue.get()]
            time.sleep(self.flush_interval)  # let a few more records accumulate
            while len(batch) < self.batch_size and not self._queue.empty():
                batch.append(self._queue.get_nowait())
            self._send(batch)

    def _send(self, batch: list[dict]) -> None:
        # Drop empty run ids so the API does not reject the batch.
        cleaned = [{k: v for k, v in item.items() if v is not None} for item in batch]
        req = urllib.request.Request(
            self.logs_url,
            data=json.dumps(cleaned).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=5).close()
        except (
            Exception
        ) as exc:  # do not use logging here (would loop back into this handler)
            print(
                f"[prefect-log-handler] could not send {len(batch)} log(s): {exc}",
                file=sys.stderr,
            )


def setup_logger(name: str) -> logging.Logger:
    """Logger that writes to stdout and, if PREFECT_API_URL is set, to the Prefect API."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s - %(message)s")
        )
        logger.addHandler(stream)
        api_url = os.getenv("PREFECT_API_URL")
        if api_url:
            logger.addHandler(PrefectLogHandler(api_url))
    return logger
