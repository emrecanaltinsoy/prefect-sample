"""Send Python log records to the Prefect API so they show up in the Prefect UI.

Usage:

    from prefect_log_handler import setup_logger

    logger = setup_logger("my-service")
    logger.info("hello")  # stdout, and the Prefect API if PREFECT_API_URL is set

Pass flow_run_id / task_run_id through a LoggerAdapter to attach records to a run:

    log = logging.LoggerAdapter(logger, {"flow_run_id": fid, "task_run_id": tid})
"""

from prefect_log_handler.handler import PrefectLogHandler, setup_logger

__all__ = ["PrefectLogHandler", "setup_logger"]
__version__ = "0.1.0"
