"""Reaper: marks flow runs stuck in 'Submitting' as Crashed.

If the Prefect server dies while the worker is submitting a run, that run stays in
Submitting forever, because nothing moves it on. A healthy submission takes seconds, so any
run that has been in Submitting longer than STUCK_AFTER_SECONDS is treated as dead.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateName,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
from prefect.states import Crashed

SUBMITTING = "Submitting"
STUCK_AFTER = timedelta(seconds=int(os.getenv("STUCK_AFTER_SECONDS", "600")))
INTERVAL_SECONDS = float(os.getenv("REAPER_INTERVAL_SECONDS", "60"))


def find_stuck(runs, now: datetime) -> list:
    """Runs whose current state started more than STUCK_AFTER ago."""
    return [
        r
        for r in runs
        if r.state and r.state.timestamp and now - r.state.timestamp > STUCK_AFTER
    ]


async def reap_once(client) -> list[str]:
    """Mark stuck runs as Crashed. Returns the IDs of the runs it changed."""
    flow_run_filter = FlowRunFilter(
        state=FlowRunFilterState(
            type=FlowRunFilterStateType(any_=[StateType.PENDING]),
            name=FlowRunFilterStateName(any_=[SUBMITTING]),
        )
    )
    runs = await client.read_flow_runs(
        flow_run_filter=flow_run_filter, limit=200
    )  # the API caps a page at 200
    stuck = find_stuck(runs, datetime.now(timezone.utc))
    for run in stuck:
        await client.set_flow_run_state(
            run.id,
            Crashed(
                message=(
                    f"Stuck in {SUBMITTING} for more than {int(STUCK_AFTER.total_seconds())}s; "
                    "marked crashed by the reaper."
                )
            ),
            force=True,
        )
    return [str(run.id) for run in stuck]


async def main() -> None:
    print(
        f"reaper: every {INTERVAL_SECONDS:.0f}s, stuck after {STUCK_AFTER.total_seconds():.0f}s",
        flush=True,
    )
    while True:
        try:
            async with get_client() as client:
                reaped = await reap_once(client)
            for run_id in reaped:
                print(f"reaper: marked {run_id} as Crashed", flush=True)
        except Exception as exc:  # keep running; the API may be restarting
            print(f"reaper: check failed: {exc}", flush=True)
        await asyncio.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
