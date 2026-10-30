import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import reaper


def run_in_state(age: timedelta):
    """A flow run whose current state started `age` ago."""
    return SimpleNamespace(
        id=uuid.uuid4(),
        state=SimpleNamespace(timestamp=datetime.now(timezone.utc) - age),
    )


def test_find_stuck_keeps_only_old_runs():
    now = datetime.now(timezone.utc)
    old = run_in_state(timedelta(hours=1))
    fresh = run_in_state(timedelta(seconds=5))
    no_state = SimpleNamespace(id=uuid.uuid4(), state=None)

    assert reaper.find_stuck([old, fresh, no_state], now) == [old]


class FakeClient:
    def __init__(self, runs):
        self.runs = runs
        self.states = {}

    async def read_flow_runs(self, flow_run_filter, limit):
        return self.runs

    async def set_flow_run_state(self, flow_run_id, state, force=False):
        self.states[flow_run_id] = state


@pytest.mark.asyncio
async def test_reap_once_marks_only_stuck_runs_crashed():
    stuck = run_in_state(timedelta(hours=1))
    fresh = run_in_state(timedelta(seconds=5))
    client = FakeClient([stuck, fresh])

    reaped = await reaper.reap_once(client)

    assert reaped == [str(stuck.id)]
    assert list(client.states) == [stuck.id]
    assert client.states[stuck.id].type.value == "CRASHED"
