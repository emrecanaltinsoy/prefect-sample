import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from prefect.exceptions import ObjectNotFound

import main

DEPLOYMENT_ID = uuid.uuid4()


class FakePrefect:
    def __init__(self):
        self.created = []  # (parameters, idempotency_key) per create call
        self.runs = {}
        self.deployment_exists = True

    async def read_deployment_by_name(self, name):
        if not self.deployment_exists:
            raise ObjectNotFound(http_exc=None)
        return SimpleNamespace(id=DEPLOYMENT_ID)

    async def create_flow_run_from_deployment(
        self, deployment_id, parameters, idempotency_key=None
    ):
        run_id = uuid.uuid4()
        self.created.append((parameters, idempotency_key))
        run = SimpleNamespace(id=run_id, state=SimpleNamespace(name="Scheduled"))
        self.runs[run_id] = run
        return run

    async def read_flow_run(self, flow_run_id):
        if flow_run_id not in self.runs:
            raise ObjectNotFound(http_exc=None)
        now = datetime.now(timezone.utc)
        return SimpleNamespace(
            id=flow_run_id,
            name="agate-tamarin",
            state=SimpleNamespace(name="Completed", message=None),
            created=now,
            start_time=now,
            end_time=now,
        )


@pytest.fixture
def fake_prefect(monkeypatch):
    fake = FakePrefect()

    @asynccontextmanager
    async def fake_client():
        yield fake

    monkeypatch.setattr(main, "get_client", fake_client)
    return fake


@pytest.fixture
def client(fake_prefect):
    return TestClient(main.app)


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_create_pipeline_returns_202_with_run(client, fake_prefect):
    resp = client.post("/pipelines", json={"num_orders": 100})

    assert resp.status_code == 202
    body = resp.json()
    assert body["state"] == "Scheduled"
    assert body["ui_url"].endswith(f"/runs/flow-run/{body['flow_run_id']}")
    assert fake_prefect.created == [({"num_orders": 100}, None)]


def test_idempotency_key_is_passed_to_prefect(client, fake_prefect):
    client.post(
        "/pipelines", json={"num_orders": 5}, headers={"Idempotency-Key": "order-42"}
    )

    assert fake_prefect.created == [({"num_orders": 5}, "order-42")]


def test_rejects_invalid_num_orders(client):
    assert client.post("/pipelines", json={"num_orders": 0}).status_code == 422


def test_missing_deployment_is_503(client, fake_prefect):
    fake_prefect.deployment_exists = False
    assert client.post("/pipelines", json={"num_orders": 5}).status_code == 503


def test_get_pipeline_returns_status(client):
    run_id = client.post("/pipelines", json={"num_orders": 5}).json()["flow_run_id"]

    resp = client.get(f"/pipelines/{run_id}")

    assert resp.status_code == 200
    assert resp.json()["state"] == "Completed"


def test_get_unknown_pipeline_is_404(client):
    assert client.get(f"/pipelines/{uuid.uuid4()}").status_code == 404
