def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_run_writes_csv_under_run_folder(client, fake_s3):
    resp = client.post("/run", json={"run_id": "run-1", "num_orders": 10})

    assert resp.status_code == 200
    assert resp.json() == {
        "run_id": "run-1",
        "object_key": "runs/run-1/raw/orders.csv",
        "order_count": 10,
    }
    body = fake_s3.objects[("pipeline-data", "runs/run-1/raw/orders.csv")].decode()
    lines = body.strip().splitlines()
    assert lines[0].startswith("order_id,customer_id,product")
    assert len(lines) == 11  # header + 10 orders


def test_same_seed_gives_same_output(client, fake_s3):
    client.post("/run", json={"run_id": "a", "num_orders": 20, "seed": 7})
    client.post("/run", json={"run_id": "b", "num_orders": 20, "seed": 7})

    assert (
        fake_s3.objects[("pipeline-data", "runs/a/raw/orders.csv")]
        == fake_s3.objects[("pipeline-data", "runs/b/raw/orders.csv")]
    )


def test_generates_run_id_when_missing(client):
    resp = client.post("/run", json={"num_orders": 3})
    assert resp.status_code == 200
    run_id = resp.json()["run_id"]
    assert resp.json()["object_key"] == f"runs/{run_id}/raw/orders.csv"


def test_rejects_unsafe_run_id(client):
    assert client.post("/run", json={"run_id": "../etc"}).status_code == 422


def test_rejects_zero_orders(client):
    assert client.post("/run", json={"num_orders": 0}).status_code == 422
