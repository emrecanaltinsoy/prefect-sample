import json

CSV = """order_id,customer_id,product,quantity,unit_price,order_date
1,1,keyboard,2,45.0,2024-01-02
2,2,mouse,1,20.0,2024-01-03
3,3,keyboard,1,45.0,2024-01-04
"""


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_summarises_run(client, fake_s3):
    fake_s3.objects[("pipeline-data", "runs/run-1/raw/orders.csv")] = CSV.encode()

    resp = client.post("/run", json={"run_id": "run-1"})

    assert resp.status_code == 200
    assert resp.json() == {
        "run_id": "run-1",
        "object_key": "runs/run-1/processed/summary.json",
        "order_count": 3,
        "total_revenue": 155.0,
    }
    summary = json.loads(
        fake_s3.objects[("pipeline-data", "runs/run-1/processed/summary.json")]
    )
    assert summary["products"]["keyboard"] == {"units": 3, "revenue": 135.0}
    assert summary["products"]["mouse"] == {"units": 1, "revenue": 20.0}


def test_missing_input_is_404(client):
    assert client.post("/run", json={"run_id": "nope"}).status_code == 404


def test_rejects_unsafe_run_id(client):
    assert client.post("/run", json={"run_id": "a/../b"}).status_code == 422
