import os
import sys
from pathlib import Path

import pytest

# Make main.py importable and give it the S3 settings it requires at import time.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("S3_ACCESS_KEY", "test")
os.environ.setdefault("S3_SECRET_KEY", "test")


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[(Bucket, Key)] = Body


@pytest.fixture
def fake_s3(monkeypatch):
    import main

    s3 = FakeS3()
    monkeypatch.setattr(main, "S3", s3)
    return s3


@pytest.fixture
def client(fake_s3):
    from fastapi.testclient import TestClient

    import main

    return TestClient(main.app)
