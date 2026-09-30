"""HTTP 层：三类拒绝可区分、幂等重放、分组状态与死信原因可查。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from taskdispatch.app import create_app
from taskdispatch.models import TaskState
from taskdispatch.service import TaskService
from taskdispatch.store import TaskStore

from .conftest import make_worker


@pytest.fixture
def client(store) -> TestClient:
    service = TaskService(store, max_attempts=3)
    return TestClient(create_app(store, service=service))


def test_dispatch_and_distinct_rejections(client: TestClient) -> None:
    r = client.post("/tasks", json={"group": "g", "key": "k", "payload": {"v": 1}})
    assert r.status_code == 201
    body = r.json()
    assert body["state"] == "PENDING" and body["duplicated"] is False

    r = client.post("/tasks", json={"group": "g", "key": "k", "payload": {"v": 1}})
    assert r.status_code == 409
    assert r.json()["reason"] == "key_occupied"
    assert r.json()["key"] == "k"

    r = client.post("/tasks", json={"group": "g", "key": "k", "payload": {"v": 2}})
    assert r.status_code == 409
    assert r.json()["reason"] == "payload_conflict"

    r = client.post("/tasks", json={"group": "g", "key": " ", "payload": {"v": 1}})
    assert r.status_code == 400 and r.json()["reason"] == "invalid_request"


def test_idempotent_replay_after_processing(store, client: TestClient) -> None:
    from taskdispatch.models import TaskState as _S

    client.post("/tasks", json={"group": "g", "key": "k", "payload": {"v": 1}})

    w = make_worker(store, "g", lambda record: "handled")
    w.run_until_idle(timeout=5)

    r = client.post("/tasks", json={"group": "g", "key": "k", "payload": {"v": 1}})
    assert r.status_code == 200
    body = r.json()
    assert body["duplicated"] is True
    assert body["state"] == _S.SUCCEEDED.value


def test_group_status_reports_counts_and_dead_reasons(store, client: TestClient) -> None:
    client.post("/tasks", json={"group": "g", "key": "ok", "payload": {}})
    client.post(
        "/tasks",
        json={"group": "g", "key": "bad", "payload": {}, "max_attempts": 1},
    )

    def handler(record):
        if record.key == "bad":
            raise RuntimeError("permanent-boom")
        return "fine"

    w = make_worker(store, "g", handler)
    w.run_until_idle(timeout=5)

    r = client.get("/groups/g")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert data["counts"]["SUCCEEDED"] == 1
    assert data["counts"]["DEAD"] == 1
    assert len(data["dead"]) == 1
    dead = data["dead"][0]
    assert dead["key"] == "bad"
    assert dead["attempts"] == 1
    assert "permanent-boom" in dead["last_error"]
    assert "retries exhausted" in dead["last_error"]


def test_health_aggregates_all_groups(client: TestClient) -> None:
    client.post("/tasks", json={"group": "a", "key": "k", "payload": {}})
    client.post("/tasks", json={"group": "b", "key": "k", "payload": {}})
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["states"]["PENDING"] == 2
