from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from app.main import create_app
from app.models import ErrorCode, PlannedOutcome, TaskStatus
from app.schemas import CreateRunRequest
from app.storage import RedisStorage
from app.worker import Worker


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def storage(redis) -> RedisStorage:
    return RedisStorage(redis)


@pytest.mark.asyncio
async def test_post_runs_creates_run_and_tasks(redis) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs",
            json={"scenario": "demo", "count": 2, "seed": 42, "max_concurrency": 2},
        )
        assert response.status_code == 200
        run_id = response.json()["run_id"]

        run_response = await client.get(f"/runs/{run_id}")
        assert run_response.status_code == 200
        assert run_response.json()["summary"]["pending"] == 2

        tasks_response = await client.get(f"/runs/{run_id}/tasks")
        assert tasks_response.status_code == 200
        assert len(tasks_response.json()["tasks"]) == 2


@pytest.mark.asyncio
async def test_duplicate_active_seed_returns_409(redis) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        payload = {"scenario": "a", "count": 2, "seed": 99, "max_concurrency": 1}
        first = await client.post("/runs", json=payload)
        second = await client.post(
            "/runs",
            json={**payload, "scenario": "b", "count": 5, "max_concurrency": 5},
        )

        assert first.status_code == 200
        assert second.status_code == 409
        body = second.json()
        assert body["error"] == "duplicate_active_seed"
        assert body["existing_run_id"] == first.json()["run_id"]

        storage = RedisStorage(redis)
        assert len(await storage.get_run_tasks(first.json()["run_id"])) == 2


@pytest.mark.asyncio
async def test_concurrency_slots_respect_per_run_and_global_limits(storage) -> None:
    run_a = await storage.create_run(
        CreateRunRequest(scenario="a", count=12, seed=1, max_concurrency=7)
    )
    run_b = await storage.create_run(
        CreateRunRequest(scenario="b", count=12, seed=2, max_concurrency=7)
    )

    assert [await storage.acquire_slots(run_a["run_id"], 7) for _ in range(7)] == [
        True,
    ] * 7
    assert await storage.acquire_slots(run_a["run_id"], 7) is False

    assert [await storage.acquire_slots(run_b["run_id"], 7) for _ in range(3)] == [
        True,
    ] * 3
    assert await storage.acquire_slots(run_b["run_id"], 7) is False
    assert await storage.global_running_count() == 10


@pytest.mark.asyncio
async def test_failed_task_retries_once_then_fails(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=3, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 0
    task["planned_first_attempt_outcome"] = PlannedOutcome.FAILED.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.FAILED.value
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.RETRYING.value
    assert task["attempt"] == 1

    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    run = await storage.get_run(run["run_id"])
    assert task["status"] == TaskStatus.FAILED.value
    assert task["attempt"] == 2
    assert task["error"] == ErrorCode.TASK_FAILED.value
    assert run["status"] == "failed"
    assert run["error"] == "one_or_more_tasks_failed"


@pytest.mark.asyncio
async def test_timeout_task_retries_once_then_completes(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=4, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 0
    task["planned_first_attempt_outcome"] = PlannedOutcome.TIMEOUT.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    await worker.process_once()
    await asyncio.gather(*worker.running)
    await worker.process_once()
    await asyncio.gather(*worker.running)

    task = await storage.get_task(task["task_id"])
    run = await storage.get_run(run["run_id"])
    assert task["status"] == TaskStatus.COMPLETED.value
    assert task["attempt"] == 2
    assert task["result"] == {"ok": True}
    assert run["status"] == "completed"


@pytest.mark.asyncio
async def test_insufficient_memory_blocks_task(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.memory_is_available", lambda: False)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=5, max_concurrency=1)
    )
    worker = Worker(storage, poll_timeout=0)

    assert await worker.process_once() is False
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    assert task["status"] == TaskStatus.PENDING.value
    assert task["blocked_reason"] == ErrorCode.INSUFFICIENT_MEMORY.value
    assert "available memory" in task["message"]
