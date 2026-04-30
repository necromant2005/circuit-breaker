from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from app.main import ACTIVE_RUN_CONCURRENCY_KEY, create_app
from app.models import ErrorCode, PlannedOutcome, TaskStatus
from app.schemas import CreateRunRequest
from app.storage import GLOBAL_RUNNING_KEY, TASK_QUEUE_KEY, RedisStorage
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


OUTCOME_MAP = {
    "success": PlannedOutcome.COMPLETED.value,
    "failed": PlannedOutcome.FAILED.value,
    "timeout": PlannedOutcome.TIMEOUT.value,
}


async def create_task_with_forced_plan(
    storage: RedisStorage,
    *,
    seed: int,
    attempt_1: str,
    attempt_2: str | None,
) -> tuple[dict, dict]:
    run = await storage.create_run(
        CreateRunRequest(scenario="deterministic", count=1, seed=seed, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 0
    task["retry_duration"] = 0
    task["planned_execution_case"] = f"forced_{attempt_1}_then_{attempt_2 or 'none'}"
    task["planned_first_attempt_outcome"] = OUTCOME_MAP[attempt_1]
    task["planned_retry_attempt_outcome"] = OUTCOME_MAP[attempt_2] if attempt_2 else None
    task["planned_result"] = {
        "scenario": "deterministic",
        "task_index": task["task_index"],
        "child_seed": task["child_seed"],
        "value": 1234,
    }
    task["result"] = None
    task["error"] = None
    task["reason"] = None
    await storage.update_task(task)
    return run, task


async def run_forced_task_to_terminal(
    storage: RedisStorage,
    task_id: str,
) -> tuple[dict, list[str]]:
    worker = Worker(storage, poll_timeout=0)
    transitions = []

    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task_id)
    transitions.append(task["status"])

    if task["status"] == TaskStatus.RETRYING.value:
        assert await worker.process_once() is True
        await asyncio.gather(*worker.running)
        task = await storage.get_task(task_id)
        transitions.append(task["status"])

    return task, transitions


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
        tasks = tasks_response.json()["tasks"]
        assert len(tasks) == 2


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
@pytest.mark.parametrize("missing_field", ["scenario", "count", "seed", "max_concurrency"])
async def test_post_runs_requires_all_fields(redis, missing_field: str) -> None:
    app = create_app(redis)
    payload = {
        "scenario": "demo",
        "count": 1,
        "seed": 10,
        "max_concurrency": 1,
    }
    payload.pop(missing_field)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/runs", json=payload)

    assert response.status_code == 422
    assert missing_field in str(response.json()["detail"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"scenario": "", "count": 1, "seed": 10, "max_concurrency": 1},
        {"scenario": "   ", "count": 1, "seed": 10, "max_concurrency": 1},
        {"scenario": 123, "count": 1, "seed": 10, "max_concurrency": 1},
        {"scenario": "demo", "count": 0, "seed": 10, "max_concurrency": 1},
        {"scenario": "demo", "count": "1", "seed": 10, "max_concurrency": 1},
        {"scenario": "demo", "count": 1, "seed": "10", "max_concurrency": 1},
        {"scenario": "demo", "count": 1, "seed": 10, "max_concurrency": 0},
        {"scenario": "demo", "count": 1, "seed": 10, "max_concurrency": "1"},
        {
            "scenario": "demo",
            "count": 1,
            "seed": 10,
            "max_concurrency": 1,
            "extra": "not allowed",
        },
    ],
)
async def test_post_runs_rejects_invalid_fields(redis, payload: dict) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/runs", json=payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_post_runs_rejects_max_concurrency_above_global_limit(redis) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs",
            json={
                "scenario": "demo",
                "count": 500,
                "seed": 45,
                "max_concurrency": 10000,
            },
        )

    assert response.status_code == 422
    assert "max_concurrency must be less than or equal to 1000" in response.json()["detail"]


@pytest.mark.asyncio
async def test_post_runs_rejects_overbooking_active_run_concurrency(redis) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        first = await client.post(
            "/runs",
            json={
                "scenario": "demo",
                "count": 500,
                "seed": 45,
                "max_concurrency": 1000,
            },
        )
        second = await client.post(
            "/runs",
            json={
                "scenario": "demo",
                "count": 500,
                "seed": 41,
                "max_concurrency": 1000,
            },
        )

    assert first.status_code == 200
    assert second.status_code == 409
    body = second.json()
    assert body["error"] == "global_concurrency_capacity_exceeded"
    assert body["requested"] == 1000
    assert body["reserved"] == 1000
    assert body["limit"] == 1000
    assert int(await redis.get(ACTIVE_RUN_CONCURRENCY_KEY)) == 1000


@pytest.mark.asyncio
async def test_finished_run_releases_reserved_concurrency(redis, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs",
            json={
                "scenario": "demo",
                "count": 1,
                "seed": 46,
                "max_concurrency": 1000,
            },
        )

    assert response.status_code == 200
    run_id = response.json()["run_id"]
    assert int(await redis.get(ACTIVE_RUN_CONCURRENCY_KEY)) == 1000

    storage = RedisStorage(redis)
    task = (await storage.get_run_tasks(run_id))[0]
    task["duration"] = 0
    task["planned_first_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)

    run = await storage.get_run(run_id)
    assert run["status"] == "completed"
    assert run["concurrency_released"] is True
    assert int(await redis.get(ACTIVE_RUN_CONCURRENCY_KEY)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/runs/not-a-uuid", "/runs/not-a-uuid/tasks"])
async def test_run_id_must_be_valid_uuid(redis, path: str) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(path)

    assert response.status_code == 422
    assert response.json()["detail"] == "run_id must be a valid UUID"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/runs/00000000-0000-0000-0000-000000000000",
        "/runs/00000000-0000-0000-0000-000000000000/tasks",
    ],
)
async def test_valid_missing_run_id_returns_404(redis, path: str) -> None:
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(path)

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_post_runs_rejects_oversized_record(redis, monkeypatch) -> None:
    monkeypatch.setattr("app.main.MAX_TASK_RECORD_SIZE_BYTES", 500)
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs",
            json={
                "scenario": "x" * 1000,
                "count": 1,
                "seed": 101,
                "max_concurrency": 1,
            },
        )

    assert response.status_code == 413
    assert response.json()["error"] == "post_input_too_large"


@pytest.mark.asyncio
async def test_post_input_size_counts_only_post_values(redis, monkeypatch) -> None:
    monkeypatch.setattr("app.main.MAX_TASK_RECORD_SIZE_BYTES", 10)
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs",
            json={
                "scenario": "abcd",
                "count": 12,
                "seed": 34,
                "max_concurrency": 5,
            },
        )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_post_runs_rejects_when_memory_is_unavailable(redis, monkeypatch) -> None:
    monkeypatch.setattr("app.main.memory_is_available", lambda threshold_mb: False)
    app = create_app(redis)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/runs",
            json={
                "scenario": "demo",
                "count": 1,
                "seed": 107,
                "max_concurrency": 1,
            },
        )

    assert response.status_code == 503
    assert response.json()["error"] == ErrorCode.INSUFFICIENT_MEMORY.value
    assert await redis.keys("run:*") == []
    assert await redis.llen(TASK_QUEUE_KEY) == 0
    assert await redis.get(ACTIVE_RUN_CONCURRENCY_KEY) is None


@pytest.mark.asyncio
async def test_concurrency_slots_respect_per_run_and_global_limits(storage) -> None:
    run_a = await storage.create_run(
        CreateRunRequest(scenario="a", count=12, seed=1, max_concurrency=7)
    )
    run_b = await storage.create_run(
        CreateRunRequest(scenario="b", count=12, seed=2, max_concurrency=7)
    )
    limited_storage = RedisStorage(storage.redis, global_concurrency_limit=10)

    assert [await limited_storage.acquire_slots(run_a["run_id"], 7) for _ in range(7)] == [
        True,
    ] * 7
    assert await limited_storage.acquire_slots(run_a["run_id"], 7) is False

    assert [await limited_storage.acquire_slots(run_b["run_id"], 7) for _ in range(3)] == [
        True,
    ] * 3
    assert await limited_storage.acquire_slots(run_b["run_id"], 7) is False
    assert await limited_storage.global_running_count() == 10


@pytest.mark.asyncio
async def test_summarize_running_tasks_counts_global_and_run_running(storage) -> None:
    run_a = await storage.create_run(
        CreateRunRequest(scenario="a", count=2, seed=103, max_concurrency=2)
    )
    run_b = await storage.create_run(
        CreateRunRequest(scenario="b", count=1, seed=104, max_concurrency=1)
    )
    for task in await storage.get_run_tasks(run_a["run_id"]):
        task["status"] = TaskStatus.RUNNING.value
        await storage.update_task(task)
    task_b = (await storage.get_run_tasks(run_b["run_id"]))[0]
    task_b["status"] = TaskStatus.RUNNING.value
    await storage.update_task(task_b)

    summary = await storage.summarize_running_tasks(run_a["run_id"])

    assert summary["global_running"] == 3
    assert summary["run_running"] == 2
    assert summary["global_limit"] == 1000


@pytest.mark.asyncio
async def test_global_subtask_process_limit_returns_early_from_summary(redis) -> None:
    storage = RedisStorage(redis)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=2, seed=105, max_concurrency=2)
    )
    limited_storage = RedisStorage(redis, global_concurrency_limit=1)
    running_task, pending_task = await storage.get_run_tasks(run["run_id"])
    running_task["status"] = TaskStatus.RUNNING.value
    await storage.update_task(running_task)

    worker = Worker(limited_storage, poll_timeout=0)
    assert await worker.process_once() is False
    assert await worker.process_once() is False

    pending_task = await limited_storage.get_task(pending_task["task_id"])
    run = await limited_storage.get_run(run["run_id"])
    assert pending_task["status"] == TaskStatus.PENDING.value
    assert pending_task["blocked_reason"] == ErrorCode.GLOBAL_CONCURRENCY_LIMIT.value
    assert "1 running processes" in pending_task["message"]
    assert run["status"] == "queued"


@pytest.mark.asyncio
async def test_run_concurrency_limit_returns_early_from_summary(storage) -> None:
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=2, seed=106, max_concurrency=1)
    )
    running_task, pending_task = await storage.get_run_tasks(run["run_id"])
    running_task["status"] = TaskStatus.RUNNING.value
    await storage.update_task(running_task)

    worker = Worker(storage, poll_timeout=0)
    assert await worker.process_once() is False
    assert await worker.process_once() is False

    pending_task = await storage.get_task(pending_task["task_id"])
    assert pending_task["status"] == TaskStatus.PENDING.value
    assert pending_task["blocked_reason"] == "run_concurrency_limit"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempt_1,attempt_2,expected_status,expected_attempts,expected_error",
    [
        ("success", None, "completed", 1, None),
        ("failed", "success", "completed", 2, None),
        ("timeout", "success", "completed", 2, None),
        ("failed", "failed", "failed", 2, "task_failed"),
        ("failed", "timeout", "failed", 2, "task_timeout"),
        ("timeout", "failed", "failed", 2, "task_failed"),
        ("timeout", "timeout", "failed", 2, "task_timeout"),
    ],
)
async def test_task_execution_variations(
    storage,
    monkeypatch,
    attempt_1: str,
    attempt_2: str | None,
    expected_status: str,
    expected_attempts: int,
    expected_error: str | None,
) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    _run, initial_task = await create_task_with_forced_plan(
        storage,
        seed=2000 + expected_attempts + len(attempt_1) + len(attempt_2 or ""),
        attempt_1=attempt_1,
        attempt_2=attempt_2,
    )

    task, transitions = await run_forced_task_to_terminal(storage, initial_task["task_id"])

    assert task["status"] == expected_status
    assert task["attempt"] == expected_attempts
    assert task["error"] == expected_error
    assert task["planned_first_attempt_outcome"] == OUTCOME_MAP[attempt_1]
    assert task["planned_retry_attempt_outcome"] == (OUTCOME_MAP[attempt_2] if attempt_2 else None)
    assert task["message"]

    if expected_status == TaskStatus.COMPLETED.value:
        assert task["result"] == initial_task["planned_result"]
        assert task["error"] is None
    else:
        assert task["result"] is None

    if expected_attempts == 1:
        assert transitions == [TaskStatus.COMPLETED.value]
        assert task["reason"] is None
        assert await storage.redis.llen(TASK_QUEUE_KEY) == 0
    else:
        assert transitions[0] == TaskStatus.RETRYING.value
        assert transitions[-1] == expected_status
        if expected_status == TaskStatus.COMPLETED.value:
            assert task["reason"] == OUTCOME_MAP[attempt_1]
        else:
            assert task["reason"] == OUTCOME_MAP[attempt_2]

    if expected_status == TaskStatus.FAILED.value:
        assert task["finished_at"] is not None
        assert "failed" in task["message"].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempt_1",
    ["failed", "timeout"],
)
async def test_successful_retry_records_first_failure_reason(
    storage,
    monkeypatch,
    attempt_1: str,
) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    _run, initial_task = await create_task_with_forced_plan(
        storage,
        seed=3000 + len(attempt_1),
        attempt_1=attempt_1,
        attempt_2="success",
    )

    worker = Worker(storage, poll_timeout=0)
    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)
    retrying_task = await storage.get_task(initial_task["task_id"])
    assert retrying_task["status"] == TaskStatus.RETRYING.value
    assert retrying_task["reason"] == OUTCOME_MAP[attempt_1]
    assert retrying_task["error"] in {ErrorCode.TASK_FAILED.value, ErrorCode.TASK_TIMEOUT.value}

    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)
    completed_task = await storage.get_task(initial_task["task_id"])
    assert completed_task["status"] == TaskStatus.COMPLETED.value
    assert completed_task["attempt"] == 2
    assert completed_task["result"] == initial_task["planned_result"]
    assert completed_task["error"] is None
    assert completed_task["reason"] == OUTCOME_MAP[attempt_1]


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
async def test_failed_task_can_succeed_on_retry(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=8, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 0
    task["retry_duration"] = 0
    task["planned_first_attempt_outcome"] = PlannedOutcome.FAILED.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    await worker.process_once()
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.RETRYING.value
    assert task["error"] == ErrorCode.TASK_FAILED.value

    await worker.process_once()
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    run = await storage.get_run(run["run_id"])
    assert task["status"] == TaskStatus.COMPLETED.value
    assert task["attempt"] == 2
    assert task["result"] == {"ok": True}
    assert run["status"] == "completed"


@pytest.mark.asyncio
async def test_planner_timeout_outcome_is_not_caused_by_short_duration(storage) -> None:
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=3, seed=4, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[2]
    assert task["planned_first_attempt_outcome"] == PlannedOutcome.TIMEOUT.value
    assert task["duration"] == 11


@pytest.mark.asyncio
async def test_planned_timeout_attempt_runs_until_kill_timeout(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.MAX_SUBPROCESS_TIMEOUT_SECONDS", 11)
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    worker = Worker(storage, poll_timeout=0)

    with pytest.raises(TimeoutError):
        await worker._run_subprocess_simulation(
            {
                "attempt": 1,
                "duration": 2,
                "planned_first_attempt_outcome": PlannedOutcome.TIMEOUT.value,
            }
        )

    assert sleeps == [11]


@pytest.mark.asyncio
async def test_success_attempt_uses_planned_two_to_ten_second_duration(
    storage,
    monkeypatch,
) -> None:
    monkeypatch.setattr("app.worker.MAX_SUBPROCESS_TIMEOUT_SECONDS", 11)
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    worker = Worker(storage, poll_timeout=0)
    await worker._run_subprocess_simulation(
        {
            "attempt": 1,
            "duration": 2,
            "planned_first_attempt_outcome": PlannedOutcome.COMPLETED.value,
        }
    )

    assert sleeps == [2]


@pytest.mark.asyncio
async def test_retry_waits_before_requeue(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)
    monkeypatch.setattr("app.worker.RETRY_DELAY_SECONDS", 1)
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=108, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 2
    task["planned_first_attempt_outcome"] = PlannedOutcome.FAILED.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)

    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.RETRYING.value
    assert sleeps == [2, 1]


@pytest.mark.asyncio
async def test_retry_delay_increases_with_failed_count(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)
    monkeypatch.setattr("app.worker.RETRY_DELAY_SECONDS", 2)
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    worker = Worker(storage, poll_timeout=0)

    await worker._delay_before_retry(failed_count=1)
    await worker._delay_before_retry(failed_count=2)

    assert sleeps == [2, 4]


@pytest.mark.asyncio
async def test_configured_retry_count_allows_more_attempts(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 0)
    monkeypatch.setattr("app.worker.MAX_TASK_RETRIES", 2)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=109, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 2
    task["retry_duration"] = 2
    task["planned_first_attempt_outcome"] = PlannedOutcome.FAILED.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.FAILED.value
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    for expected_attempt in (1, 2):
        assert await worker.process_once() is True
        await asyncio.gather(*worker.running)
        task = await storage.get_task(task["task_id"])
        assert task["status"] == TaskStatus.RETRYING.value
        assert task["attempt"] == expected_attempt

    assert await worker.process_once() is True
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.FAILED.value
    assert task["attempt"] == 3
    assert task["error"] == ErrorCode.TASK_FAILED.value


@pytest.mark.asyncio
async def test_subprocess_timeout_kills_and_retries_once(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.MAX_SUBPROCESS_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=6, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 1
    task["planned_first_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    await worker.process_once()
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.RETRYING.value
    assert task["error"] == ErrorCode.TASK_TIMEOUT.value
    assert task["reason"] == PlannedOutcome.TIMEOUT.value

    await worker.process_once()
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    run = await storage.get_run(run["run_id"])
    assert task["status"] == TaskStatus.FAILED.value
    assert task["attempt"] == 2
    assert task["error"] == ErrorCode.TASK_TIMEOUT.value
    assert run["status"] == "failed"


@pytest.mark.asyncio
async def test_subprocess_timeout_can_succeed_on_retry(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.MAX_SUBPROCESS_TIMEOUT_SECONDS", 11)
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=9, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 11
    task["retry_duration"] = 1
    task["planned_first_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_retry_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    await worker.process_once()
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.RETRYING.value
    assert task["error"] == ErrorCode.TASK_TIMEOUT.value
    assert task["reason"] == PlannedOutcome.TIMEOUT.value

    await worker.process_once()
    await asyncio.gather(*worker.running)
    task = await storage.get_task(task["task_id"])
    run = await storage.get_run(run["run_id"])
    assert task["status"] == TaskStatus.COMPLETED.value
    assert task["attempt"] == 2
    assert task["result"] == {"ok": True}
    assert run["status"] == "completed"


@pytest.mark.asyncio
async def test_subprocess_timeout_allows_exact_limit(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.MAX_SUBPROCESS_TIMEOUT_SECONDS", 11)
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=7, max_concurrency=1)
    )
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    task["duration"] = 10
    task["planned_first_attempt_outcome"] = PlannedOutcome.COMPLETED.value
    task["planned_result"] = {"ok": True}
    await storage.update_task(task)

    worker = Worker(storage, poll_timeout=0)
    await worker.process_once()
    await asyncio.gather(*worker.running)

    task = await storage.get_task(task["task_id"])
    assert task["status"] == TaskStatus.COMPLETED.value
    assert task["error"] is None
    assert 10 in sleeps


@pytest.mark.asyncio
async def test_subprocess_timeout_kills_after_allowed_limit(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.MAX_SUBPROCESS_TIMEOUT_SECONDS", 11)
    monkeypatch.setattr("app.worker.TASK_DURATION_SCALE", 1)
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        return None

    monkeypatch.setattr("app.worker.asyncio.sleep", fake_sleep)
    worker = Worker(storage, poll_timeout=0)

    with pytest.raises(TimeoutError):
        await worker._run_subprocess_simulation({"duration": 11})

    assert sleeps == [11]


@pytest.mark.asyncio
async def test_insufficient_memory_blocks_task(storage, monkeypatch) -> None:
    monkeypatch.setattr("app.worker.memory_is_available", lambda threshold_mb: False)
    run = await storage.create_run(
        CreateRunRequest(scenario="demo", count=1, seed=5, max_concurrency=1)
    )
    worker = Worker(storage, poll_timeout=0)

    assert await worker.process_once() is False
    task = (await storage.get_run_tasks(run["run_id"]))[0]
    assert task["status"] == TaskStatus.PENDING.value
    assert task["blocked_reason"] == ErrorCode.INSUFFICIENT_MEMORY.value
    assert "available memory" in task["message"]
