from __future__ import annotations

import json
import os
import uuid
from collections import Counter
from datetime import UTC, datetime
from collections.abc import Mapping, Sequence
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.models import ErrorCode, RunStatus, TaskStatus, TERMINAL_RUN_STATUSES
from app.planner import build_task_plan
from app.schemas import CreateRunRequest


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TASK_QUEUE_KEY = "queue:tasks"
RUN_QUEUE_KEY = "queue:runs"
GLOBAL_RUNNING_KEY = "global:running_tasks"


class DuplicateActiveSeedError(Exception):
    def __init__(self, run_id: str, status: str) -> None:
        self.run_id = run_id
        self.status = status
        super().__init__(f"Active run {run_id} already exists for seed")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def get_redis() -> Redis:
    return Redis.from_url(REDIS_URL, decode_responses=True)


class RedisStorage:
    def __init__(
        self,
        redis: Redis,
        *,
        global_concurrency_limit: int = 1000,
    ) -> None:
        self.redis = redis
        self.global_concurrency_limit = global_concurrency_limit

    async def create_run(
        self,
        request: CreateRunRequest,
        *,
        execution_variations: Mapping[str, Mapping[str, str | None]]
        | Sequence[Mapping[str, str | None] | tuple[str, str | None]]
        | None = None,
        min_task_duration_seconds: int = 2,
        max_task_duration_seconds: int = 10,
        timeout_seconds: int = 11,
    ) -> dict[str, Any]:
        active_key = self._active_seed_key(request.seed)
        existing_run_id = await self.redis.get(active_key)
        if existing_run_id:
            existing_run = await self.get_run(existing_run_id)
            if existing_run and existing_run["status"] in {
                RunStatus.QUEUED.value,
                RunStatus.RUNNING.value,
            }:
                raise DuplicateActiveSeedError(existing_run_id, existing_run["status"])
            await self.redis.delete(active_key)

        run_id = str(uuid.uuid4())
        now = utc_now()
        run = {
            "run_id": run_id,
            "scenario": request.scenario,
            "count": request.count,
            "seed": request.seed,
            "max_concurrency": request.max_concurrency,
            "status": RunStatus.QUEUED.value,
            "error": None,
            "message": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "finished_at": None,
            "concurrency_released": False,
        }
        tasks = build_task_plan(
            run_id=run_id,
            scenario=request.scenario,
            run_seed=request.seed,
            count=request.count,
            execution_variations=execution_variations,
            min_task_duration_seconds=min_task_duration_seconds,
            max_task_duration_seconds=max_task_duration_seconds,
            timeout_seconds=timeout_seconds,
        )
        for task in tasks:
            task["created_at"] = now
            task["updated_at"] = now

        was_set = await self.redis.set(
            active_key,
            run_id,
            nx=True,
        )
        if not was_set:
            existing_run_id = await self.redis.get(active_key)
            existing_run = await self.get_run(existing_run_id) if existing_run_id else None
            if existing_run_id and existing_run:
                raise DuplicateActiveSeedError(existing_run_id, existing_run["status"])
            raise DuplicateActiveSeedError("unknown", RunStatus.QUEUED.value)

        try:
            pipe = self.redis.pipeline(transaction=True)
            pipe.set(
                self._run_key(run_id),
                self._dumps(run),
            )
            pipe.delete(self._run_tasks_key(run_id))
            for task in tasks:
                pipe.set(
                    self._task_key(task["task_id"]),
                    self._dumps(task),
                )
                pipe.rpush(self._run_tasks_key(run_id), task["task_id"])
                pipe.rpush(TASK_QUEUE_KEY, task["task_id"])
            pipe.rpush(RUN_QUEUE_KEY, run_id)
            pipe.set(self._run_running_key(run_id), 0)
            await pipe.execute()
        except Exception:
            await self.redis.delete(active_key)
            raise

        return run

    async def active_run_concurrency_total(self) -> int:
        total = 0
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match="run:*")
            for key in keys:
                if key.endswith(":tasks") or key.endswith(":running_tasks"):
                    continue
                raw = await self.redis.get(key)
                if not raw:
                    continue
                run = self._loads(raw)
                if run.get("status") in {RunStatus.QUEUED.value, RunStatus.RUNNING.value}:
                    total += int(run.get("max_concurrency", 0))
            if cursor == 0:
                break
        return total

    async def get_run(self, run_id: str | None) -> dict[str, Any] | None:
        if not run_id:
            return None
        raw = await self.redis.get(self._run_key(run_id))
        return self._loads(raw) if raw else None

    async def get_task(self, task_id: str) -> dict[str, Any] | None:
        raw = await self.redis.get(self._task_key(task_id))
        return self._loads(raw) if raw else None

    async def get_run_tasks(self, run_id: str) -> list[dict[str, Any]]:
        task_ids = await self.redis.lrange(self._run_tasks_key(run_id), 0, -1)
        tasks = []
        for task_id in task_ids:
            task = await self.get_task(task_id)
            if task:
                tasks.append(task)
        return sorted(tasks, key=lambda task: task["task_index"])

    async def update_run(self, run: dict[str, Any]) -> None:
        run["updated_at"] = utc_now()
        await self.redis.set(
            self._run_key(run["run_id"]),
            self._dumps(run),
        )

    async def update_task(self, task: dict[str, Any]) -> None:
        task["updated_at"] = utc_now()
        await self.redis.set(
            self._task_key(task["task_id"]),
            self._dumps(task),
        )

    async def enqueue_task(self, task_id: str) -> None:
        await self.redis.rpush(TASK_QUEUE_KEY, task_id)

    async def pop_task_id(self, timeout: int = 1) -> str | None:
        item = await self.redis.blpop(TASK_QUEUE_KEY, timeout=timeout)
        if not item:
            return None
        return item[1]

    async def acquire_slots(self, run_id: str, max_concurrency: int) -> bool:
        run_key = self._run_running_key(run_id)
        async with self.redis.pipeline() as pipe:
            while True:
                try:
                    await pipe.watch(GLOBAL_RUNNING_KEY, run_key)
                    global_running = int(await pipe.get(GLOBAL_RUNNING_KEY) or 0)
                    run_running = int(await pipe.get(run_key) or 0)
                    if (
                        global_running >= self.global_concurrency_limit
                        or run_running >= max_concurrency
                    ):
                        await pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.incr(GLOBAL_RUNNING_KEY)
                    pipe.incr(run_key)
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    async def release_slots(self, run_id: str) -> None:
        run_key = self._run_running_key(run_id)
        async with self.redis.pipeline() as pipe:
            while True:
                try:
                    await pipe.watch(GLOBAL_RUNNING_KEY, run_key)
                    global_running = int(await pipe.get(GLOBAL_RUNNING_KEY) or 0)
                    run_running = int(await pipe.get(run_key) or 0)
                    pipe.multi()
                    if global_running > 0:
                        pipe.decr(GLOBAL_RUNNING_KEY)
                    if run_running > 0:
                        pipe.decr(run_key)
                    await pipe.execute()
                    return
                except WatchError:
                    continue

    async def global_running_count(self) -> int:
        value = await self.redis.get(GLOBAL_RUNNING_KEY)
        return int(value or 0)

    async def run_running_count(self, run_id: str) -> int:
        value = await self.redis.get(self._run_running_key(run_id))
        return int(value or 0)

    async def summarize_running_tasks(self, run_id: str) -> dict[str, int]:
        summary = {
            "global_running": 0,
            "run_running": 0,
            "global_limit": self.global_concurrency_limit,
        }
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match="task:*")
            for key in keys:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                task = self._loads(raw)
                if task.get("status") != TaskStatus.RUNNING.value:
                    continue
                summary["global_running"] += 1
                if task.get("run_id") == run_id:
                    summary["run_running"] += 1
            if cursor == 0:
                break
        return summary

    async def summarize_run(self, run_id: str) -> dict[str, int]:
        tasks = await self.get_run_tasks(run_id)
        counts = Counter(task["status"] for task in tasks)
        return {
            TaskStatus.PENDING.value: counts[TaskStatus.PENDING.value],
            TaskStatus.RUNNING.value: counts[TaskStatus.RUNNING.value],
            TaskStatus.RETRYING.value: counts[TaskStatus.RETRYING.value],
            TaskStatus.COMPLETED.value: counts[TaskStatus.COMPLETED.value],
            TaskStatus.FAILED.value: counts[TaskStatus.FAILED.value],
        }

    async def refresh_run_terminal_state(self, run_id: str) -> None:
        run = await self.get_run(run_id)
        if not run or run["status"] in {status.value for status in TERMINAL_RUN_STATUSES}:
            return

        tasks = await self.get_run_tasks(run_id)
        if not tasks:
            return
        unfinished = [
            task
            for task in tasks
            if task["status"] not in {TaskStatus.COMPLETED.value, TaskStatus.FAILED.value}
        ]
        if unfinished:
            return

        failed = [task for task in tasks if task["status"] == TaskStatus.FAILED.value]
        run["finished_at"] = utc_now()
        if failed:
            run["status"] = RunStatus.FAILED.value
            run["error"] = ErrorCode.ONE_OR_MORE_TASKS_FAILED.value
            run["message"] = (
                f"Run failed because {len(failed)} task"
                f"{'' if len(failed) == 1 else 's'} failed after retry."
            )
        else:
            run["status"] = RunStatus.COMPLETED.value
            run["error"] = None
            run["message"] = "Run completed successfully."

        await self.update_run(run)
        await self.redis.delete(self._active_seed_key(run["seed"]))

    async def mark_run_concurrency_released(self, run_id: str) -> int | None:
        run_key = self._run_key(run_id)
        async with self.redis.pipeline() as pipe:
            while True:
                try:
                    await pipe.watch(run_key)
                    raw = await pipe.get(run_key)
                    if not raw:
                        await pipe.unwatch()
                        return None
                    run = self._loads(raw)
                    if (
                        run.get("status")
                        not in {RunStatus.COMPLETED.value, RunStatus.FAILED.value}
                        or run.get("concurrency_released")
                    ):
                        await pipe.unwatch()
                        return None
                    release_amount = int(run.get("max_concurrency", 0))
                    run["concurrency_released"] = True
                    run["updated_at"] = utc_now()
                    pipe.multi()
                    pipe.set(
                        run_key,
                        self._dumps(run),
                    )
                    await pipe.execute()
                    return release_amount
                except WatchError:
                    continue

    async def mark_run_running(self, run_id: str) -> None:
        run = await self.get_run(run_id)
        if not run or run["status"] != RunStatus.QUEUED.value:
            return
        now = utc_now()
        run["status"] = RunStatus.RUNNING.value
        run["started_at"] = now
        run["updated_at"] = now
        await self.redis.set(
            self._run_key(run_id),
            self._dumps(run),
        )

    async def repair_running_state(self) -> None:
        await self.redis.set(GLOBAL_RUNNING_KEY, 0)
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match="run:*:running_tasks")
            if keys:
                pipe = self.redis.pipeline()
                for key in keys:
                    pipe.set(key, 0)
                await pipe.execute()
            if cursor == 0:
                break

        affected_runs: set[str] = set()
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match="task:*")
            for key in keys:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                task = self._loads(raw)
                if task["status"] == TaskStatus.RUNNING.value:
                    if int(task["attempt"]) < 2:
                        task["status"] = TaskStatus.RETRYING.value
                        task["error"] = ErrorCode.UNKNOWN_ERROR.value
                        task["reason"] = "worker_restart"
                        task["message"] = "Task was interrupted by worker restart; retrying once."
                        await self.update_task(task)
                        await self.enqueue_task(task["task_id"])
                    else:
                        task["status"] = TaskStatus.FAILED.value
                        task["error"] = ErrorCode.UNKNOWN_ERROR.value
                        task["reason"] = "worker_restart"
                        task["message"] = "Task failed because worker restarted during final attempt."
                        task["finished_at"] = utc_now()
                        await self.update_task(task)
                    affected_runs.add(task["run_id"])
                elif task["status"] == TaskStatus.RETRYING.value:
                    await self.enqueue_task(task["task_id"])
            if cursor == 0:
                break

        for run_id in affected_runs:
            await self.refresh_run_terminal_state(run_id)

    def _run_key(self, run_id: str) -> str:
        return f"run:{run_id}"

    def _run_tasks_key(self, run_id: str) -> str:
        return f"run:{run_id}:tasks"

    def _task_key(self, task_id: str) -> str:
        return f"task:{task_id}"

    def _active_seed_key(self, seed: int) -> str:
        return f"seed:{seed}:active_run"

    def _run_running_key(self, run_id: str) -> str:
        return f"run:{run_id}:running_tasks"

    def _dumps(self, value: dict[str, Any]) -> str:
        return json.dumps(value, separators=(",", ":"))

    def _loads(self, value: str) -> dict[str, Any]:
        return json.loads(value)
