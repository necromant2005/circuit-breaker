from __future__ import annotations

import asyncio
import os
from typing import Any

import psutil

from app.models import ErrorCode, PlannedOutcome, TaskStatus
from app.storage import GLOBAL_CONCURRENCY_LIMIT, RedisStorage, get_redis, utc_now


MIN_AVAILABLE_MEMORY_MB = int(os.getenv("MIN_AVAILABLE_MEMORY_MB", "256"))
TASK_DURATION_SCALE = float(os.getenv("TASK_DURATION_SCALE", "1.0"))


def available_memory_mb() -> float:
    return psutil.virtual_memory().available / 1024 / 1024


def memory_is_available(threshold_mb: int = MIN_AVAILABLE_MEMORY_MB) -> bool:
    return available_memory_mb() >= threshold_mb


class Worker:
    def __init__(self, storage: RedisStorage, *, poll_timeout: int = 1) -> None:
        self.storage = storage
        self.poll_timeout = poll_timeout
        self.running: set[asyncio.Task[None]] = set()
        self._stopping = False

    async def run_forever(self) -> None:
        await self.storage.repair_running_state()
        while not self._stopping:
            await self.process_once()
            self._cleanup_finished_tasks()

    async def stop(self) -> None:
        self._stopping = True
        if self.running:
            await asyncio.gather(*self.running, return_exceptions=True)

    async def process_once(self) -> bool:
        task_id = await self.storage.pop_task_id(timeout=self.poll_timeout)
        if task_id is None:
            return False

        task = await self.storage.get_task(task_id)
        if not task or task["status"] in {
            TaskStatus.COMPLETED.value,
            TaskStatus.FAILED.value,
        }:
            return False

        run = await self.storage.get_run(task["run_id"])
        if not run:
            return False

        if not await self._can_start_task(run, task):
            await self.storage.enqueue_task(task_id)
            await asyncio.sleep(0.2)
            return False

        task_runner = asyncio.create_task(self._execute_task(run, task))
        self.running.add(task_runner)
        task_runner.add_done_callback(self.running.discard)
        return True

    async def _can_start_task(self, run: dict[str, Any], task: dict[str, Any]) -> bool:
        global_running = await self.storage.global_running_count()
        if global_running >= GLOBAL_CONCURRENCY_LIMIT:
            task["blocked_reason"] = ErrorCode.GLOBAL_CONCURRENCY_LIMIT.value
            task["message"] = (
                "Task is waiting because the global concurrency limit of "
                f"{GLOBAL_CONCURRENCY_LIMIT} running tasks has been reached."
            )
            await self.storage.update_task(task)
            return False

        run_running = await self.storage.run_running_count(run["run_id"])
        if run_running >= int(run["max_concurrency"]):
            task["blocked_reason"] = "run_concurrency_limit"
            task["message"] = (
                "Task is waiting because the run concurrency limit of "
                f"{run['max_concurrency']} running tasks has been reached."
            )
            await self.storage.update_task(task)
            return False

        acquired = await self.storage.acquire_slots(run["run_id"], int(run["max_concurrency"]))
        if not acquired:
            task["blocked_reason"] = ErrorCode.GLOBAL_CONCURRENCY_LIMIT.value
            task["message"] = "Task is waiting for an available execution slot."
            await self.storage.update_task(task)
            return False

        if not memory_is_available():
            await self.storage.release_slots(run["run_id"])
            task["blocked_reason"] = ErrorCode.INSUFFICIENT_MEMORY.value
            task["message"] = (
                "Task is waiting because available memory is below "
                f"{MIN_AVAILABLE_MEMORY_MB} MB."
            )
            await self.storage.update_task(task)
            return False

        return True

    async def _execute_task(self, run: dict[str, Any], task: dict[str, Any]) -> None:
        task["blocked_reason"] = None
        task["status"] = TaskStatus.RUNNING.value
        task["attempt"] = int(task["attempt"]) + 1
        task["started_at"] = task["started_at"] or utc_now()
        task["message"] = f"Task attempt {task['attempt']} is running."
        await self.storage.update_task(task)
        await self.storage.mark_run_running(run["run_id"])

        try:
            await asyncio.sleep(float(task["duration"]) * TASK_DURATION_SCALE)
            outcome = self._outcome_for_attempt(task)
            if outcome == PlannedOutcome.COMPLETED.value:
                task["status"] = TaskStatus.COMPLETED.value
                task["result"] = task["planned_result"]
                task["error"] = None
                task["reason"] = None
                task["message"] = "Task completed successfully."
                task["finished_at"] = utc_now()
                await self.storage.update_task(task)
                return

            task["error"] = self._error_for_outcome(outcome)
            task["reason"] = outcome
            if int(task["attempt"]) < 2:
                task["status"] = TaskStatus.RETRYING.value
                task["message"] = (
                    f"Task attempt {task['attempt']} ended with {outcome}; retrying once."
                )
                await self.storage.update_task(task)
                await self.storage.enqueue_task(task["task_id"])
                return

            task["status"] = TaskStatus.FAILED.value
            task["message"] = (
                f"Task failed after {task['attempt']} attempts. Last reason: {outcome}."
            )
            task["finished_at"] = utc_now()
            await self.storage.update_task(task)
        except Exception as exc:
            task["error"] = ErrorCode.UNKNOWN_ERROR.value
            task["reason"] = type(exc).__name__
            task["message"] = f"Task failed with unexpected error: {exc}"
            if int(task["attempt"]) < 2:
                task["status"] = TaskStatus.RETRYING.value
                await self.storage.update_task(task)
                await self.storage.enqueue_task(task["task_id"])
            else:
                task["status"] = TaskStatus.FAILED.value
                task["finished_at"] = utc_now()
                await self.storage.update_task(task)
        finally:
            await self.storage.release_slots(run["run_id"])
            await self.storage.refresh_run_terminal_state(run["run_id"])

    def _outcome_for_attempt(self, task: dict[str, Any]) -> str:
        if int(task["attempt"]) == 1:
            return task["planned_first_attempt_outcome"]
        return task["planned_retry_attempt_outcome"] or task["planned_first_attempt_outcome"]

    def _error_for_outcome(self, outcome: str) -> str:
        if outcome == PlannedOutcome.TIMEOUT.value:
            return ErrorCode.TASK_TIMEOUT.value
        if outcome == PlannedOutcome.FAILED.value:
            return ErrorCode.TASK_FAILED.value
        return ErrorCode.UNKNOWN_ERROR.value

    def _cleanup_finished_tasks(self) -> None:
        self.running = {task for task in self.running if not task.done()}


async def main() -> None:
    redis = get_redis()
    worker = Worker(RedisStorage(redis))
    try:
        await worker.run_forever()
    finally:
        await worker.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
