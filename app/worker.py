from __future__ import annotations

import asyncio
from typing import Any

from app.main import (
    GLOBAL_MAX_SUBTASK_PROCESSES,
    MAX_SUBPROCESS_TIMEOUT_SECONDS,
    MIN_AVAILABLE_MEMORY_MB,
    RETRY_DELAY_SECONDS,
    TASK_DURATION_SCALE,
    memory_is_available,
    release_run_concurrency,
)
from app.models import ErrorCode, PlannedOutcome, TaskStatus
from app.storage import RedisStorage, get_redis, utc_now


class Worker:
    def __init__(self, storage: RedisStorage, *, poll_timeout: int = 1) -> None:
        self.storage = storage
        self.poll_timeout = poll_timeout
        self.running: set[asyncio.Task[None]] = set()
        self._stopping = False

    async def run_forever(self) -> None:
        await self.storage.repair_running_state()
        await self._release_finished_run_reservations()
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
            TaskStatus.RUNNING.value,
            TaskStatus.COMPLETED.value,
            TaskStatus.FAILED.value,
        }:
            return False

        run = await self.storage.get_run(task["run_id"])
        if not run:
            return False

        if not await self._can_start_task(run, task):
            refreshed_task = await self.storage.get_task(task_id)
            if refreshed_task and refreshed_task["status"] not in {
                TaskStatus.COMPLETED.value,
                TaskStatus.FAILED.value,
            }:
                await self.storage.enqueue_task(task_id)
            await asyncio.sleep(0.2)
            return False

        task_runner = asyncio.create_task(self._execute_task(run, task))
        self.running.add(task_runner)
        task_runner.add_done_callback(self.running.discard)
        return True

    async def _can_start_task(self, run: dict[str, Any], task: dict[str, Any]) -> bool:
        running_summary = await self.storage.summarize_running_tasks(run["run_id"])
        if running_summary["global_running"] >= running_summary["global_limit"]:
            task["blocked_reason"] = ErrorCode.GLOBAL_CONCURRENCY_LIMIT.value
            task["message"] = (
                "Task is waiting because the global subtask process limit of "
                f"{running_summary['global_limit']} running processes has been reached."
            )
            await self.storage.update_task(task)
            return False

        if running_summary["run_running"] >= int(run["max_concurrency"]):
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

        if not memory_is_available(MIN_AVAILABLE_MEMORY_MB):
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
            await self._run_subprocess_simulation(task)
            outcome = self._outcome_for_attempt(task)
            if outcome == PlannedOutcome.COMPLETED.value:
                task["status"] = TaskStatus.COMPLETED.value
                task["result"] = task["planned_result"]
                task["error"] = None
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
                await self._delay_before_retry()
                await self.storage.enqueue_task(task["task_id"])
                return

            task["status"] = TaskStatus.FAILED.value
            task["message"] = (
                f"Task failed after {task['attempt']} attempts. Last reason: {outcome}."
            )
            task["finished_at"] = utc_now()
            await self.storage.update_task(task)
        except TimeoutError:
            task["error"] = ErrorCode.TASK_TIMEOUT.value
            task["reason"] = PlannedOutcome.TIMEOUT.value
            if int(task["attempt"]) < 2:
                task["status"] = TaskStatus.RETRYING.value
                task["message"] = (
                    f"Task attempt {task['attempt']} reached the "
                    f"{MAX_SUBPROCESS_TIMEOUT_SECONDS} second kill timeout; retrying once."
                )
                await self.storage.update_task(task)
                await self._delay_before_retry()
                await self.storage.enqueue_task(task["task_id"])
            else:
                task["status"] = TaskStatus.FAILED.value
                task["message"] = (
                    "Task failed because the subprocess reached the "
                    f"{MAX_SUBPROCESS_TIMEOUT_SECONDS} second kill timeout twice."
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
                await self._delay_before_retry()
                await self.storage.enqueue_task(task["task_id"])
            else:
                task["status"] = TaskStatus.FAILED.value
                task["finished_at"] = utc_now()
                await self.storage.update_task(task)
        finally:
            await self.storage.release_slots(run["run_id"])
            await self.storage.refresh_run_terminal_state(run["run_id"])
            await self._release_run_reservation_if_terminal(run["run_id"])

    async def _run_subprocess_simulation(
        self,
        task: dict[str, Any],
    ) -> None:
        outcome = None
        if task.get("planned_first_attempt_outcome"):
            outcome = self._outcome_for_attempt(task)
        duration = float(task["duration"])
        if int(task.get("attempt", 1)) > 1 and task.get("retry_duration") is not None:
            duration = float(task["retry_duration"])

        if outcome == PlannedOutcome.TIMEOUT.value:
            runtime = max(duration, MAX_SUBPROCESS_TIMEOUT_SECONDS) * TASK_DURATION_SCALE
            await asyncio.sleep(runtime)
            raise TimeoutError(
                f"subprocess reached {MAX_SUBPROCESS_TIMEOUT_SECONDS} second kill timeout"
            )

        runtime = duration * TASK_DURATION_SCALE
        if runtime >= MAX_SUBPROCESS_TIMEOUT_SECONDS:
            await asyncio.sleep(MAX_SUBPROCESS_TIMEOUT_SECONDS)
            raise TimeoutError(
                f"subprocess reached {MAX_SUBPROCESS_TIMEOUT_SECONDS} second kill timeout"
            )
        await asyncio.sleep(runtime)

    async def _delay_before_retry(self) -> None:
        delay = RETRY_DELAY_SECONDS * TASK_DURATION_SCALE
        if delay > 0:
            await asyncio.sleep(delay)

    def _outcome_for_attempt(self, task: dict[str, Any]) -> str:
        if int(task["attempt"]) == 1:
            return task["planned_first_attempt_outcome"]
        return task["planned_retry_attempt_outcome"] or task["planned_first_attempt_outcome"]

    def _error_for_outcome(self, outcome: str) -> str:
        if outcome == PlannedOutcome.FAILED.value:
            return ErrorCode.TASK_FAILED.value
        if outcome == PlannedOutcome.TIMEOUT.value:
            return ErrorCode.TASK_TIMEOUT.value
        return ErrorCode.UNKNOWN_ERROR.value

    async def _release_run_reservation_if_terminal(self, run_id: str) -> None:
        release_amount = await self.storage.mark_run_concurrency_released(run_id)
        if release_amount:
            await release_run_concurrency(self.storage.redis, release_amount)

    async def _release_finished_run_reservations(self) -> None:
        cursor = 0
        while True:
            cursor, keys = await self.storage.redis.scan(cursor=cursor, match="run:*")
            for key in keys:
                if key.endswith(":tasks") or key.endswith(":running_tasks"):
                    continue
                run_id = key.removeprefix("run:")
                await self._release_run_reservation_if_terminal(run_id)
            if cursor == 0:
                break

    def _cleanup_finished_tasks(self) -> None:
        self.running = {task for task in self.running if not task.done()}


async def main() -> None:
    redis = get_redis()
    worker = Worker(
        RedisStorage(
            redis,
            global_concurrency_limit=GLOBAL_MAX_SUBTASK_PROCESSES,
        )
    )
    try:
        await worker.run_forever()
    finally:
        await worker.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
