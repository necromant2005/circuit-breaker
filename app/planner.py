from __future__ import annotations

import hashlib
import random
from typing import Any

from app.models import PlannedOutcome, TaskStatus


def derive_child_seed(run_seed: int, task_index: int) -> int:
    digest = hashlib.sha256(f"{run_seed}:{task_index}".encode()).hexdigest()
    return int(digest[:8], 16)


def _planned_outcome(rng: random.Random) -> PlannedOutcome:
    value = rng.random()
    if value < 0.7:
        return PlannedOutcome.COMPLETED
    if value < 0.85:
        return PlannedOutcome.FAILED
    return PlannedOutcome.TIMEOUT


def build_task_plan(
    *,
    run_id: str,
    scenario: str,
    run_seed: int,
    count: int,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []

    for index in range(count):
        child_seed = derive_child_seed(run_seed, index)
        rng = random.Random(child_seed)
        duration = rng.randint(2, 10)
        first_outcome = _planned_outcome(rng)
        retry_outcome = None
        if first_outcome != PlannedOutcome.COMPLETED:
            retry_outcome = _planned_outcome(rng)

        eventual_success = (
            first_outcome == PlannedOutcome.COMPLETED
            or retry_outcome == PlannedOutcome.COMPLETED
        )
        result = None
        if eventual_success:
            result = {
                "scenario": scenario,
                "task_index": index,
                "child_seed": child_seed,
                "value": rng.randint(1000, 9999),
            }

        task_id = f"{run_id}:{index}"
        tasks.append(
            {
                "task_id": task_id,
                "run_id": run_id,
                "task_index": index,
                "child_seed": child_seed,
                "status": TaskStatus.PENDING.value,
                "attempt": 0,
                "duration": float(duration),
                "planned_first_attempt_outcome": first_outcome.value,
                "planned_retry_attempt_outcome": retry_outcome.value if retry_outcome else None,
                "planned_result": result,
                "result": None,
                "error": None,
                "reason": None,
                "blocked_reason": None,
                "message": "Task is waiting to be scheduled.",
                "created_at": None,
                "started_at": None,
                "updated_at": None,
                "finished_at": None,
            }
        )

    return tasks
