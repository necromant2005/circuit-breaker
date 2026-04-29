from __future__ import annotations

import hashlib
import random
from collections.abc import Mapping, Sequence
from typing import Any

from app.models import PlannedOutcome, TaskStatus


ExecutionVariation = Mapping[str, str | None] | tuple[str, str | None]
ExecutionVariationMap = Mapping[str, Mapping[str, str | None]]


DEFAULT_EXECUTION_VARIATIONS: ExecutionVariationMap = {
    "success_first_attempt": {"attempt_1": PlannedOutcome.COMPLETED.value, "attempt_2": None},
    "failed_then_success": {
        "attempt_1": PlannedOutcome.FAILED.value,
        "attempt_2": PlannedOutcome.COMPLETED.value,
    },
    "timeout_then_success": {
        "attempt_1": PlannedOutcome.TIMEOUT.value,
        "attempt_2": PlannedOutcome.COMPLETED.value,
    },
    "failed_then_failed": {
        "attempt_1": PlannedOutcome.FAILED.value,
        "attempt_2": PlannedOutcome.FAILED.value,
    },
    "failed_then_timeout": {
        "attempt_1": PlannedOutcome.FAILED.value,
        "attempt_2": PlannedOutcome.TIMEOUT.value,
    },
    "timeout_then_failed": {
        "attempt_1": PlannedOutcome.TIMEOUT.value,
        "attempt_2": PlannedOutcome.FAILED.value,
    },
    "timeout_then_timeout": {
        "attempt_1": PlannedOutcome.TIMEOUT.value,
        "attempt_2": PlannedOutcome.TIMEOUT.value,
    },
}


def _normalize_execution_variations(
    variations: ExecutionVariationMap | Sequence[ExecutionVariation] | None,
) -> tuple[tuple[str, PlannedOutcome, PlannedOutcome | None], ...]:
    normalized = []
    configured = variations or DEFAULT_EXECUTION_VARIATIONS
    items: Sequence[tuple[str | None, ExecutionVariation]]
    if isinstance(configured, Mapping):
        items = tuple((name, plan) for name, plan in configured.items())
    else:
        items = tuple((None, plan) for plan in configured)

    for index, (configured_name, variation) in enumerate(items):
        if isinstance(variation, Mapping):
            name = str(configured_name or variation.get("name") or f"case_{index + 1}")
            first = variation["attempt_1"]
            retry = variation.get("attempt_2")
        else:
            first, retry = variation
            name = f"case_{index + 1}_{first}_then_{retry or 'none'}"
        normalized.append(
            (
                name,
                PlannedOutcome(first),
                PlannedOutcome(retry) if retry else None,
            )
        )
    return tuple(normalized)


def derive_child_seed(run_seed: int, task_index: int) -> int:
    digest = hashlib.sha256(f"{run_seed}:{task_index}".encode()).hexdigest()
    return int(digest[:8], 16)


def planned_outcomes_for_task(
    task_index: int,
    *,
    execution_variations: ExecutionVariationMap | Sequence[ExecutionVariation] | None = None,
) -> tuple[str, PlannedOutcome, PlannedOutcome | None]:
    variations = _normalize_execution_variations(execution_variations)
    return variations[task_index % len(variations)]


def build_task_plan(
    *,
    run_id: str,
    scenario: str,
    run_seed: int,
    count: int,
    execution_variations: ExecutionVariationMap | Sequence[ExecutionVariation] | None = None,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []

    for index in range(count):
        child_seed = derive_child_seed(run_seed, index)
        rng = random.Random(child_seed)
        duration = rng.randint(2, 10)
        case_name, first_outcome, retry_outcome = planned_outcomes_for_task(
            index,
            execution_variations=execution_variations,
        )

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
                "retry_duration": float(duration),
                "planned_execution_case": case_name,
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
