from __future__ import annotations

from enum import StrEnum


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"


class PlannedOutcome(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


class ErrorCode(StrEnum):
    TASK_FAILED = "task_failed"
    TASK_TIMEOUT = "task_timeout"
    INSUFFICIENT_MEMORY = "insufficient_memory"
    GLOBAL_CONCURRENCY_LIMIT = "global_concurrency_limit"
    ONE_OR_MORE_TASKS_FAILED = "one_or_more_tasks_failed"
    UNKNOWN_ERROR = "unknown_error"


TERMINAL_RUN_STATUSES = {RunStatus.COMPLETED, RunStatus.FAILED}
TERMINAL_TASK_STATUSES = {TaskStatus.COMPLETED, TaskStatus.FAILED}
