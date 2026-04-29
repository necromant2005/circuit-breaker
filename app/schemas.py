from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.models import RunStatus, TaskStatus


class CreateRunRequest(BaseModel):
    scenario: str = Field(..., min_length=1)
    count: int = Field(..., ge=1)
    seed: int
    max_concurrency: int = Field(..., ge=1, le=10)


class CreateRunResponse(BaseModel):
    run_id: str
    status: RunStatus


class DuplicateActiveSeedResponse(BaseModel):
    error: str
    message: str
    existing_run_id: str
    existing_status: RunStatus


class SummaryCounters(BaseModel):
    pending: int = 0
    running: int = 0
    retrying: int = 0
    completed: int = 0
    failed: int = 0


class RunResponse(BaseModel):
    run_id: str
    scenario: str
    count: int
    seed: int
    max_concurrency: int
    status: RunStatus
    error: str | None = None
    message: str | None = None
    summary: SummaryCounters


class TaskResponse(BaseModel):
    task_id: str
    run_id: str
    task_index: int
    child_seed: int
    status: TaskStatus
    attempt: int
    duration: float
    planned_first_attempt_outcome: str
    planned_retry_attempt_outcome: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    reason: str | None = None
    blocked_reason: str | None = None
    message: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None


class TasksResponse(BaseModel):
    tasks: list[TaskResponse]
