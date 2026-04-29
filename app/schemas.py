from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models import RunStatus, TaskStatus


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario: str = Field(..., min_length=1, strict=True)
    count: int = Field(..., ge=1, strict=True)
    seed: int = Field(..., strict=True)
    max_concurrency: int = Field(..., ge=1, strict=True)

    @field_validator("scenario")
    @classmethod
    def scenario_must_not_be_blank(cls, value: str) -> str:
        scenario = value.strip()
        if not scenario:
            raise ValueError("scenario must not be blank")
        return scenario


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
    retry_duration: float | None = None
    planned_execution_case: str | None = None
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
