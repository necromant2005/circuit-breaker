from __future__ import annotations

import os
from uuid import UUID
from contextlib import asynccontextmanager

import psutil
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.schemas import (
    CreateRunRequest,
    CreateRunResponse,
    RunResponse,
    SummaryCounters,
    TasksResponse,
)
from app.storage import (
    DuplicateActiveSeedError,
    RedisStorage,
    get_redis,
)


GLOBAL_MAX_SUBTASK_PROCESSES = 1000
ACTIVE_RUN_CONCURRENCY_KEY = "active_runs:reserved_concurrency"
MAX_TASK_RECORD_SIZE_BYTES = 100_000
MIN_AVAILABLE_MEMORY_MB = 256
MAX_SUBPROCESS_TIMEOUT_SECONDS = 11
TASK_DURATION_SCALE = float(os.getenv("TASK_DURATION_SCALE", "1.0"))

# Task execution policy:
# task_index % len(TASK_EXECUTION_VARIATIONS) selects one of these plans in order.
# "completed" means the attempt succeeds. Failed or timeout first attempts get one retry.
TASK_EXECUTION_VARIATIONS = {
    "success_first_attempt": {"attempt_1": "completed", "attempt_2": None},
    "failed_then_success": {"attempt_1": "failed", "attempt_2": "completed"},
    "timeout_then_success": {"attempt_1": "timeout", "attempt_2": "completed"},
    "failed_then_failed": {"attempt_1": "failed", "attempt_2": "failed"},
    "failed_then_timeout": {"attempt_1": "failed", "attempt_2": "timeout"},
    "timeout_then_failed": {"attempt_1": "timeout", "attempt_2": "failed"},
    "timeout_then_timeout": {"attempt_1": "timeout", "attempt_2": "timeout"},
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = get_redis()
    storage = RedisStorage(redis)
    app.state.redis = redis
    app.state.storage = storage
    await reconcile_active_run_concurrency(redis, storage)
    try:
        yield
    finally:
        await redis.aclose()


app = FastAPI(title="Async Run Orchestrator", lifespan=lifespan)


def get_storage(request: Request) -> RedisStorage:
    return request.app.state.storage


def available_memory_mb() -> float:
    return psutil.virtual_memory().available / 1024 / 1024


def memory_is_available(threshold_mb: int = MIN_AVAILABLE_MEMORY_MB) -> bool:
    return available_memory_mb() >= threshold_mb


def validate_run_id(run_id: str) -> str:
    try:
        return str(UUID(run_id))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="run_id must be a valid UUID",
        ) from exc


def post_input_values_size(payload: CreateRunRequest) -> int:
    return sum(
        len(str(value).encode("utf-8"))
        for value in (
            payload.scenario,
            payload.count,
            payload.seed,
            payload.max_concurrency,
        )
    )


class ConcurrencyReservationError(Exception):
    def __init__(self, requested: int, reserved: int, limit: int) -> None:
        self.requested = requested
        self.reserved = reserved
        self.limit = limit
        super().__init__(
            f"Requested concurrency {requested} exceeds remaining capacity "
            f"{max(limit - reserved, 0)}"
        )


async def reserve_run_concurrency(redis: Redis, requested: int) -> None:
    async with redis.pipeline() as pipe:
        while True:
            try:
                await pipe.watch(ACTIVE_RUN_CONCURRENCY_KEY)
                reserved = int(await pipe.get(ACTIVE_RUN_CONCURRENCY_KEY) or 0)
                if reserved + requested > GLOBAL_MAX_SUBTASK_PROCESSES:
                    await pipe.unwatch()
                    raise ConcurrencyReservationError(
                        requested=requested,
                        reserved=reserved,
                        limit=GLOBAL_MAX_SUBTASK_PROCESSES,
                    )
                pipe.multi()
                pipe.incrby(ACTIVE_RUN_CONCURRENCY_KEY, requested)
                await pipe.execute()
                return
            except WatchError:
                continue


async def release_run_concurrency(redis: Redis, released: int) -> None:
    async with redis.pipeline() as pipe:
        while True:
            try:
                await pipe.watch(ACTIVE_RUN_CONCURRENCY_KEY)
                reserved = int(await pipe.get(ACTIVE_RUN_CONCURRENCY_KEY) or 0)
                pipe.multi()
                pipe.set(ACTIVE_RUN_CONCURRENCY_KEY, max(reserved - released, 0))
                await pipe.execute()
                return
            except WatchError:
                continue


async def reconcile_active_run_concurrency(redis: Redis, storage: RedisStorage) -> None:
    reserved = await storage.active_run_concurrency_total()
    await redis.set(ACTIVE_RUN_CONCURRENCY_KEY, reserved)


@app.post(
    "/runs",
    response_model=CreateRunResponse,
    responses={409: {"description": "Duplicate active seed"}},
)
async def create_run(payload: CreateRunRequest, request: Request) -> CreateRunResponse:
    input_size = post_input_values_size(payload)
    if input_size > MAX_TASK_RECORD_SIZE_BYTES:
        return JSONResponse(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            content={
                "error": "post_input_too_large",
                "message": (
                    f"POST /runs input values are {input_size} bytes, which exceeds "
                    f"the {MAX_TASK_RECORD_SIZE_BYTES} byte limit."
                ),
                "size": input_size,
                "limit": MAX_TASK_RECORD_SIZE_BYTES,
            },
        )

    if payload.max_concurrency > GLOBAL_MAX_SUBTASK_PROCESSES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "max_concurrency must be less than or equal to "
                f"{GLOBAL_MAX_SUBTASK_PROCESSES}"
            ),
        )

    if not memory_is_available(MIN_AVAILABLE_MEMORY_MB):
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "insufficient_memory",
                "message": (
                    "Run was not queued because available memory is below "
                    f"{MIN_AVAILABLE_MEMORY_MB} MB."
                ),
                "min_available_memory_mb": MIN_AVAILABLE_MEMORY_MB,
            },
        )

    storage = get_storage(request)
    redis: Redis = request.app.state.redis
    reserved = False
    try:
        await reserve_run_concurrency(redis, int(payload.max_concurrency))
        reserved = True
        run = await storage.create_run(
            payload,
            execution_variations=TASK_EXECUTION_VARIATIONS,
        )
    except DuplicateActiveSeedError as exc:
        if reserved:
            await release_run_concurrency(redis, int(payload.max_concurrency))
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "duplicate_active_seed",
                "message": f"A run with seed {payload.seed} is already queued or running.",
                "existing_run_id": exc.run_id,
                "existing_status": exc.status,
            },
        )
    except ConcurrencyReservationError as exc:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "global_concurrency_capacity_exceeded",
                "message": (
                    f"Cannot reserve {exc.requested} concurrency slots because "
                    f"{exc.reserved} of {exc.limit} are already reserved by active runs."
                ),
                "requested": exc.requested,
                "reserved": exc.reserved,
                "limit": exc.limit,
            },
        )
    except Exception:
        if reserved:
            await release_run_concurrency(redis, int(payload.max_concurrency))
        raise

    return CreateRunResponse(run_id=run["run_id"], status=run["status"])


@app.get("/runs/{run_id}", response_model=RunResponse)
async def get_run(run_id: str, request: Request) -> RunResponse:
    run_id = validate_run_id(run_id)
    storage = get_storage(request)
    run = await storage.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    summary = await storage.summarize_run(run_id)
    return RunResponse(
        run_id=run["run_id"],
        scenario=run["scenario"],
        count=run["count"],
        seed=run["seed"],
        max_concurrency=run["max_concurrency"],
        status=run["status"],
        error=run.get("error"),
        message=run.get("message"),
        summary=SummaryCounters(**summary),
    )


@app.get("/runs/{run_id}/tasks", response_model=TasksResponse)
async def get_run_tasks(run_id: str, request: Request) -> TasksResponse:
    run_id = validate_run_id(run_id)
    storage = get_storage(request)
    run = await storage.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return TasksResponse(tasks=await storage.get_run_tasks(run_id))


def create_app(redis: Redis | None = None) -> FastAPI:
    if redis is None:
        return app

    test_app = FastAPI(title="Async Run Orchestrator")
    test_app.state.redis = redis
    test_app.state.storage = RedisStorage(redis)
    test_app.include_router(app.router)
    return test_app
