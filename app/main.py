from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from app.schemas import (
    CreateRunRequest,
    CreateRunResponse,
    RunResponse,
    SummaryCounters,
    TasksResponse,
)
from app.storage import DuplicateActiveSeedError, RedisStorage, get_redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = get_redis()
    app.state.redis = redis
    app.state.storage = RedisStorage(redis)
    try:
        yield
    finally:
        await redis.aclose()


app = FastAPI(title="Async Run Orchestrator", lifespan=lifespan)


def get_storage(request: Request) -> RedisStorage:
    return request.app.state.storage


@app.post(
    "/runs",
    response_model=CreateRunResponse,
    responses={409: {"description": "Duplicate active seed"}},
)
async def create_run(payload: CreateRunRequest, request: Request) -> CreateRunResponse:
    storage = get_storage(request)
    try:
        run = await storage.create_run(payload)
    except DuplicateActiveSeedError as exc:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "duplicate_active_seed",
                "message": f"A run with seed {payload.seed} is already queued or running.",
                "existing_run_id": exc.run_id,
                "existing_status": exc.status,
            },
        )

    return CreateRunResponse(run_id=run["run_id"], status=run["status"])


@app.get("/runs/{run_id}", response_model=RunResponse)
async def get_run(run_id: str, request: Request) -> RunResponse:
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
