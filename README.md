# Async Run Orchestrator

Small FastAPI service for deterministic asynchronous run/task orchestration.

## Features

- Create a run and split it into deterministic simulated tasks.
- Persist run state, task state, queues, active seed mapping, and counters in Redis.
- Execute work in a separate asyncio worker process.
- Enforce per-run `max_concurrency` and a global limit of 10 running tasks.
- Retry failed or timed-out tasks once.
- Expose run summaries, task plans, results, and failure explanations through API.

## Setup

```bash
docker compose build
```

## Run

Start Redis, the API, and the worker:

```bash
docker compose up
```

Useful environment variables:

```bash
REDIS_URL=redis://localhost:6379/0
MIN_AVAILABLE_MEMORY_MB=256
TASK_DURATION_SCALE=1.0
```

`TASK_DURATION_SCALE` is useful for tests or demos. For example,
`TASK_DURATION_SCALE=0.1` makes a planned 10 second task run for 1 second.

## API

Create a run:

```bash
curl -X POST http://localhost:8000/runs \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"demo","count":5,"seed":42,"max_concurrency":3}'
```

Get run state:

```bash
curl http://localhost:8000/runs/{run_id}
```

Get task state:

```bash
curl http://localhost:8000/runs/{run_id}/tasks
```

## Determinism

Each task gets a stable child seed:

```python
int(sha256(f"{run_seed}:{task_index}".encode()).hexdigest()[:8], 16)
```

The planner persists the duration, first outcome, retry outcome, and result payload
before tasks are queued. Workers only execute that persisted plan.

## Tests

```bash
docker compose --profile test run --rm tests
```
