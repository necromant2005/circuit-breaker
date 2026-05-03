# Async Run Orchestrator

Small Elixir service for deterministic asynchronous run/task orchestration.

## Features

- Create a run and split it into deterministic simulated tasks.
- Persist run state, task state, queues, active seed mapping, and counters in Redis.
- Execute work in a separate Elixir worker process.
- Enforce per-run `max_concurrency` and a global reserved concurrency limit.
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
TASK_DURATION_SCALE=1.0
WORKER_ENABLED=false
PORT=8000
```

## API

Create a run:

```bash
curl -X POST http://localhost:8000/runs \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"demo","count":5,"seed":42,"max_concurrency":3}'
```

Service limits are defined in `lib/circuit_breaker/main.ex`:

```elixir
@global_max_subtask_processes 1000
@max_task_record_size_bytes 100_000
@min_available_memory_mb 256
@max_subprocess_timeout_seconds 11
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

```elixir
:crypto.hash(:sha256, "#{run_seed}:#{task_index}")
|> Base.encode16(case: :lower)
|> binary_part(0, 8)
|> String.to_integer(16)
```

The planner persists the duration, first outcome, retry outcome, execution case
name, and result payload before tasks are queued. Workers only execute that
persisted plan.

## Tests

```bash
docker compose --profile test run --rm tests
```
