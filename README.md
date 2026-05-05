# Async Run Orchestrator

Small Ruby service for deterministic asynchronous run/task orchestration.

## Features

- Create a run and split it into deterministic simulated tasks.
- Persist run state, task state, queues, active seed mapping, and counters in Redis.
- Execute work in a separate Ruby worker process.
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

Starting only the API service also starts the worker dependency:

```bash
docker compose up api
```

Useful environment variables:

```bash
REDIS_URL=redis://localhost:6379/0
TASK_DURATION_SCALE=1.0
PORT=8000
```

## API

Create a run:

```bash
curl -X POST http://localhost:8000/runs \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"demo","count":5,"seed":42,"max_concurrency":3}'
```

Service limits are defined in `lib/circuit_breaker/main.rb`:

```ruby
GLOBAL_MAX_SUBTASK_PROCESSES = 1000
MAX_TASK_RECORD_SIZE_BYTES = 100_000
MIN_AVAILABLE_MEMORY_MB = 256
MAX_SUBPROCESS_TIMEOUT_SECONDS = 11
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

```ruby
Digest::SHA256.hexdigest("#{run_seed}:#{task_index}")[0, 8].to_i(16)
```

The planner persists the duration, first outcome, retry outcome, execution case
name, and result payload before tasks are queued. Workers only execute that
persisted plan.

## Tests

```bash
docker compose --profile test run --rm tests
```
