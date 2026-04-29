from app.planner import build_task_plan, derive_child_seed


def test_child_seed_is_stable() -> None:
    assert derive_child_seed(42, 0) == derive_child_seed(42, 0)
    assert derive_child_seed(42, 0) != derive_child_seed(42, 1)


def test_same_seed_produces_same_execution_plan() -> None:
    first = build_task_plan(run_id="run-a", scenario="demo", run_seed=42, count=3)
    second = build_task_plan(run_id="run-a", scenario="demo", run_seed=42, count=3)
    assert first == second
