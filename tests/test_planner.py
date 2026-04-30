from app.main import TASK_EXECUTION_VARIATIONS
from app.planner import build_task_plan, derive_child_seed, stable_int


EXPECTED_VARIATIONS = [
    ("success_first_attempt", "completed", None),
    ("failed_then_success", "failed", "completed"),
    ("timeout_then_success", "timeout", "completed"),
    ("failed_then_failed", "failed", "failed"),
    ("failed_then_timeout", "failed", "timeout"),
    ("timeout_then_failed", "timeout", "failed"),
    ("timeout_then_timeout", "timeout", "timeout"),
    ("success_first_attempt", "completed", None),
]


def test_child_seed_is_stable() -> None:
    assert derive_child_seed(42, 0) == derive_child_seed(42, 0)
    assert derive_child_seed(42, 0) != derive_child_seed(42, 1)


def test_stable_int_is_stable() -> None:
    assert stable_int(42, 0, "duration") == stable_int(42, 0, "duration")
    assert stable_int(42, 0, "duration") != stable_int(42, 1, "duration")


def test_same_seed_produces_same_execution_plan() -> None:
    first = build_task_plan(run_id="run-a", scenario="demo", run_seed=42, count=3)
    second = build_task_plan(run_id="run-a", scenario="demo", run_seed=42, count=3)
    assert first == second


def test_planner_assigns_execution_variations_by_task_index() -> None:
    tasks = build_task_plan(
        run_id="run-a",
        scenario="demo",
        run_seed=42,
        count=8,
        execution_variations=TASK_EXECUTION_VARIATIONS,
    )

    assert [
        (
            task["planned_execution_case"],
            task["planned_first_attempt_outcome"],
            task["planned_retry_attempt_outcome"],
        )
        for task in tasks
    ] == EXPECTED_VARIATIONS


def test_planner_assigns_stable_attempt_durations_by_outcome() -> None:
    tasks = build_task_plan(run_id="run-a", scenario="demo", run_seed=42, count=7)

    for task in tasks:
        if task["planned_first_attempt_outcome"] == "timeout":
            assert task["duration"] == 11
        else:
            assert 2 <= task["duration"] <= 10

        if task["planned_retry_attempt_outcome"] == "timeout":
            assert task["retry_duration"] == 11
        elif task["planned_retry_attempt_outcome"] is not None:
            assert 2 <= task["retry_duration"] <= 10
        else:
            assert task["retry_duration"] is None


def test_planner_does_not_use_random_module() -> None:
    import app.planner as planner

    assert "random" not in planner.__dict__
