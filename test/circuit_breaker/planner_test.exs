defmodule CircuitBreaker.PlannerTest do
  use ExUnit.Case, async: true

  alias CircuitBreaker.{Main, Planner}

  @expected_variations [
    {"success_first_attempt", "completed", nil},
    {"failed_then_success", "failed", "completed"},
    {"timeout_then_success", "timeout", "completed"},
    {"failed_then_failed", "failed", "failed"},
    {"failed_then_timeout", "failed", "timeout"},
    {"timeout_then_failed", "timeout", "failed"},
    {"timeout_then_timeout", "timeout", "timeout"},
    {"success_first_attempt", "completed", nil}
  ]

  test "child seed is stable" do
    assert Planner.derive_child_seed(42, 0) == Planner.derive_child_seed(42, 0)
    assert Planner.derive_child_seed(42, 0) != Planner.derive_child_seed(42, 1)
  end

  test "same seed produces same execution plan" do
    first = Planner.build_task_plan(run_id: "run-a", scenario: "demo", run_seed: 42, count: 3)
    second = Planner.build_task_plan(run_id: "run-a", scenario: "demo", run_seed: 42, count: 3)
    assert first == second
  end

  test "planner assigns named execution variations by task index" do
    tasks =
      Planner.build_task_plan(
        run_id: "run-a",
        scenario: "demo",
        run_seed: 42,
        count: 8,
        execution_variations: Main.task_execution_variations()
      )

    assert Enum.map(tasks, fn task ->
             {
               task["planned_execution_case"],
               task["planned_first_attempt_outcome"],
               task["planned_retry_attempt_outcome"]
             }
           end) == @expected_variations
  end

  test "timeout outcomes use kill timeout duration" do
    tasks = Planner.build_task_plan(run_id: "run-a", scenario: "demo", run_seed: 42, count: 7)

    for task <- tasks do
      if task["planned_first_attempt_outcome"] == "timeout" do
        assert task["duration"] == 11.0
      else
        assert task["duration"] >= 2.0
        assert task["duration"] <= 10.0
      end
    end
  end
end
