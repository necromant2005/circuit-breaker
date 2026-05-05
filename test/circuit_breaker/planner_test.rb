# frozen_string_literal: true

require_relative "../test_helper"

class PlannerTest < Minitest::Test
  include TestHelpers

  EXPECTED_VARIATIONS = [
    ["success_first_attempt", "completed", nil],
    ["failed_then_success", "failed", "completed"],
    ["timeout_then_success", "timeout", "completed"],
    ["failed_then_failed", "failed", "failed"],
    ["failed_then_timeout", "failed", "timeout"],
    ["timeout_then_failed", "timeout", "failed"],
    ["timeout_then_timeout", "timeout", "timeout"],
    ["success_first_attempt", "completed", nil]
  ].freeze

  def test_child_seed_is_stable
    assert_equal CircuitBreaker::Planner.derive_child_seed(42, 0), CircuitBreaker::Planner.derive_child_seed(42, 0)
    refute_equal CircuitBreaker::Planner.derive_child_seed(42, 0), CircuitBreaker::Planner.derive_child_seed(42, 1)
  end

  def test_same_seed_produces_same_execution_plan
    first = CircuitBreaker::Planner.build_task_plan(run_id: "run-a", scenario: "demo", run_seed: 42, count: 3)
    second = CircuitBreaker::Planner.build_task_plan(run_id: "run-a", scenario: "demo", run_seed: 42, count: 3)
    assert_equal first, second
  end

  def test_planner_assigns_named_execution_variations_by_task_index
    tasks = CircuitBreaker::Planner.build_task_plan(
      run_id: "run-a",
      scenario: "demo",
      run_seed: 42,
      count: 8,
      execution_variations: CircuitBreaker::Main.task_execution_variations
    )

    assert_equal EXPECTED_VARIATIONS, tasks.map { |task|
      [
        task["planned_execution_case"],
        task["planned_first_attempt_outcome"],
        task["planned_retry_attempt_outcome"]
      ]
    }
  end

  def test_timeout_outcomes_use_kill_timeout_duration
    tasks = CircuitBreaker::Planner.build_task_plan(run_id: "run-a", scenario: "demo", run_seed: 42, count: 7)

    tasks.each do |task|
      if task["planned_first_attempt_outcome"] == "timeout"
        assert_equal 11.0, task["duration"]
      else
        assert_operator task["duration"], :>=, 2.0
        assert_operator task["duration"], :<=, 10.0
      end
    end
  end
end
