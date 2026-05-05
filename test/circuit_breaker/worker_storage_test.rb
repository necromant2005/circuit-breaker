# frozen_string_literal: true

require_relative "../test_helper"

class WorkerStorageTest < Minitest::Test
  include TestHelpers

  OUTCOME_MAP = {
    "success" => "completed",
    "failed" => "failed",
    "timeout" => "timeout"
  }.freeze

  def test_concurrency_slots_respect_per_run_and_global_limits
    _, run_a = create_run(seed: 1, count: 12, max_concurrency: 7)
    _, run_b = create_run(seed: 2, count: 12, max_concurrency: 7)

    assert_equal [true] * 7, 7.times.map { CircuitBreaker::Storage.acquire_slots(run_a["run_id"], 7, 10) }
    refute CircuitBreaker::Storage.acquire_slots(run_a["run_id"], 7, 10)

    assert_equal [true] * 3, 3.times.map { CircuitBreaker::Storage.acquire_slots(run_b["run_id"], 7, 10) }
    refute CircuitBreaker::Storage.acquire_slots(run_b["run_id"], 7, 10)
    assert_equal 10, CircuitBreaker::Storage.global_running_count
  end

  def test_summarize_running_tasks_counts_global_and_run_running
    _, run_a = create_run(seed: 103, count: 2, max_concurrency: 2)
    _, run_b = create_run(seed: 104, count: 1, max_concurrency: 1)

    CircuitBreaker::Storage.get_run_tasks(run_a["run_id"]).each do |task|
      CircuitBreaker::Storage.update_task(task.merge("status" => "running"))
    end
    task_b = CircuitBreaker::Storage.get_run_tasks(run_b["run_id"]).first
    CircuitBreaker::Storage.update_task(task_b.merge("status" => "running"))

    summary = CircuitBreaker::Storage.summarize_running_tasks(run_a["run_id"])
    assert_equal 3, summary["global_running"]
    assert_equal 2, summary["run_running"]
    assert_equal 1000, summary["global_limit"]
  end

  def test_global_subtask_process_limit_returns_early_from_summary
    CircuitBreaker::Main.global_max_subtask_processes = 1
    _, run = create_run(seed: 105, count: 2, max_concurrency: 2)
    running_task, pending_task = CircuitBreaker::Storage.get_run_tasks(run["run_id"])
    CircuitBreaker::Storage.update_task(running_task.merge("status" => "running"))

    refute CircuitBreaker::Worker.process_once(0)
    refute CircuitBreaker::Worker.process_once(0)

    pending_task = CircuitBreaker::Storage.get_task(pending_task["task_id"])
    run = CircuitBreaker::Storage.get_run(run["run_id"])
    assert_equal "pending", pending_task["status"]
    assert_equal "global_concurrency_limit", pending_task["blocked_reason"]
    assert_includes pending_task["message"], "1 running processes"
    assert_equal "queued", run["status"]
  end

  def test_run_concurrency_limit_returns_early_from_summary
    _, run = create_run(seed: 106, count: 2, max_concurrency: 1)
    running_task, pending_task = CircuitBreaker::Storage.get_run_tasks(run["run_id"])
    CircuitBreaker::Storage.update_task(running_task.merge("status" => "running"))

    refute CircuitBreaker::Worker.process_once(0)
    refute CircuitBreaker::Worker.process_once(0)

    pending_task = CircuitBreaker::Storage.get_task(pending_task["task_id"])
    assert_equal "pending", pending_task["status"]
    assert_equal "run_concurrency_limit", pending_task["blocked_reason"]
  end

  def test_insufficient_memory_blocks_task_and_releases_acquired_slot
    CircuitBreaker::Main.min_available_memory_mb = 1_000_000_000
    _, run = create_run(seed: 5, count: 1, max_concurrency: 1)

    refute CircuitBreaker::Worker.process_once(0)

    task = CircuitBreaker::Storage.get_run_tasks(run["run_id"]).first
    assert_equal "pending", task["status"]
    assert_equal "insufficient_memory", task["blocked_reason"]
    assert_includes task["message"], "available memory"
    assert_equal 0, CircuitBreaker::Storage.global_running_count
    assert_equal 0, CircuitBreaker::Storage.run_running_count(run["run_id"])
  end

  def test_finished_run_releases_reserved_concurrency
    response = post_json("/runs", "scenario" => "demo", "count" => 1, "seed" => 46, "max_concurrency" => 1000)
    run_id = json(response)["run_id"]
    assert_equal "1000", CircuitBreaker::Storage.redis.get(CircuitBreaker::Main.active_run_concurrency_key)

    task = CircuitBreaker::Storage.get_run_tasks(run_id).first
    CircuitBreaker::Storage.update_task(task.merge("duration" => 0.0, "planned_first_attempt_outcome" => "completed", "planned_result" => { "ok" => true }))

    assert CircuitBreaker::Worker.process_once(0)

    run = CircuitBreaker::Storage.get_run(run_id)
    assert_equal "completed", run["status"]
    assert_equal true, run["concurrency_released"]
    assert_equal "0", CircuitBreaker::Storage.redis.get(CircuitBreaker::Main.active_run_concurrency_key)
  end

  def test_all_named_task_execution_variations_reach_expected_terminal_status
    cases = [
      ["success", nil, "completed", 1, nil],
      ["failed", "success", "completed", 2, nil],
      ["timeout", "success", "completed", 2, nil],
      ["failed", "failed", "failed", 2, "task_failed"],
      ["failed", "timeout", "failed", 2, "task_timeout"],
      ["timeout", "failed", "failed", 2, "task_failed"],
      ["timeout", "timeout", "failed", 2, "task_timeout"]
    ]

    cases.each do |attempt_1, attempt_2, expected_status, expected_attempts, expected_error|
      CircuitBreaker::Storage.redis.flushdb
      _run, initial_task = create_task_with_forced_plan(seed: 2000 + expected_attempts, attempt_1: attempt_1, attempt_2: attempt_2)

      task, transitions = run_forced_task_to_terminal(initial_task["task_id"])

      assert_equal expected_status, task["status"]
      assert_equal expected_attempts, task["attempt"]
      expected_error.nil? ? assert_nil(task["error"]) : assert_equal(expected_error, task["error"])
      assert_equal OUTCOME_MAP[attempt_1], task["planned_first_attempt_outcome"]
      expected_retry_outcome = attempt_2 && OUTCOME_MAP[attempt_2]
      expected_retry_outcome.nil? ? assert_nil(task["planned_retry_attempt_outcome"]) : assert_equal(expected_retry_outcome, task["planned_retry_attempt_outcome"])

      if expected_status == "completed"
        assert_equal initial_task["planned_result"], task["result"]
        assert_nil task["error"]
      else
        assert_nil task["result"]
        refute_nil task["finished_at"]
        assert_includes task["message"].downcase, "failed"
      end

      if expected_attempts == 1
        assert_equal ["completed"], transitions
        assert_nil task["reason"]
        assert_equal 0, CircuitBreaker::Storage.redis.llen(CircuitBreaker::Storage.task_queue_key)
      else
        assert_equal "retrying", transitions.first
        assert_equal expected_status, transitions.last
        expected_reason = expected_status == "completed" ? OUTCOME_MAP[attempt_1] : OUTCOME_MAP[attempt_2]
        assert_equal expected_reason, task["reason"]
      end
    end
  end

  def test_successful_retry_keeps_first_failure_reason
    %w[failed timeout].each do |attempt_1|
      CircuitBreaker::Storage.redis.flushdb
      _run, initial_task = create_task_with_forced_plan(seed: 3000 + attempt_1.bytesize, attempt_1: attempt_1, attempt_2: "success")

      assert CircuitBreaker::Worker.process_once(0)
      retrying_task = CircuitBreaker::Storage.get_task(initial_task["task_id"])
      assert_equal "retrying", retrying_task["status"]
      assert_equal OUTCOME_MAP[attempt_1], retrying_task["reason"]
      assert_includes %w[task_failed task_timeout], retrying_task["error"]

      assert CircuitBreaker::Worker.process_once(0)
      completed_task = CircuitBreaker::Storage.get_task(initial_task["task_id"])
      assert_equal "completed", completed_task["status"]
      assert_equal 2, completed_task["attempt"]
      assert_equal initial_task["planned_result"], completed_task["result"]
      assert_nil completed_task["error"]
      assert_equal OUTCOME_MAP[attempt_1], completed_task["reason"]
    end
  end

  def test_retry_is_prioritized_before_newer_pending_tasks
    _, run = create_run(seed: 700, count: 2, max_concurrency: 1)
    first, second = CircuitBreaker::Storage.get_run_tasks(run["run_id"])

    CircuitBreaker::Storage.update_task(first.merge(
      "duration" => 0.0,
      "retry_duration" => 0.0,
      "planned_first_attempt_outcome" => "failed",
      "planned_retry_attempt_outcome" => "completed",
      "planned_result" => { "task" => "first" }
    ))
    CircuitBreaker::Storage.update_task(second.merge(
      "duration" => 0.0,
      "planned_first_attempt_outcome" => "completed",
      "planned_result" => { "task" => "second" }
    ))

    assert CircuitBreaker::Worker.process_once(0)
    first = CircuitBreaker::Storage.get_task(first["task_id"])
    second = CircuitBreaker::Storage.get_task(second["task_id"])
    assert_equal "retrying", first["status"]
    assert_equal "pending", second["status"]

    assert CircuitBreaker::Worker.process_once(0)
    first = CircuitBreaker::Storage.get_task(first["task_id"])
    second = CircuitBreaker::Storage.get_task(second["task_id"])
    assert_equal "completed", first["status"]
    assert_equal 2, first["attempt"]
    assert_equal "pending", second["status"]
  end

  def test_configured_retry_count_allows_more_attempts
    CircuitBreaker::Main.max_task_retries = 2
    _, run = create_run(seed: 109, count: 1, max_concurrency: 1)
    task = CircuitBreaker::Storage.get_run_tasks(run["run_id"]).first

    CircuitBreaker::Storage.update_task(task.merge(
      "duration" => 2.0,
      "retry_duration" => 2.0,
      "planned_first_attempt_outcome" => "failed",
      "planned_retry_attempt_outcome" => "failed"
    ))

    [1, 2].each do |expected_attempt|
      assert CircuitBreaker::Worker.process_once(0)
      task = CircuitBreaker::Storage.get_task(task["task_id"])
      assert_equal "retrying", task["status"]
      assert_equal expected_attempt, task["attempt"]
    end

    assert CircuitBreaker::Worker.process_once(0)
    task = CircuitBreaker::Storage.get_task(task["task_id"])
    assert_equal "failed", task["status"]
    assert_equal 3, task["attempt"]
    assert_equal "task_failed", task["error"]
  end

  def test_planned_timeout_outcome_is_not_caused_by_short_duration
    _, run = create_run(seed: 4, count: 3, max_concurrency: 1)
    task = CircuitBreaker::Storage.get_run_tasks(run["run_id"])[2]
    assert_equal "timeout", task["planned_first_attempt_outcome"]
    assert_equal 11.0, task["duration"]
  end

  def test_subprocess_timeout_can_fail_then_succeed_on_retry
    _, run = create_run(seed: 9, count: 1, max_concurrency: 1)
    task = CircuitBreaker::Storage.get_run_tasks(run["run_id"]).first

    CircuitBreaker::Storage.update_task(task.merge(
      "duration" => 11.0,
      "retry_duration" => 1.0,
      "planned_first_attempt_outcome" => "completed",
      "planned_retry_attempt_outcome" => "completed",
      "planned_result" => { "ok" => true }
    ))

    CircuitBreaker::Main.max_subprocess_timeout_seconds = 0
    assert CircuitBreaker::Worker.process_once(0)
    retrying_task = CircuitBreaker::Storage.get_task(task["task_id"])
    assert_equal "retrying", retrying_task["status"]
    assert_equal "task_timeout", retrying_task["error"]

    CircuitBreaker::Main.max_subprocess_timeout_seconds = 11
    assert CircuitBreaker::Worker.process_once(0)
    completed_task = CircuitBreaker::Storage.get_task(task["task_id"])
    assert_equal "completed", completed_task["status"]
    assert_equal 2, completed_task["attempt"]
    assert_equal({ "ok" => true }, completed_task["result"])
  end

  def test_repair_running_state_retries_first_attempt_and_fails_final_attempt
    _, run = create_run(seed: 500, count: 2, max_concurrency: 1)
    first, second = CircuitBreaker::Storage.get_run_tasks(run["run_id"])

    CircuitBreaker::Storage.update_task(first.merge("status" => "running", "attempt" => 1))
    CircuitBreaker::Storage.update_task(second.merge("status" => "running", "attempt" => 2))

    CircuitBreaker::Storage.repair_running_state

    first = CircuitBreaker::Storage.get_task(first["task_id"])
    second = CircuitBreaker::Storage.get_task(second["task_id"])

    assert_equal "retrying", first["status"]
    assert_equal "worker_restart", first["reason"]
    assert_equal "failed", second["status"]
    assert_equal "worker_restart", second["reason"]
    assert_equal "0", CircuitBreaker::Storage.redis.get(CircuitBreaker::Storage.global_running_key)
  end

  private

  def create_task_with_forced_plan(seed:, attempt_1:, attempt_2:)
    _, run = create_run(seed: seed, count: 1, max_concurrency: 1, scenario: "deterministic")
    task = CircuitBreaker::Storage.get_run_tasks(run["run_id"]).first
    task = task.merge(
      "duration" => 0.0,
      "retry_duration" => 0.0,
      "planned_execution_case" => "forced_#{attempt_1}_then_#{attempt_2 || "none"}",
      "planned_first_attempt_outcome" => OUTCOME_MAP[attempt_1],
      "planned_retry_attempt_outcome" => (attempt_2 && OUTCOME_MAP[attempt_2]),
      "planned_result" => {
        "scenario" => "deterministic",
        "task_index" => task["task_index"],
        "child_seed" => task["child_seed"],
        "value" => 1234
      },
      "result" => nil,
      "error" => nil,
      "reason" => nil
    )
    CircuitBreaker::Storage.update_task(task)
    [run, task]
  end

  def run_forced_task_to_terminal(task_id)
    transitions = []
    assert CircuitBreaker::Worker.process_once(0)
    task = CircuitBreaker::Storage.get_task(task_id)
    transitions << task["status"]

    if task["status"] == "retrying"
      assert CircuitBreaker::Worker.process_once(0)
      task = CircuitBreaker::Storage.get_task(task_id)
      transitions << task["status"]
    end

    [task, transitions]
  end
end
