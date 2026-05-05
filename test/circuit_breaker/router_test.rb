# frozen_string_literal: true

require_relative "../test_helper"

class RouterTest < Minitest::Test
  include TestHelpers

  def test_post_runs_creates_a_run_and_tasks
    response = post_json("/runs", "scenario" => "demo", "count" => 2, "seed" => 42, "max_concurrency" => 2)

    assert_equal 200, response.status
    created = json(response)
    assert_equal "queued", created["status"]

    tasks_response = get("/runs/#{created["run_id"]}/tasks")
    assert_equal 200, tasks_response.status
    assert_equal 2, json(tasks_response)["tasks"].length
  end

  def test_duplicate_active_seed_returns_409
    payload = { "scenario" => "demo", "count" => 1, "seed" => 99, "max_concurrency" => 1 }

    assert_equal 200, post_json("/runs", payload).status
    response = post_json("/runs", payload)

    assert_equal 409, response.status
    assert_equal "duplicate_active_seed", json(response)["error"]
  end

  def test_global_active_run_reservation_rejects_overbooking
    post_json("/runs", "scenario" => "demo", "count" => 1, "seed" => 1, "max_concurrency" => 1000)

    response = post_json("/runs", "scenario" => "demo", "count" => 1, "seed" => 2, "max_concurrency" => 1000)

    assert_equal 409, response.status
    assert_equal "global_concurrency_capacity_exceeded", json(response)["error"]
    assert_equal "1000", CircuitBreaker::Storage.redis.get(CircuitBreaker::Main.active_run_concurrency_key)
  end

  def test_invalid_run_id_returns_422
    response = get("/runs/not-a-uuid")
    assert_equal 422, response.status
    assert_equal "run_id must be a valid UUID", json(response)["detail"]
  end

  def test_task_path_invalid_run_id_returns_422
    response = get("/runs/not-a-uuid/tasks")
    assert_equal 422, response.status
    assert_equal "run_id must be a valid UUID", json(response)["detail"]
  end

  def test_valid_missing_run_id_returns_404_for_run_and_tasks_paths
    run_id = "00000000-0000-0000-0000-000000000000"

    assert_equal 404, get("/runs/#{run_id}").status
    assert_equal 404, get("/runs/#{run_id}/tasks").status
  end

  def test_post_runs_requires_all_fields
    base = { "scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => 1 }

    base.keys.each do |missing_field|
      response = post_json("/runs", base.reject { |key, _| key == missing_field })

      assert_equal 422, response.status
      assert_includes json(response)["detail"].inspect, missing_field
    end
  end

  def test_post_runs_rejects_invalid_fields
    invalid_payloads = [
      { "scenario" => "", "count" => 1, "seed" => 10, "max_concurrency" => 1 },
      { "scenario" => "   ", "count" => 1, "seed" => 10, "max_concurrency" => 1 },
      { "scenario" => 123, "count" => 1, "seed" => 10, "max_concurrency" => 1 },
      { "scenario" => "demo", "count" => 0, "seed" => 10, "max_concurrency" => 1 },
      { "scenario" => "demo", "count" => "1", "seed" => 10, "max_concurrency" => 1 },
      { "scenario" => "demo", "count" => 1, "seed" => "10", "max_concurrency" => 1 },
      { "scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => 0 },
      { "scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => "1" },
      { "scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => 1, "extra" => "not allowed" }
    ]

    invalid_payloads.each { |payload| assert_equal 422, post_json("/runs", payload).status }
  end

  def test_post_runs_rejects_max_concurrency_above_global_limit
    response = post_json("/runs", "scenario" => "demo", "count" => 500, "seed" => 45, "max_concurrency" => 10_000)

    assert_equal 422, response.status
    assert_includes json(response)["detail"], "max_concurrency must be less than or equal to 1000"
  end

  def test_post_runs_rejects_oversized_input_values_only
    CircuitBreaker::Main.max_task_record_size_bytes = 500

    response = post_json("/runs", "scenario" => "x" * 1000, "count" => 1, "seed" => 101, "max_concurrency" => 1)
    assert_equal 413, response.status
    assert_equal "post_input_too_large", json(response)["error"]

    CircuitBreaker::Main.max_task_record_size_bytes = 10
    response = post_json("/runs", "scenario" => "abcd", "count" => 12, "seed" => 34, "max_concurrency" => 5)
    assert_equal 200, response.status
  end

  def test_post_runs_rejects_before_redis_writes_when_memory_is_unavailable
    CircuitBreaker::Main.min_available_memory_mb = 1_000_000_000

    response = post_json("/runs", "scenario" => "demo", "count" => 1, "seed" => 107, "max_concurrency" => 1)

    assert_equal 503, response.status
    assert_equal "insufficient_memory", json(response)["error"]
    assert_empty CircuitBreaker::Storage.redis.keys("run:*")
    assert_equal 0, CircuitBreaker::Storage.redis.llen(CircuitBreaker::Storage.task_queue_key)
    assert_nil CircuitBreaker::Storage.redis.get(CircuitBreaker::Main.active_run_concurrency_key)
  end
end
