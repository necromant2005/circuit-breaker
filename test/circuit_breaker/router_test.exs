defmodule CircuitBreaker.RouterTest do
  use ExUnit.Case
  import Plug.Conn
  import Plug.Test

  alias CircuitBreaker.{Main, Router, Storage}

  @opts Router.init([])

  setup do
    Redix.command!(Storage.redis_name(), ["FLUSHDB"])
    Application.put_env(:circuit_breaker, :global_max_subtask_processes, 1000)
    Application.put_env(:circuit_breaker, :max_task_record_size_bytes, 100_000)
    Application.put_env(:circuit_breaker, :min_available_memory_mb, 256)
    :ok
  end

  test "POST /runs creates a run and tasks" do
    body = Jason.encode!(%{"scenario" => "demo", "count" => 2, "seed" => 42, "max_concurrency" => 2})

    response =
      conn(:post, "/runs", body)
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert response.status == 200
    created = Jason.decode!(response.resp_body)
    assert created["status"] == "queued"

    tasks_response =
      conn(:get, "/runs/#{created["run_id"]}/tasks")
      |> Router.call(@opts)

    assert tasks_response.status == 200
    assert %{"tasks" => tasks} = Jason.decode!(tasks_response.resp_body)
    assert length(tasks) == 2
  end

  test "duplicate active seed returns 409" do
    body = Jason.encode!(%{"scenario" => "demo", "count" => 1, "seed" => 99, "max_concurrency" => 1})

    first =
      conn(:post, "/runs", body)
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    second =
      conn(:post, "/runs", body)
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert first.status == 200
    assert second.status == 409
    assert Jason.decode!(second.resp_body)["error"] == "duplicate_active_seed"
  end

  test "global active run reservation rejects overbooking" do
    first = Jason.encode!(%{"scenario" => "demo", "count" => 1, "seed" => 1, "max_concurrency" => 1000})
    second = Jason.encode!(%{"scenario" => "demo", "count" => 1, "seed" => 2, "max_concurrency" => 1000})

    conn(:post, "/runs", first)
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    response =
      conn(:post, "/runs", second)
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert response.status == 409
    assert Jason.decode!(response.resp_body)["error"] == "global_concurrency_capacity_exceeded"
    assert Redix.command!(Storage.redis_name(), ["GET", Main.active_run_concurrency_key()]) == "1000"
  end

  test "invalid run id returns 422" do
    response = conn(:get, "/runs/not-a-uuid") |> Router.call(@opts)
    assert response.status == 422
    assert Jason.decode!(response.resp_body)["detail"] == "run_id must be a valid UUID"
  end

  test "task path invalid run id returns 422" do
    response = conn(:get, "/runs/not-a-uuid/tasks") |> Router.call(@opts)
    assert response.status == 422
    assert Jason.decode!(response.resp_body)["detail"] == "run_id must be a valid UUID"
  end

  test "valid missing run id returns 404 for run and tasks paths" do
    run_id = "00000000-0000-0000-0000-000000000000"

    assert (conn(:get, "/runs/#{run_id}") |> Router.call(@opts)).status == 404
    assert (conn(:get, "/runs/#{run_id}/tasks") |> Router.call(@opts)).status == 404
  end

  test "POST /runs requires all fields" do
    base = %{"scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => 1}

    for missing_field <- Map.keys(base) do
      response = post_json("/runs", Map.delete(base, missing_field))

      assert response.status == 422
      assert inspect(Jason.decode!(response.resp_body)["detail"]) =~ missing_field
    end
  end

  test "POST /runs rejects invalid fields" do
    invalid_payloads = [
      %{"scenario" => "", "count" => 1, "seed" => 10, "max_concurrency" => 1},
      %{"scenario" => "   ", "count" => 1, "seed" => 10, "max_concurrency" => 1},
      %{"scenario" => 123, "count" => 1, "seed" => 10, "max_concurrency" => 1},
      %{"scenario" => "demo", "count" => 0, "seed" => 10, "max_concurrency" => 1},
      %{"scenario" => "demo", "count" => "1", "seed" => 10, "max_concurrency" => 1},
      %{"scenario" => "demo", "count" => 1, "seed" => "10", "max_concurrency" => 1},
      %{"scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => 0},
      %{"scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => "1"},
      %{"scenario" => "demo", "count" => 1, "seed" => 10, "max_concurrency" => 1, "extra" => "not allowed"}
    ]

    for payload <- invalid_payloads do
      assert post_json("/runs", payload).status == 422
    end
  end

  test "POST /runs rejects max concurrency above global limit" do
    response = post_json("/runs", %{"scenario" => "demo", "count" => 500, "seed" => 45, "max_concurrency" => 10000})

    assert response.status == 422
    assert Jason.decode!(response.resp_body)["detail"] =~ "max_concurrency must be less than or equal to 1000"
  end

  test "POST /runs rejects oversized input values only" do
    Application.put_env(:circuit_breaker, :max_task_record_size_bytes, 500)

    response = post_json("/runs", %{"scenario" => String.duplicate("x", 1000), "count" => 1, "seed" => 101, "max_concurrency" => 1})
    assert response.status == 413
    assert Jason.decode!(response.resp_body)["error"] == "post_input_too_large"

    Application.put_env(:circuit_breaker, :max_task_record_size_bytes, 10)
    response = post_json("/runs", %{"scenario" => "abcd", "count" => 12, "seed" => 34, "max_concurrency" => 5})
    assert response.status == 200
  end

  test "POST /runs rejects before Redis writes when memory is unavailable" do
    Application.put_env(:circuit_breaker, :min_available_memory_mb, 1_000_000_000)

    response = post_json("/runs", %{"scenario" => "demo", "count" => 1, "seed" => 107, "max_concurrency" => 1})

    assert response.status == 503
    assert Jason.decode!(response.resp_body)["error"] == "insufficient_memory"
    assert Redix.command!(Storage.redis_name(), ["KEYS", "run:*"]) == []
    assert Redix.command!(Storage.redis_name(), ["LLEN", Storage.task_queue_key()]) == 0
    assert Redix.command!(Storage.redis_name(), ["GET", Main.active_run_concurrency_key()]) == nil
  end

  defp post_json(path, payload) do
    conn(:post, path, Jason.encode!(payload))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)
  end
end
