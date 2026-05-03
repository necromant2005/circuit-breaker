defmodule CircuitBreaker.WorkerStorageTest do
  use ExUnit.Case

  alias CircuitBreaker.{Main, Storage, Worker}

  @outcome_map %{
    "success" => "completed",
    "failed" => "failed",
    "timeout" => "timeout"
  }

  setup do
    Redix.command!(Storage.redis_name(), ["FLUSHDB"])
    Application.put_env(:circuit_breaker, :task_duration_scale, 0.0)
    Application.put_env(:circuit_breaker, :global_max_subtask_processes, 1000)
    Application.put_env(:circuit_breaker, :max_task_record_size_bytes, 100_000)
    Application.put_env(:circuit_breaker, :min_available_memory_mb, 256)
    Application.put_env(:circuit_breaker, :max_subprocess_timeout_seconds, 11)
    Application.put_env(:circuit_breaker, :max_task_retries, 1)
    Application.put_env(:circuit_breaker, :retry_delay_seconds, 1)
    :ok
  end

  test "concurrency slots respect per-run and global limits" do
    {:ok, run_a} = create_run(seed: 1, count: 12, max_concurrency: 7)
    {:ok, run_b} = create_run(seed: 2, count: 12, max_concurrency: 7)

    assert for(_ <- 1..7, do: Storage.acquire_slots(run_a["run_id"], 7, 10)) == List.duplicate(true, 7)
    refute Storage.acquire_slots(run_a["run_id"], 7, 10)

    assert for(_ <- 1..3, do: Storage.acquire_slots(run_b["run_id"], 7, 10)) == List.duplicate(true, 3)
    refute Storage.acquire_slots(run_b["run_id"], 7, 10)
    assert Storage.global_running_count() == 10
  end

  test "summarize running tasks counts global and run running" do
    {:ok, run_a} = create_run(seed: 103, count: 2, max_concurrency: 2)
    {:ok, run_b} = create_run(seed: 104, count: 1, max_concurrency: 1)

    for task <- Storage.get_run_tasks(run_a["run_id"]) do
      task |> Map.put("status", "running") |> Storage.update_task()
    end

    [task_b] = Storage.get_run_tasks(run_b["run_id"])
    task_b |> Map.put("status", "running") |> Storage.update_task()

    summary = Storage.summarize_running_tasks(run_a["run_id"])
    assert summary["global_running"] == 3
    assert summary["run_running"] == 2
    assert summary["global_limit"] == 1000
  end

  test "global subtask process limit returns early from summary" do
    Application.put_env(:circuit_breaker, :global_max_subtask_processes, 1)
    {:ok, run} = create_run(seed: 105, count: 2, max_concurrency: 2)
    [running_task, pending_task] = Storage.get_run_tasks(run["run_id"])
    running_task |> Map.put("status", "running") |> Storage.update_task()

    refute Worker.process_once_sync(0)
    refute Worker.process_once_sync(0)

    pending_task = Storage.get_task(pending_task["task_id"])
    run = Storage.get_run(run["run_id"])
    assert pending_task["status"] == "pending"
    assert pending_task["blocked_reason"] == "global_concurrency_limit"
    assert pending_task["message"] =~ "1 running processes"
    assert run["status"] == "queued"
  end

  test "run concurrency limit returns early from summary" do
    {:ok, run} = create_run(seed: 106, count: 2, max_concurrency: 1)
    [running_task, pending_task] = Storage.get_run_tasks(run["run_id"])
    running_task |> Map.put("status", "running") |> Storage.update_task()

    refute Worker.process_once_sync(0)
    refute Worker.process_once_sync(0)

    pending_task = Storage.get_task(pending_task["task_id"])
    assert pending_task["status"] == "pending"
    assert pending_task["blocked_reason"] == "run_concurrency_limit"
  end

  test "insufficient memory blocks task and releases acquired slot" do
    Application.put_env(:circuit_breaker, :min_available_memory_mb, 1_000_000_000)
    {:ok, run} = create_run(seed: 5, count: 1, max_concurrency: 1)

    refute Worker.process_once_sync(0)

    [task] = Storage.get_run_tasks(run["run_id"])
    assert task["status"] == "pending"
    assert task["blocked_reason"] == "insufficient_memory"
    assert task["message"] =~ "available memory"
    assert Storage.global_running_count() == 0
    assert Storage.run_running_count(run["run_id"]) == 0
  end

  test "finished run releases reserved concurrency" do
    payload = %{"scenario" => "demo", "count" => 1, "seed" => 46, "max_concurrency" => 1000}
    conn = post_run(payload)
    assert conn.status == 200
    run_id = Jason.decode!(conn.resp_body)["run_id"]
    assert Redix.command!(Storage.redis_name(), ["GET", Main.active_run_concurrency_key()]) == "1000"

    [task] = Storage.get_run_tasks(run_id)

    task
    |> Map.merge(%{"duration" => 0.0, "planned_first_attempt_outcome" => "completed", "planned_result" => %{"ok" => true}})
    |> Storage.update_task()

    assert Worker.process_once_sync(0)

    run = Storage.get_run(run_id)
    assert run["status"] == "completed"
    assert run["concurrency_released"] == true
    assert Redix.command!(Storage.redis_name(), ["GET", Main.active_run_concurrency_key()]) == "0"
  end

  @tag :capture_log
  test "all named task execution variations reach expected terminal status" do
    cases = [
      {"success", nil, "completed", 1, nil},
      {"failed", "success", "completed", 2, nil},
      {"timeout", "success", "completed", 2, nil},
      {"failed", "failed", "failed", 2, "task_failed"},
      {"failed", "timeout", "failed", 2, "task_timeout"},
      {"timeout", "failed", "failed", 2, "task_failed"},
      {"timeout", "timeout", "failed", 2, "task_timeout"}
    ]

    for {attempt_1, attempt_2, expected_status, expected_attempts, expected_error} <- cases do
      Redix.command!(Storage.redis_name(), ["FLUSHDB"])
      {_run, initial_task} = create_task_with_forced_plan(seed: 2000 + expected_attempts, attempt_1: attempt_1, attempt_2: attempt_2)

      {task, transitions} = run_forced_task_to_terminal(initial_task["task_id"])

      assert task["status"] == expected_status
      assert task["attempt"] == expected_attempts
      assert task["error"] == expected_error
      assert task["planned_first_attempt_outcome"] == @outcome_map[attempt_1]
      assert task["planned_retry_attempt_outcome"] == if(attempt_2, do: @outcome_map[attempt_2])

      if expected_status == "completed" do
        assert task["result"] == initial_task["planned_result"]
        assert task["error"] == nil
      else
        assert task["result"] == nil
        assert task["finished_at"] != nil
        assert String.downcase(task["message"]) =~ "failed"
      end

      if expected_attempts == 1 do
        assert transitions == ["completed"]
        assert task["reason"] == nil
        assert Redix.command!(Storage.redis_name(), ["LLEN", Storage.task_queue_key()]) == 0
      else
        assert hd(transitions) == "retrying"
        assert List.last(transitions) == expected_status
        expected_reason = if expected_status == "completed", do: @outcome_map[attempt_1], else: @outcome_map[attempt_2]
        assert task["reason"] == expected_reason
      end
    end
  end

  test "successful retry keeps first failure reason" do
    for attempt_1 <- ["failed", "timeout"] do
      Redix.command!(Storage.redis_name(), ["FLUSHDB"])
      {_run, initial_task} = create_task_with_forced_plan(seed: 3000 + byte_size(attempt_1), attempt_1: attempt_1, attempt_2: "success")

      assert Worker.process_once_sync(0)
      retrying_task = Storage.get_task(initial_task["task_id"])
      assert retrying_task["status"] == "retrying"
      assert retrying_task["reason"] == @outcome_map[attempt_1]
      assert retrying_task["error"] in ~w(task_failed task_timeout)

      assert Worker.process_once_sync(0)
      completed_task = Storage.get_task(initial_task["task_id"])
      assert completed_task["status"] == "completed"
      assert completed_task["attempt"] == 2
      assert completed_task["result"] == initial_task["planned_result"]
      assert completed_task["error"] == nil
      assert completed_task["reason"] == @outcome_map[attempt_1]
    end
  end

  test "configured retry count allows more attempts" do
    Application.put_env(:circuit_breaker, :max_task_retries, 2)
    {:ok, run} = create_run(seed: 109, count: 1, max_concurrency: 1)
    [task] = Storage.get_run_tasks(run["run_id"])

    task
    |> Map.merge(%{"duration" => 2.0, "retry_duration" => 2.0, "planned_first_attempt_outcome" => "failed", "planned_retry_attempt_outcome" => "failed"})
    |> Storage.update_task()

    for expected_attempt <- [1, 2] do
      assert Worker.process_once_sync(0)
      task = Storage.get_task(task["task_id"])
      assert task["status"] == "retrying"
      assert task["attempt"] == expected_attempt
    end

    assert Worker.process_once_sync(0)
    task = Storage.get_task(task["task_id"])
    assert task["status"] == "failed"
    assert task["attempt"] == 3
    assert task["error"] == "task_failed"
  end

  test "planned timeout outcome is not caused by short duration" do
    {:ok, run} = create_run(seed: 4, count: 3, max_concurrency: 1)
    task = Enum.at(Storage.get_run_tasks(run["run_id"]), 2)
    assert task["planned_first_attempt_outcome"] == "timeout"
    assert task["duration"] == 11.0
  end

  test "subprocess timeout can fail then succeed on retry" do
    {:ok, run} = create_run(seed: 9, count: 1, max_concurrency: 1)
    [task] = Storage.get_run_tasks(run["run_id"])

    task
    |> Map.merge(%{
      "duration" => 11.0,
      "retry_duration" => 1.0,
      "planned_first_attempt_outcome" => "completed",
      "planned_retry_attempt_outcome" => "completed",
      "planned_result" => %{"ok" => true}
    })
    |> Storage.update_task()

    Application.put_env(:circuit_breaker, :max_subprocess_timeout_seconds, 0)
    assert Worker.process_once_sync(0)
    retrying_task = Storage.get_task(task["task_id"])
    assert retrying_task["status"] == "retrying"
    assert retrying_task["error"] == "task_timeout"

    Application.put_env(:circuit_breaker, :max_subprocess_timeout_seconds, 11)
    assert Worker.process_once_sync(0)
    completed_task = Storage.get_task(task["task_id"])
    assert completed_task["status"] == "completed"
    assert completed_task["attempt"] == 2
    assert completed_task["result"] == %{"ok" => true}
  end

  test "repair running state retries first attempt and fails final attempt" do
    {:ok, run} = create_run(seed: 500, count: 2, max_concurrency: 1)
    [first, second] = Storage.get_run_tasks(run["run_id"])

    first |> Map.merge(%{"status" => "running", "attempt" => 1}) |> Storage.update_task()
    second |> Map.merge(%{"status" => "running", "attempt" => 2}) |> Storage.update_task()

    Storage.repair_running_state()

    first = Storage.get_task(first["task_id"])
    second = Storage.get_task(second["task_id"])

    assert first["status"] == "retrying"
    assert first["reason"] == "worker_restart"
    assert second["status"] == "failed"
    assert second["reason"] == "worker_restart"
    assert Redix.command!(Storage.redis_name(), ["GET", Storage.global_running_key()]) == "0"
  end

  defp create_task_with_forced_plan(opts) do
    {:ok, run} = create_run(seed: opts[:seed], count: 1, max_concurrency: 1, scenario: "deterministic")
    [task] = Storage.get_run_tasks(run["run_id"])
    attempt_1 = opts[:attempt_1]
    attempt_2 = opts[:attempt_2]

    task =
      task
      |> Map.merge(%{
        "duration" => 0.0,
        "retry_duration" => 0.0,
        "planned_execution_case" => "forced_#{attempt_1}_then_#{attempt_2 || "none"}",
        "planned_first_attempt_outcome" => @outcome_map[attempt_1],
        "planned_retry_attempt_outcome" => if(attempt_2, do: @outcome_map[attempt_2]),
        "planned_result" => %{
          "scenario" => "deterministic",
          "task_index" => task["task_index"],
          "child_seed" => task["child_seed"],
          "value" => 1234
        },
        "result" => nil,
        "error" => nil,
        "reason" => nil
      })

    Storage.update_task(task)
    {run, task}
  end

  defp run_forced_task_to_terminal(task_id) do
    transitions = []
    assert Worker.process_once_sync(0)
    task = Storage.get_task(task_id)
    transitions = transitions ++ [task["status"]]

    if task["status"] == "retrying" do
      assert Worker.process_once_sync(0)
      task = Storage.get_task(task_id)
      {task, transitions ++ [task["status"]]}
    else
      {task, transitions}
    end
  end

  defp create_run(opts) do
    Storage.create_run(%{
      scenario: Keyword.get(opts, :scenario, "demo"),
      count: Keyword.fetch!(opts, :count),
      seed: Keyword.fetch!(opts, :seed),
      max_concurrency: Keyword.fetch!(opts, :max_concurrency)
    })
  end

  defp post_run(payload) do
    body = Jason.encode!(payload)

    Plug.Test.conn(:post, "/runs", body)
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> CircuitBreaker.Router.call(CircuitBreaker.Router.init([]))
  end
end
