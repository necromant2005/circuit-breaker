defmodule CircuitBreaker.Storage do
  alias CircuitBreaker.{Main, Models, Planner}

  @redis_name __MODULE__.Redis
  @task_queue_key "queue:tasks"
  @run_queue_key "queue:runs"
  @global_running_key "global:running_tasks"

  def redis_name, do: @redis_name
  def task_queue_key, do: @task_queue_key
  def global_running_key, do: @global_running_key

  def create_run(request, opts \\ []) do
    active_key = active_seed_key(request.seed)

    with {:ok, nil} <- active_run_for_seed(active_key) do
      run_id = uuid4()
      now = utc_now()

      run = %{
        "run_id" => run_id,
        "scenario" => request.scenario,
        "count" => request.count,
        "seed" => request.seed,
        "max_concurrency" => request.max_concurrency,
        "status" => "queued",
        "error" => nil,
        "message" => nil,
        "created_at" => now,
        "updated_at" => now,
        "started_at" => nil,
        "finished_at" => nil,
        "concurrency_released" => false
      }

      tasks =
        Planner.build_task_plan(
          run_id: run_id,
          scenario: request.scenario,
          run_seed: request.seed,
          count: request.count,
          execution_variations: Keyword.get(opts, :execution_variations, Main.task_execution_variations()),
          min_task_duration_seconds: Keyword.get(opts, :min_task_duration_seconds, Main.min_task_duration_seconds()),
          max_task_duration_seconds: Keyword.get(opts, :max_task_duration_seconds, Main.max_task_duration_seconds()),
          timeout_seconds: Keyword.get(opts, :timeout_seconds, Main.max_subprocess_timeout_seconds())
        )
        |> Enum.map(&Map.merge(&1, %{"created_at" => now, "updated_at" => now}))

      case Redix.command(@redis_name, ["SET", active_key, run_id, "NX"]) do
        {:ok, "OK"} ->
          commands =
            [
              ["SET", run_key(run_id), Jason.encode!(run)],
              ["DEL", run_tasks_key(run_id)]
            ] ++
              Enum.flat_map(tasks, fn task ->
                [
                  ["SET", task_key(task["task_id"]), Jason.encode!(task)],
                  ["RPUSH", run_tasks_key(run_id), task["task_id"]],
                  ["RPUSH", @task_queue_key, task["task_id"]]
                ]
              end) ++
              [
                ["RPUSH", @run_queue_key, run_id],
                ["SET", run_running_key(run_id), 0]
              ]

          case Redix.pipeline(@redis_name, commands) do
            {:ok, _} ->
              {:ok, run}

            error ->
              Redix.command(@redis_name, ["DEL", active_key])
              error
          end

        _ ->
          duplicate_active_seed(active_key)
      end
    end
  end

  def active_run_concurrency_total do
    scan_keys("run:*")
    |> Enum.reject(&(String.ends_with?(&1, ":tasks") or String.ends_with?(&1, ":running_tasks")))
    |> Enum.reduce(0, fn key, total ->
      case Redix.command(@redis_name, ["GET", key]) do
        {:ok, nil} -> total
        {:ok, raw} ->
          run = Jason.decode!(raw)
          if run["status"] in ~w(queued running), do: total + to_int(run["max_concurrency"]), else: total
      end
    end)
  end

  def get_run(nil), do: nil
  def get_run(run_id), do: get_json(run_key(run_id))
  def get_task(task_id), do: get_json(task_key(task_id))

  def get_run_tasks(run_id) do
    {:ok, ids} = Redix.command(@redis_name, ["LRANGE", run_tasks_key(run_id), 0, -1])

    ids
    |> Enum.map(&get_task/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(& &1["task_index"])
  end

  def update_run(run) do
    run = Map.put(run, "updated_at", utc_now())
    Redix.command(@redis_name, ["SET", run_key(run["run_id"]), Jason.encode!(run)])
  end

  def update_task(task) do
    task = Map.put(task, "updated_at", utc_now())
    Redix.command(@redis_name, ["SET", task_key(task["task_id"]), Jason.encode!(task)])
  end

  def enqueue_task(task_id), do: Redix.command(@redis_name, ["RPUSH", @task_queue_key, task_id])

  def pop_task_id(timeout \\ 1) do
    case Redix.command(@redis_name, ["BLPOP", @task_queue_key, timeout]) do
      {:ok, nil} -> nil
      {:ok, [_key, task_id]} -> task_id
    end
  end

  def acquire_slots(run_id, max_concurrency, global_limit \\ Main.global_max_subtask_processes()) do
    script = """
    local global_key = KEYS[1]
    local run_key = KEYS[2]
    local global_limit = tonumber(ARGV[1])
    local run_limit = tonumber(ARGV[2])
    local global_running = tonumber(redis.call('GET', global_key) or '0')
    local run_running = tonumber(redis.call('GET', run_key) or '0')
    if global_running >= global_limit or run_running >= run_limit then
      return 0
    end
    redis.call('INCR', global_key)
    redis.call('INCR', run_key)
    return 1
    """

    case Redix.command(@redis_name, ["EVAL", script, "2", @global_running_key, run_running_key(run_id), global_limit, max_concurrency]) do
      {:ok, 1} -> true
      _ -> false
    end
  end

  def release_slots(run_id) do
    script = """
    local global_key = KEYS[1]
    local run_key = KEYS[2]
    local global_running = tonumber(redis.call('GET', global_key) or '0')
    local run_running = tonumber(redis.call('GET', run_key) or '0')
    if global_running > 0 then redis.call('DECR', global_key) end
    if run_running > 0 then redis.call('DECR', run_key) end
    return 1
    """

    Redix.command(@redis_name, ["EVAL", script, "2", @global_running_key, run_running_key(run_id)])
  end

  def global_running_count do
    case Redix.command(@redis_name, ["GET", @global_running_key]) do
      {:ok, nil} -> 0
      {:ok, value} -> to_int(value)
    end
  end

  def run_running_count(run_id) do
    case Redix.command(@redis_name, ["GET", run_running_key(run_id)]) do
      {:ok, nil} -> 0
      {:ok, value} -> to_int(value)
    end
  end

  def summarize_running_tasks(run_id, global_limit \\ Main.global_max_subtask_processes()) do
    scan_keys("task:*")
    |> Enum.reduce(%{"global_running" => 0, "run_running" => 0, "global_limit" => global_limit}, fn key, acc ->
      task = key |> get_json_by_key()

      if task && task["status"] == "running" do
        acc
        |> Map.update!("global_running", &(&1 + 1))
        |> then(fn updated ->
          if task["run_id"] == run_id, do: Map.update!(updated, "run_running", &(&1 + 1)), else: updated
        end)
      else
        acc
      end
    end)
  end

  def summarize_run(run_id) do
    counts = get_run_tasks(run_id) |> Enum.frequencies_by(& &1["status"])

    %{
      "pending" => Map.get(counts, "pending", 0),
      "running" => Map.get(counts, "running", 0),
      "retrying" => Map.get(counts, "retrying", 0),
      "completed" => Map.get(counts, "completed", 0),
      "failed" => Map.get(counts, "failed", 0)
    }
  end

  def refresh_run_terminal_state(run_id) do
    run = get_run(run_id)

    cond do
      is_nil(run) or Models.terminal_run_status?(run["status"]) ->
        :ok

      true ->
        tasks = get_run_tasks(run_id)
        unfinished = Enum.reject(tasks, &Models.terminal_task_status?(&1["status"]))

        if tasks != [] and unfinished == [] do
          failed = Enum.filter(tasks, &(&1["status"] == "failed"))
          run = Map.put(run, "finished_at", utc_now())

          run =
            if failed == [] do
              run
              |> Map.put("status", "completed")
              |> Map.put("error", nil)
              |> Map.put("message", "Run completed successfully.")
            else
              plural = if length(failed) == 1, do: "", else: "s"

              run
              |> Map.put("status", "failed")
              |> Map.put("error", "one_or_more_tasks_failed")
              |> Map.put("message", "Run failed because #{length(failed)} task#{plural} failed after retry.")
            end

          update_run(run)
          Redix.command(@redis_name, ["DEL", active_seed_key(run["seed"])])
        end
    end
  end

  def mark_run_concurrency_released(run_id) do
    run = get_run(run_id)

    if run && run["status"] in ~w(completed failed) && !run["concurrency_released"] do
      update_run(Map.put(run, "concurrency_released", true))
      to_int(run["max_concurrency"])
    end
  end

  def mark_run_running(run_id) do
    run = get_run(run_id)

    if run && run["status"] == "queued" do
      now = utc_now()

      run
      |> Map.put("status", "running")
      |> Map.put("started_at", now)
      |> Map.put("updated_at", now)
      |> then(&Redix.command(@redis_name, ["SET", run_key(run_id), Jason.encode!(&1)]))
    end
  end

  def repair_running_state do
    Redix.command(@redis_name, ["SET", @global_running_key, 0])

    scan_keys("run:*:running_tasks")
    |> Enum.each(&Redix.command(@redis_name, ["SET", &1, 0]))

    scan_keys("task:*")
    |> Enum.each(fn key ->
      task = get_json_by_key(key)

      cond do
        task["status"] == "running" && to_int(task["attempt"]) < 2 ->
          task
          |> Map.merge(%{
            "status" => "retrying",
            "error" => "unknown_error",
            "reason" => "worker_restart",
            "message" => "Task was interrupted by worker restart; retrying once."
          })
          |> update_task()

          enqueue_task(task["task_id"])

        task["status"] == "running" ->
          task
          |> Map.merge(%{
            "status" => "failed",
            "error" => "unknown_error",
            "reason" => "worker_restart",
            "message" => "Task failed because worker restarted during final attempt.",
            "finished_at" => utc_now()
          })
          |> update_task()

          refresh_run_terminal_state(task["run_id"])

        task["status"] == "retrying" ->
          enqueue_task(task["task_id"])

        true ->
          :ok
      end
    end)
  end

  def utc_now, do: DateTime.utc_now() |> DateTime.to_iso8601()

  defp active_run_for_seed(active_key) do
    case Redix.command(@redis_name, ["GET", active_key]) do
      {:ok, nil} -> {:ok, nil}
      {:ok, run_id} ->
        run = get_run(run_id)

        if run && run["status"] in ~w(queued running) do
          {:error, {:duplicate_active_seed, run_id, run["status"]}}
        else
          Redix.command(@redis_name, ["DEL", active_key])
          {:ok, nil}
        end
    end
  end

  defp duplicate_active_seed(active_key) do
    {:ok, run_id} = Redix.command(@redis_name, ["GET", active_key])
    run = get_run(run_id)
    {:error, {:duplicate_active_seed, run_id || "unknown", if(run, do: run["status"], else: "queued")}}
  end

  defp get_json(key), do: get_json_by_key(key)

  defp get_json_by_key(key) do
    case Redix.command(@redis_name, ["GET", key]) do
      {:ok, nil} -> nil
      {:ok, raw} -> Jason.decode!(raw)
    end
  end

  defp scan_keys(match) do
    do_scan("0", match, [])
  end

  defp do_scan(cursor, match, acc) do
    {:ok, [next_cursor, keys]} = Redix.command(@redis_name, ["SCAN", cursor, "MATCH", match])
    acc = acc ++ keys
    if next_cursor == "0", do: acc, else: do_scan(next_cursor, match, acc)
  end

  defp uuid4 do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)
    c = Bitwise.band(c, 0x0FFF) |> Bitwise.bor(0x4000)
    d = Bitwise.band(d, 0x3FFF) |> Bitwise.bor(0x8000)
    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e]) |> IO.iodata_to_binary()
  end

  defp run_key(run_id), do: "run:#{run_id}"
  defp run_tasks_key(run_id), do: "run:#{run_id}:tasks"
  defp task_key(task_id), do: "task:#{task_id}"
  defp active_seed_key(seed), do: "seed:#{seed}:active_run"
  defp run_running_key(run_id), do: "run:#{run_id}:running_tasks"
  defp to_int(value) when is_integer(value), do: value
  defp to_int(value) when is_binary(value), do: String.to_integer(value)
end
