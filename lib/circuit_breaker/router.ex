defmodule CircuitBreaker.Router do
  use Plug.Router

  alias CircuitBreaker.{Main, Storage}

  plug Plug.Logger
  plug :match
  plug Plug.Parsers, parsers: [:json], json_decoder: Jason
  plug :dispatch

  post "/runs" do
    with {:ok, payload} <- validate_create_run(conn.body_params),
         :ok <- validate_post_size(payload),
         :ok <- validate_global_concurrency(payload),
         :ok <- validate_memory(),
         :ok <- Main.reserve_run_concurrency(payload.max_concurrency),
         {:ok, run} <- create_reserved_run(payload) do
      json(conn, 200, %{"run_id" => run["run_id"], "status" => run["status"]})
    else
      {:error, :invalid, detail} ->
        json(conn, 422, %{"detail" => detail})

      {:error, :too_large, size, limit} ->
        json(conn, 413, %{
          "error" => "post_input_too_large",
          "message" => "POST /runs input values are #{size} bytes, which exceeds the #{limit} byte limit.",
          "size" => size,
          "limit" => limit
        })

      {:error, :insufficient_memory} ->
        json(conn, 503, %{
          "error" => "insufficient_memory",
          "message" => "Run was not queued because available memory is below #{Main.min_available_memory_mb()} MB.",
          "min_available_memory_mb" => Main.min_available_memory_mb()
        })

      {:error, {:capacity_exceeded, requested, reserved, limit}} ->
        json(conn, 409, %{
          "error" => "global_concurrency_capacity_exceeded",
          "message" => "Cannot reserve #{requested} concurrency slots because #{reserved} of #{limit} are already reserved by active runs.",
          "requested" => requested,
          "reserved" => reserved,
          "limit" => limit
        })

      {:error, {:duplicate_active_seed, run_id, status, payload}} ->
        Main.release_run_concurrency(payload.max_concurrency)

        json(conn, 409, %{
          "error" => "duplicate_active_seed",
          "message" => "A run with seed #{payload.seed} is already queued or running.",
          "existing_run_id" => run_id,
          "existing_status" => status
        })
    end
  end

  get "/runs/:run_id" do
    with {:ok, run_id} <- Main.validate_run_id(run_id),
         run when not is_nil(run) <- Storage.get_run(run_id) do
      json(conn, 200, %{
        "run_id" => run["run_id"],
        "scenario" => run["scenario"],
        "count" => run["count"],
        "seed" => run["seed"],
        "max_concurrency" => run["max_concurrency"],
        "status" => run["status"],
        "error" => run["error"],
        "message" => run["message"],
        "summary" => Storage.summarize_run(run_id)
      })
    else
      {:error, message} -> json(conn, 422, %{"detail" => message})
      nil -> json(conn, 404, %{"detail" => "Run not found"})
    end
  end

  get "/runs/:run_id/tasks" do
    with {:ok, run_id} <- Main.validate_run_id(run_id),
         run when not is_nil(run) <- Storage.get_run(run_id) do
      json(conn, 200, %{"tasks" => Storage.get_run_tasks(run_id)})
    else
      {:error, message} -> json(conn, 422, %{"detail" => message})
      nil -> json(conn, 404, %{"detail" => "Run not found"})
    end
  end

  match _ do
    json(conn, 404, %{"detail" => "Not Found"})
  end

  defp create_reserved_run(payload) do
    case Storage.create_run(payload,
           execution_variations: Main.task_execution_variations(),
           min_task_duration_seconds: Main.min_task_duration_seconds(),
           max_task_duration_seconds: Main.max_task_duration_seconds(),
           timeout_seconds: Main.max_subprocess_timeout_seconds()
         ) do
      {:ok, run} -> {:ok, run}
      {:error, {:duplicate_active_seed, run_id, status}} -> {:error, {:duplicate_active_seed, run_id, status, payload}}
      error ->
        Main.release_run_concurrency(payload.max_concurrency)
        error
    end
  end

  defp validate_create_run(%{} = params) do
    required = ["scenario", "count", "seed", "max_concurrency"]
    extra = Map.keys(params) -- required
    missing = required -- Map.keys(params)

    cond do
      missing != [] ->
        {:error, :invalid, Enum.map(missing, &"#{&1} is required")}

      extra != [] ->
        {:error, :invalid, Enum.map(extra, &"#{&1} is not allowed")}

      not is_binary(params["scenario"]) ->
        {:error, :invalid, "scenario must be a string"}

      String.trim(params["scenario"]) == "" ->
        {:error, :invalid, "scenario must not be blank"}

      not is_integer(params["count"]) or params["count"] < 1 ->
        {:error, :invalid, "count must be an integer greater than or equal to 1"}

      not is_integer(params["seed"]) ->
        {:error, :invalid, "seed must be an integer"}

      not is_integer(params["max_concurrency"]) or params["max_concurrency"] < 1 ->
        {:error, :invalid, "max_concurrency must be an integer greater than or equal to 1"}

      true ->
        {:ok,
         %{
           scenario: String.trim(params["scenario"]),
           count: params["count"],
           seed: params["seed"],
           max_concurrency: params["max_concurrency"]
         }}
    end
  end

  defp validate_post_size(payload) do
    size = Main.post_input_values_size(payload)
    limit = Main.max_task_record_size_bytes()
    if size > limit, do: {:error, :too_large, size, limit}, else: :ok
  end

  defp validate_global_concurrency(payload) do
    if payload.max_concurrency > Main.global_max_subtask_processes() do
      {:error, :invalid, "max_concurrency must be less than or equal to #{Main.global_max_subtask_processes()}"}
    else
      :ok
    end
  end

  defp validate_memory do
    if Main.memory_is_available(Main.min_available_memory_mb()), do: :ok, else: {:error, :insufficient_memory}
  end

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(body))
  end
end
