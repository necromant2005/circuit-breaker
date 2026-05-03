defmodule CircuitBreaker.Worker do
  use GenServer

  alias CircuitBreaker.{Main, Storage}

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state) do
    Storage.repair_running_state()
    release_finished_run_reservations()
    send(self(), :process)
    {:ok, state}
  end

  @impl true
  def handle_info(:process, state) do
    process_once()
    Process.send_after(self(), :process, 10)
    {:noreply, state}
  end

  def run_forever do
    Storage.repair_running_state()
    release_finished_run_reservations()
    loop_forever()
  end

  def process_once(poll_timeout \\ 1), do: process_once_internal(poll_timeout, true)
  def process_once_sync(poll_timeout \\ 1), do: process_once_internal(poll_timeout, false)

  defp process_once_internal(poll_timeout, async?) do
    case Storage.pop_task_id(poll_timeout) do
      nil ->
        false

      task_id ->
        task = Storage.get_task(task_id)

        cond do
          is_nil(task) or task["status"] in ~w(running completed failed) ->
            false

          true ->
            run = Storage.get_run(task["run_id"])

            if run && can_start_task?(run, task) do
              if async? do
                Task.start(fn -> execute_task(run, task) end)
              else
                execute_task(run, task)
              end

              true
            else
              refreshed = Storage.get_task(task_id)

              if refreshed && refreshed["status"] not in ~w(completed failed) do
                Storage.enqueue_task(task_id)
              end

              Process.sleep(200)
              false
            end
        end
    end
  end

  def run_subprocess_simulation(task) do
    outcome = if task["planned_first_attempt_outcome"], do: outcome_for_attempt(task)

    duration =
      if to_int(Map.get(task, "attempt", 1)) > 1 && task["retry_duration"] != nil do
        to_float(task["retry_duration"])
      else
        to_float(task["duration"])
      end

    scale = Application.get_env(:circuit_breaker, :task_duration_scale, 1.0)
    timeout = Main.max_subprocess_timeout_seconds()

    cond do
      outcome == "timeout" ->
        sleep_seconds(max(duration, timeout) * scale)
        raise TimeoutError, message: "subprocess reached #{timeout} second kill timeout"

      duration * scale >= timeout ->
        sleep_seconds(timeout)
        raise TimeoutError, message: "subprocess reached #{timeout} second kill timeout"

      true ->
        sleep_seconds(duration * scale)
        :ok
    end
  end

  def delay_before_retry(failed_count) do
    delay = Main.retry_delay_seconds(failed_count) * Application.get_env(:circuit_breaker, :task_duration_scale, 1.0)
    if delay > 0, do: sleep_seconds(delay)
  end

  defp can_start_task?(run, task) do
    summary = Storage.summarize_running_tasks(run["run_id"])

    cond do
      summary["global_running"] >= summary["global_limit"] ->
        task
        |> Map.put("blocked_reason", "global_concurrency_limit")
        |> Map.put("message", "Task is waiting because the global subtask process limit of #{summary["global_limit"]} running processes has been reached.")
        |> Storage.update_task()

        false

      summary["run_running"] >= to_int(run["max_concurrency"]) ->
        task
        |> Map.put("blocked_reason", "run_concurrency_limit")
        |> Map.put("message", "Task is waiting because the run concurrency limit of #{run["max_concurrency"]} running tasks has been reached.")
        |> Storage.update_task()

        false

      not Storage.acquire_slots(run["run_id"], to_int(run["max_concurrency"])) ->
        task
        |> Map.put("blocked_reason", "global_concurrency_limit")
        |> Map.put("message", "Task is waiting for an available execution slot.")
        |> Storage.update_task()

        false

      not Main.memory_is_available(Main.min_available_memory_mb()) ->
        Storage.release_slots(run["run_id"])

        task
        |> Map.put("blocked_reason", "insufficient_memory")
        |> Map.put("message", "Task is waiting because available memory is below #{Main.min_available_memory_mb()} MB.")
        |> Storage.update_task()

        false

      true ->
        true
    end
  end

  defp execute_task(run, task) do
    task =
      task
      |> Map.put("blocked_reason", nil)
      |> Map.put("status", "running")
      |> Map.put("attempt", to_int(task["attempt"]) + 1)
      |> Map.put("started_at", task["started_at"] || Storage.utc_now())
      |> Map.put("message", "Task attempt #{to_int(task["attempt"]) + 1} is running.")

    Storage.update_task(task)
    Storage.mark_run_running(run["run_id"])

    try do
      run_subprocess_simulation(task)
      handle_planned_outcome(task)
    rescue
      _ in TimeoutError ->
        handle_timeout(task)

      exc ->
        handle_unknown(task, exc)
    after
      Storage.release_slots(run["run_id"])
      Storage.refresh_run_terminal_state(run["run_id"])
      release_run_reservation_if_terminal(run["run_id"])
    end
  end

  defp handle_planned_outcome(task) do
    outcome = outcome_for_attempt(task)

    if outcome == "completed" do
      task
      |> Map.put("status", "completed")
      |> Map.put("result", task["planned_result"])
      |> Map.put("error", nil)
      |> Map.put("message", "Task completed successfully.")
      |> Map.put("finished_at", Storage.utc_now())
      |> Storage.update_task()
    else
      task = task |> Map.put("error", error_for_outcome(outcome)) |> Map.put("reason", outcome)

      if can_retry?(task) do
        task
        |> Map.put("status", "retrying")
        |> Map.put("message", "Task attempt #{task["attempt"]} ended with #{outcome}; retrying.")
        |> Storage.update_task()

        delay_before_retry(to_int(task["attempt"]))
        Storage.enqueue_retry_task(task["task_id"])
      else
        task
        |> Map.put("status", "failed")
        |> Map.put("message", "Task failed after #{task["attempt"]} attempts. Last reason: #{outcome}.")
        |> Map.put("finished_at", Storage.utc_now())
        |> Storage.update_task()
      end
    end
  end

  defp handle_timeout(task) do
    task = task |> Map.put("error", "task_timeout") |> Map.put("reason", "timeout")

    if can_retry?(task) do
      task
      |> Map.put("status", "retrying")
      |> Map.put("message", "Task attempt #{task["attempt"]} reached the #{Main.max_subprocess_timeout_seconds()} second kill timeout; retrying.")
      |> Storage.update_task()

      delay_before_retry(to_int(task["attempt"]))
      Storage.enqueue_retry_task(task["task_id"])
    else
      task
      |> Map.put("status", "failed")
      |> Map.put("message", "Task failed because the subprocess reached the #{Main.max_subprocess_timeout_seconds()} second kill timeout twice.")
      |> Map.put("finished_at", Storage.utc_now())
      |> Storage.update_task()
    end
  end

  defp handle_unknown(task, exc) do
    task =
      task
      |> Map.put("error", "unknown_error")
      |> Map.put("reason", inspect(exc.__struct__))
      |> Map.put("message", "Task failed with unexpected error: #{Exception.message(exc)}")

    if can_retry?(task) do
      task |> Map.put("status", "retrying") |> Storage.update_task()
      delay_before_retry(to_int(task["attempt"]))
      Storage.enqueue_retry_task(task["task_id"])
    else
      task |> Map.put("status", "failed") |> Map.put("finished_at", Storage.utc_now()) |> Storage.update_task()
    end
  end

  defp can_retry?(task), do: to_int(task["attempt"]) <= Main.max_task_retries()

  defp outcome_for_attempt(task) do
    if to_int(task["attempt"]) == 1 do
      task["planned_first_attempt_outcome"]
    else
      task["planned_retry_attempt_outcome"] || task["planned_first_attempt_outcome"]
    end
  end

  defp error_for_outcome("failed"), do: "task_failed"
  defp error_for_outcome("timeout"), do: "task_timeout"
  defp error_for_outcome(_), do: "unknown_error"

  defp release_run_reservation_if_terminal(run_id) do
    if amount = Storage.mark_run_concurrency_released(run_id) do
      Main.release_run_concurrency(amount)
    end
  end

  defp release_finished_run_reservations do
    Redix.command!(Storage.redis_name(), ["SCAN", "0", "MATCH", "run:*"])
    |> scan_release_keys()
  end

  defp scan_release_keys([cursor, keys]) do
    keys
    |> Enum.reject(&(String.ends_with?(&1, ":tasks") or String.ends_with?(&1, ":running_tasks")))
    |> Enum.each(fn "run:" <> run_id -> release_run_reservation_if_terminal(run_id) end)

    if cursor != "0" do
      Redix.command!(Storage.redis_name(), ["SCAN", cursor, "MATCH", "run:*"])
      |> scan_release_keys()
    end
  end

  defp loop_forever do
    process_once()
    loop_forever()
  end

  defp sleep_seconds(seconds), do: Process.sleep(round(seconds * 1000))
  defp to_int(value) when is_integer(value), do: value
  defp to_int(value) when is_binary(value), do: String.to_integer(value)
  defp to_float(value) when is_float(value), do: value
  defp to_float(value) when is_integer(value), do: value * 1.0
  defp to_float(value) when is_binary(value), do: String.to_float(value)
end

defmodule TimeoutError do
  defexception [:message]
end
