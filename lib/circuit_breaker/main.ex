defmodule CircuitBreaker.Main do
  @moduledoc """
  Service policy surface: limits, admission checks, and named task execution cases.
  """

  alias CircuitBreaker.Storage

  @global_max_subtask_processes 1000
  @active_run_concurrency_key "active_runs:reserved_concurrency"
  @max_task_record_size_bytes 100_000
  @min_available_memory_mb 256
  @min_task_duration_seconds 2
  @max_task_duration_seconds 10
  @max_subprocess_timeout_seconds 11
  @max_task_retries 1
  @retry_delay_seconds 1

  # task_index rem length(task_execution_variations()) selects one plan in order.
  @task_execution_variations [
    {"success_first_attempt", %{"attempt_1" => "completed", "attempt_2" => nil}},
    {"failed_then_success", %{"attempt_1" => "failed", "attempt_2" => "completed"}},
    {"timeout_then_success", %{"attempt_1" => "timeout", "attempt_2" => "completed"}},
    {"failed_then_failed", %{"attempt_1" => "failed", "attempt_2" => "failed"}},
    {"failed_then_timeout", %{"attempt_1" => "failed", "attempt_2" => "timeout"}},
    {"timeout_then_failed", %{"attempt_1" => "timeout", "attempt_2" => "failed"}},
    {"timeout_then_timeout", %{"attempt_1" => "timeout", "attempt_2" => "timeout"}}
  ]

  def global_max_subtask_processes do
    Application.get_env(:circuit_breaker, :global_max_subtask_processes, @global_max_subtask_processes)
  end

  def active_run_concurrency_key, do: @active_run_concurrency_key
  def max_task_record_size_bytes, do: Application.get_env(:circuit_breaker, :max_task_record_size_bytes, @max_task_record_size_bytes)
  def min_available_memory_mb, do: Application.get_env(:circuit_breaker, :min_available_memory_mb, @min_available_memory_mb)
  def min_task_duration_seconds, do: @min_task_duration_seconds
  def max_task_duration_seconds, do: @max_task_duration_seconds
  def max_subprocess_timeout_seconds, do: Application.get_env(:circuit_breaker, :max_subprocess_timeout_seconds, @max_subprocess_timeout_seconds)
  def max_task_retries, do: Application.get_env(:circuit_breaker, :max_task_retries, @max_task_retries)
  def retry_delay_seconds, do: Application.get_env(:circuit_breaker, :retry_delay_seconds, @retry_delay_seconds)
  def retry_delay_seconds(failed_count), do: retry_delay_seconds() * failed_count
  def task_execution_variations, do: @task_execution_variations

  def available_memory_mb do
    case File.read("/proc/meminfo") do
      {:ok, content} ->
        case Regex.run(~r/^MemAvailable:\s+(\d+)\s+kB/m, content) do
          [_, kb] -> String.to_integer(kb) / 1024
          _ -> fallback_available_memory_mb()
        end

      _ ->
        fallback_available_memory_mb()
    end
  end

  def memory_is_available(threshold_mb \\ @min_available_memory_mb) do
    available_memory_mb() >= threshold_mb
  end

  def post_input_values_size(payload) do
    [:scenario, :count, :seed, :max_concurrency]
    |> Enum.map(&Map.fetch!(payload, &1))
    |> Enum.map(&byte_size(to_string(&1)))
    |> Enum.sum()
  end

  def reserve_run_concurrency(requested) do
    script = """
    local key = KEYS[1]
    local requested = tonumber(ARGV[1])
    local limit = tonumber(ARGV[2])
    local reserved = tonumber(redis.call('GET', key) or '0')
    if reserved + requested > limit then
      return {0, reserved}
    end
    redis.call('INCRBY', key, requested)
    return {1, reserved + requested}
    """

    case Redix.command(Storage.redis_name(), [
           "EVAL",
           script,
           "1",
           @active_run_concurrency_key,
           requested,
           global_max_subtask_processes()
         ]) do
      {:ok, [1, _]} -> :ok
      {:ok, [0, reserved]} -> {:error, {:capacity_exceeded, requested, reserved, global_max_subtask_processes()}}
      error -> error
    end
  end

  def release_run_concurrency(released) do
    script = """
    local key = KEYS[1]
    local released = tonumber(ARGV[1])
    local reserved = tonumber(redis.call('GET', key) or '0')
    local next_value = reserved - released
    if next_value < 0 then next_value = 0 end
    redis.call('SET', key, next_value)
    return next_value
    """

    Redix.command(Storage.redis_name(), ["EVAL", script, "1", @active_run_concurrency_key, released])
  end

  def reconcile_active_run_concurrency do
    reserved = Storage.active_run_concurrency_total()
    Redix.command(Storage.redis_name(), ["SET", @active_run_concurrency_key, reserved])
  end

  def validate_run_id(run_id) do
    uuid_pattern = ~r/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

    if Regex.match?(uuid_pattern, run_id) do
      {:ok, String.downcase(run_id)}
    else
      {:error, "run_id must be a valid UUID"}
    end
  end

  defp fallback_available_memory_mb do
    memory = :erlang.memory(:total)
    max(1024.0, memory / 1024 / 1024)
  end
end
