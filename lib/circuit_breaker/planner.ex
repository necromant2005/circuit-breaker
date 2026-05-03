defmodule CircuitBreaker.Planner do
  alias CircuitBreaker.Main

  def derive_child_seed(run_seed, task_index) do
    stable_hex("#{run_seed}:#{task_index}")
  end

  def stable_int(parts) when is_list(parts) do
    parts |> Enum.map_join(":", &to_string/1) |> stable_hex()
  end

  def stable_int(parts), do: stable_int(Tuple.to_list(parts))

  def planned_outcomes_for_task(task_index, variations \\ Main.task_execution_variations()) do
    Enum.at(variations, rem(task_index, length(variations)))
  end

  def build_task_plan(opts) do
    run_id = Keyword.fetch!(opts, :run_id)
    scenario = Keyword.fetch!(opts, :scenario)
    run_seed = Keyword.fetch!(opts, :run_seed)
    count = Keyword.fetch!(opts, :count)
    variations = Keyword.get(opts, :execution_variations, Main.task_execution_variations())
    min_duration = Keyword.get(opts, :min_task_duration_seconds, Main.min_task_duration_seconds())
    max_duration = Keyword.get(opts, :max_task_duration_seconds, Main.max_task_duration_seconds())
    timeout = Keyword.get(opts, :timeout_seconds, Main.max_subprocess_timeout_seconds())

    for index <- 0..(count - 1) do
      child_seed = derive_child_seed(run_seed, index)
      {case_name, %{"attempt_1" => first_outcome, "attempt_2" => retry_outcome}} =
        planned_outcomes_for_task(index, variations)

      duration =
        planned_attempt_duration(child_seed, index, 1, first_outcome,
          min_task_duration_seconds: min_duration,
          max_task_duration_seconds: max_duration,
          timeout_seconds: timeout
        )

      retry_duration =
        if retry_outcome do
          planned_attempt_duration(child_seed, index, 2, retry_outcome,
            min_task_duration_seconds: min_duration,
            max_task_duration_seconds: max_duration,
            timeout_seconds: timeout
          )
        end

      eventual_success = first_outcome == "completed" or retry_outcome == "completed"

      result =
        if eventual_success do
          %{
            "scenario" => scenario,
            "task_index" => index,
            "child_seed" => child_seed,
            "value" => planned_result_value(child_seed, index)
          }
        end

      %{
        "task_id" => "#{run_id}:#{index}",
        "run_id" => run_id,
        "task_index" => index,
        "child_seed" => child_seed,
        "status" => "pending",
        "attempt" => 0,
        "duration" => duration * 1.0,
        "retry_duration" => if(retry_duration, do: retry_duration * 1.0),
        "planned_execution_case" => case_name,
        "planned_first_attempt_outcome" => first_outcome,
        "planned_retry_attempt_outcome" => retry_outcome,
        "planned_result" => result,
        "result" => nil,
        "error" => nil,
        "reason" => nil,
        "blocked_reason" => nil,
        "message" => "Task is waiting to be scheduled.",
        "created_at" => nil,
        "started_at" => nil,
        "updated_at" => nil,
        "finished_at" => nil
      }
    end
  end

  defp planned_attempt_duration(_child_seed, _task_index, _attempt, "timeout", opts) do
    Keyword.fetch!(opts, :timeout_seconds)
  end

  defp planned_attempt_duration(child_seed, task_index, attempt, _outcome, opts) do
    min_duration = Keyword.fetch!(opts, :min_task_duration_seconds)
    max_duration = Keyword.fetch!(opts, :max_task_duration_seconds)
    duration_range = max_duration - min_duration + 1
    offset = rem(stable_int([child_seed, task_index, attempt, "duration"]), duration_range)
    min_duration + offset
  end

  defp planned_result_value(child_seed, task_index) do
    1000 + rem(stable_int([child_seed, task_index, "result"]), 9000)
  end

  defp stable_hex(payload) do
    :crypto.hash(:sha256, payload)
    |> Base.encode16(case: :lower)
    |> binary_part(0, 8)
    |> String.to_integer(16)
  end
end
