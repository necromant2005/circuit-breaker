# frozen_string_literal: true

require "digest"

module CircuitBreaker
  module Planner
    module_function

    def derive_child_seed(run_seed, task_index)
      stable_int("#{run_seed}:#{task_index}")
    end

    def stable_int(parts)
      payload = parts.is_a?(Array) ? parts.join(":") : parts.to_s
      Digest::SHA256.hexdigest(payload)[0, 8].to_i(16)
    end

    def planned_outcomes_for_task(task_index, variations = Main.task_execution_variations)
      variations[task_index % variations.length]
    end

    def build_task_plan(run_id:, scenario:, run_seed:, count:, execution_variations: Main.task_execution_variations,
                        min_task_duration_seconds: Main.min_task_duration_seconds,
                        max_task_duration_seconds: Main.max_task_duration_seconds,
                        timeout_seconds: Main.max_subprocess_timeout_seconds)
      count.times.map do |index|
        child_seed = derive_child_seed(run_seed, index)
        case_name, outcomes = planned_outcomes_for_task(index, execution_variations)
        first_outcome = outcomes["attempt_1"]
        retry_outcome = outcomes["attempt_2"]

        duration = planned_attempt_duration(
          child_seed, index, 1, first_outcome,
          min_task_duration_seconds: min_task_duration_seconds,
          max_task_duration_seconds: max_task_duration_seconds,
          timeout_seconds: timeout_seconds
        )

        retry_duration = retry_outcome && planned_attempt_duration(
          child_seed, index, 2, retry_outcome,
          min_task_duration_seconds: min_task_duration_seconds,
          max_task_duration_seconds: max_task_duration_seconds,
          timeout_seconds: timeout_seconds
        )

        result = if first_outcome == "completed" || retry_outcome == "completed"
          {
            "scenario" => scenario,
            "task_index" => index,
            "child_seed" => child_seed,
            "value" => 1000 + (stable_int([child_seed, index, "result"]) % 9000)
          }
        end

        {
          "task_id" => "#{run_id}:#{index}",
          "run_id" => run_id,
          "task_index" => index,
          "child_seed" => child_seed,
          "status" => "pending",
          "attempt" => 0,
          "duration" => duration.to_f,
          "retry_duration" => retry_duration&.to_f,
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

    def planned_attempt_duration(child_seed, task_index, attempt, outcome, min_task_duration_seconds:, max_task_duration_seconds:, timeout_seconds:)
      return timeout_seconds if outcome == "timeout"

      duration_range = max_task_duration_seconds - min_task_duration_seconds + 1
      offset = stable_int([child_seed, task_index, attempt, "duration"]) % duration_range
      min_task_duration_seconds + offset
    end
  end
end
