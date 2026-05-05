# frozen_string_literal: true

module CircuitBreaker
  class TaskTimeout < StandardError; end

  module Worker
    module_function

    def run_forever
      Storage.repair_running_state
      release_finished_run_reservations
      loop do
        process_once
      end
    end

    def process_once(poll_timeout = 1, async: false)
      task_id = Storage.pop_task_id(poll_timeout)
      return false unless task_id

      task = Storage.get_task(task_id)
      return false if task.nil? || %w[running completed failed].include?(task["status"])

      run = Storage.get_run(task["run_id"])
      if run && can_start_task?(run, task)
        if async
          Thread.new { execute_task(run, task) }
        else
          execute_task(run, task)
        end
        true
      else
        refreshed = Storage.get_task(task_id)
        Storage.enqueue_task(task_id) if refreshed && !%w[completed failed].include?(refreshed["status"])
        sleep 0.2
        false
      end
    end

    def run_subprocess_simulation(task)
      outcome = outcome_for_attempt(task)
      duration = if task["attempt"].to_i > 1 && !task["retry_duration"].nil?
        task["retry_duration"].to_f
      else
        task["duration"].to_f
      end
      scale = Main.task_duration_scale
      timeout = Main.max_subprocess_timeout_seconds

      if outcome == "timeout"
        sleep_seconds([duration, timeout].max * scale)
        raise TaskTimeout, "subprocess reached #{timeout} second kill timeout"
      elsif duration * scale >= timeout
        sleep_seconds(timeout)
        raise TaskTimeout, "subprocess reached #{timeout} second kill timeout"
      else
        sleep_seconds(duration * scale)
      end
    end

    def delay_before_retry(failed_count)
      delay = Main.retry_delay_seconds(failed_count) * Main.task_duration_scale
      sleep_seconds(delay) if delay.positive?
    end

    def can_start_task?(run, task)
      summary = Storage.summarize_running_tasks(run["run_id"])

      if summary["global_running"] >= summary["global_limit"]
        Storage.update_task(task.merge(
          "blocked_reason" => "global_concurrency_limit",
          "message" => "Task is waiting because the global subtask process limit of #{summary["global_limit"]} running processes has been reached."
        ))
        return false
      end

      if summary["run_running"] >= run["max_concurrency"].to_i
        Storage.update_task(task.merge(
          "blocked_reason" => "run_concurrency_limit",
          "message" => "Task is waiting because the run concurrency limit of #{run["max_concurrency"]} running tasks has been reached."
        ))
        return false
      end

      unless Storage.acquire_slots(run["run_id"], run["max_concurrency"].to_i)
        Storage.update_task(task.merge(
          "blocked_reason" => "global_concurrency_limit",
          "message" => "Task is waiting for an available execution slot."
        ))
        return false
      end

      unless Main.memory_is_available(Main.min_available_memory_mb)
        Storage.release_slots(run["run_id"])
        Storage.update_task(task.merge(
          "blocked_reason" => "insufficient_memory",
          "message" => "Task is waiting because available memory is below #{Main.min_available_memory_mb} MB."
        ))
        return false
      end

      true
    end

    def execute_task(run, task)
      attempt = task["attempt"].to_i + 1
      task = task.merge(
        "blocked_reason" => nil,
        "status" => "running",
        "attempt" => attempt,
        "started_at" => task["started_at"] || Storage.utc_now,
        "message" => "Task attempt #{attempt} is running."
      )
      Storage.update_task(task)
      Storage.mark_run_running(run["run_id"])

      begin
        run_subprocess_simulation(task)
        handle_planned_outcome(task)
      rescue TaskTimeout
        handle_timeout(task)
      rescue StandardError => e
        handle_unknown(task, e)
      ensure
        Storage.release_slots(run["run_id"])
        Storage.refresh_run_terminal_state(run["run_id"])
        release_run_reservation_if_terminal(run["run_id"])
      end
    end

    def handle_planned_outcome(task)
      outcome = outcome_for_attempt(task)
      if outcome == "completed"
        Storage.update_task(task.merge(
          "status" => "completed",
          "result" => task["planned_result"],
          "error" => nil,
          "message" => "Task completed successfully.",
          "finished_at" => Storage.utc_now
        ))
      else
        task = task.merge("error" => error_for_outcome(outcome), "reason" => outcome)
        if can_retry?(task)
          Storage.update_task(task.merge("status" => "retrying", "message" => "Task attempt #{task["attempt"]} ended with #{outcome}; retrying."))
          delay_before_retry(task["attempt"].to_i)
          Storage.enqueue_retry_task(task["task_id"])
        else
          Storage.update_task(task.merge(
            "status" => "failed",
            "message" => "Task failed after #{task["attempt"]} attempts. Last reason: #{outcome}.",
            "finished_at" => Storage.utc_now
          ))
        end
      end
    end

    def handle_timeout(task)
      task = task.merge("error" => "task_timeout", "reason" => "timeout")
      if can_retry?(task)
        Storage.update_task(task.merge(
          "status" => "retrying",
          "message" => "Task attempt #{task["attempt"]} reached the #{Main.max_subprocess_timeout_seconds} second kill timeout; retrying."
        ))
        delay_before_retry(task["attempt"].to_i)
        Storage.enqueue_retry_task(task["task_id"])
      else
        Storage.update_task(task.merge(
          "status" => "failed",
          "message" => "Task failed because the subprocess reached the #{Main.max_subprocess_timeout_seconds} second kill timeout twice.",
          "finished_at" => Storage.utc_now
        ))
      end
    end

    def handle_unknown(task, error)
      task = task.merge(
        "error" => "unknown_error",
        "reason" => error.class.name,
        "message" => "Task failed with unexpected error: #{error.message}"
      )

      if can_retry?(task)
        Storage.update_task(task.merge("status" => "retrying"))
        delay_before_retry(task["attempt"].to_i)
        Storage.enqueue_retry_task(task["task_id"])
      else
        Storage.update_task(task.merge("status" => "failed", "finished_at" => Storage.utc_now))
      end
    end

    def can_retry?(task)
      task["attempt"].to_i <= Main.max_task_retries
    end

    def outcome_for_attempt(task)
      task["attempt"].to_i == 1 ? task["planned_first_attempt_outcome"] : (task["planned_retry_attempt_outcome"] || task["planned_first_attempt_outcome"])
    end

    def error_for_outcome(outcome)
      { "failed" => "task_failed", "timeout" => "task_timeout" }.fetch(outcome, "unknown_error")
    end

    def release_run_reservation_if_terminal(run_id)
      amount = Storage.mark_run_concurrency_released(run_id)
      Main.release_run_concurrency(amount) if amount
    end

    def release_finished_run_reservations
      Storage.scan_keys("run:*").reject { |key| key.end_with?(":tasks", ":running_tasks") }.each do |key|
        release_run_reservation_if_terminal(key.delete_prefix("run:"))
      end
    end

    def sleep_seconds(seconds)
      sleep(seconds)
    end
  end
end
