# frozen_string_literal: true

require "json"

module CircuitBreaker
  module Main
    GLOBAL_MAX_SUBTASK_PROCESSES = 1000
    ACTIVE_RUN_CONCURRENCY_KEY = "active_runs:reserved_concurrency"
    MAX_TASK_RECORD_SIZE_BYTES = 100_000
    MIN_AVAILABLE_MEMORY_MB = 256
    MIN_TASK_DURATION_SECONDS = 2
    MAX_TASK_DURATION_SECONDS = 10
    MAX_SUBPROCESS_TIMEOUT_SECONDS = 11
    MAX_TASK_RETRIES = 1
    RETRY_DELAY_SECONDS = 1

    # task_index % TASK_EXECUTION_VARIATIONS.length selects one plan in order.
    TASK_EXECUTION_VARIATIONS = [
      ["success_first_attempt", { "attempt_1" => "completed", "attempt_2" => nil }],
      ["failed_then_success", { "attempt_1" => "failed", "attempt_2" => "completed" }],
      ["timeout_then_success", { "attempt_1" => "timeout", "attempt_2" => "completed" }],
      ["failed_then_failed", { "attempt_1" => "failed", "attempt_2" => "failed" }],
      ["failed_then_timeout", { "attempt_1" => "failed", "attempt_2" => "timeout" }],
      ["timeout_then_failed", { "attempt_1" => "timeout", "attempt_2" => "failed" }],
      ["timeout_then_timeout", { "attempt_1" => "timeout", "attempt_2" => "timeout" }]
    ].freeze

    class << self
      attr_writer :global_max_subtask_processes, :max_task_record_size_bytes,
                  :min_available_memory_mb, :max_subprocess_timeout_seconds,
                  :max_task_retries, :retry_delay_seconds, :task_duration_scale

      def global_max_subtask_processes
        @global_max_subtask_processes || GLOBAL_MAX_SUBTASK_PROCESSES
      end

      def active_run_concurrency_key
        ACTIVE_RUN_CONCURRENCY_KEY
      end

      def max_task_record_size_bytes
        @max_task_record_size_bytes || MAX_TASK_RECORD_SIZE_BYTES
      end

      def min_available_memory_mb
        @min_available_memory_mb || MIN_AVAILABLE_MEMORY_MB
      end

      def min_task_duration_seconds
        MIN_TASK_DURATION_SECONDS
      end

      def max_task_duration_seconds
        MAX_TASK_DURATION_SECONDS
      end

      def max_subprocess_timeout_seconds
        @max_subprocess_timeout_seconds || MAX_SUBPROCESS_TIMEOUT_SECONDS
      end

      def max_task_retries
        @max_task_retries || MAX_TASK_RETRIES
      end

      def retry_delay_seconds(failed_count = nil)
        base = @retry_delay_seconds || RETRY_DELAY_SECONDS
        failed_count ? base * failed_count : base
      end

      def task_duration_scale
        @task_duration_scale || ENV.fetch("TASK_DURATION_SCALE", "1.0").to_f
      end

      def task_execution_variations
        TASK_EXECUTION_VARIATIONS
      end

      def reset_config!
        @global_max_subtask_processes = nil
        @max_task_record_size_bytes = nil
        @min_available_memory_mb = nil
        @max_subprocess_timeout_seconds = nil
        @max_task_retries = nil
        @retry_delay_seconds = nil
        @task_duration_scale = nil
      end

      def available_memory_mb
        if File.exist?("/proc/meminfo")
          available = File.read("/proc/meminfo").match(/^MemAvailable:\s+(\d+)\s+kB/)
          return available[1].to_i / 1024.0 if available
        end

        1024.0
      end

      def memory_is_available(threshold_mb = min_available_memory_mb)
        available_memory_mb >= threshold_mb
      end

      def post_input_values_size(payload)
        %i[scenario count seed max_concurrency].sum { |key| payload.fetch(key).to_s.bytesize }
      end

      def reserve_run_concurrency(requested)
        script = <<~LUA
          local key = KEYS[1]
          local requested = tonumber(ARGV[1])
          local limit = tonumber(ARGV[2])
          local reserved = tonumber(redis.call('GET', key) or '0')
          if reserved + requested > limit then
            return {0, reserved}
          end
          redis.call('INCRBY', key, requested)
          return {1, reserved + requested}
        LUA

        result = Storage.redis.eval(
          script,
          keys: [ACTIVE_RUN_CONCURRENCY_KEY],
          argv: [requested, global_max_subtask_processes]
        )

        return :ok if result[0] == 1

        [:capacity_exceeded, requested, result[1], global_max_subtask_processes]
      end

      def release_run_concurrency(released)
        script = <<~LUA
          local key = KEYS[1]
          local released = tonumber(ARGV[1])
          local reserved = tonumber(redis.call('GET', key) or '0')
          local next_value = reserved - released
          if next_value < 0 then next_value = 0 end
          redis.call('SET', key, next_value)
          return next_value
        LUA

        Storage.redis.eval(script, keys: [ACTIVE_RUN_CONCURRENCY_KEY], argv: [released])
      end

      def reconcile_active_run_concurrency
        Storage.redis.set(ACTIVE_RUN_CONCURRENCY_KEY, Storage.active_run_concurrency_total)
      end

      def validate_run_id(run_id)
        return run_id.downcase if run_id.match?(/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/)

        nil
      end
    end
  end
end
