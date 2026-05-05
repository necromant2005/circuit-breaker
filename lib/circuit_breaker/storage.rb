# frozen_string_literal: true

require "json"
require "redis"
require "securerandom"
require "time"

module CircuitBreaker
  module Storage
    TASK_QUEUE_KEY = "queue:tasks"
    RUN_QUEUE_KEY = "queue:runs"
    GLOBAL_RUNNING_KEY = "global:running_tasks"

    module_function

    def redis
      @redis ||= Redis.new(url: ENV.fetch("REDIS_URL", "redis://localhost:6379/0"))
    end

    def reset_connection!
      @redis&.close
      @redis = nil
    end

    def task_queue_key
      TASK_QUEUE_KEY
    end

    def global_running_key
      GLOBAL_RUNNING_KEY
    end

    def create_run(request, execution_variations: Main.task_execution_variations,
                   min_task_duration_seconds: Main.min_task_duration_seconds,
                   max_task_duration_seconds: Main.max_task_duration_seconds,
                   timeout_seconds: Main.max_subprocess_timeout_seconds)
      active_key = active_seed_key(request.fetch(:seed))
      duplicate = active_run_for_seed(active_key)
      return [:duplicate_active_seed, duplicate[0], duplicate[1]] if duplicate

      run_id = SecureRandom.uuid
      now = utc_now
      run = {
        "run_id" => run_id,
        "scenario" => request.fetch(:scenario),
        "count" => request.fetch(:count),
        "seed" => request.fetch(:seed),
        "max_concurrency" => request.fetch(:max_concurrency),
        "status" => "queued",
        "error" => nil,
        "message" => nil,
        "created_at" => now,
        "updated_at" => now,
        "started_at" => nil,
        "finished_at" => nil,
        "concurrency_released" => false
      }

      tasks = Planner.build_task_plan(
        run_id: run_id,
        scenario: request.fetch(:scenario),
        run_seed: request.fetch(:seed),
        count: request.fetch(:count),
        execution_variations: execution_variations,
        min_task_duration_seconds: min_task_duration_seconds,
        max_task_duration_seconds: max_task_duration_seconds,
        timeout_seconds: timeout_seconds
      ).map { |task| task.merge("created_at" => now, "updated_at" => now) }

      return [:duplicate_active_seed, *duplicate_active_seed(active_key)] unless redis.set(active_key, run_id, nx: true)

      redis.multi do |multi|
        multi.set(run_key(run_id), JSON.generate(run))
        multi.del(run_tasks_key(run_id))
        tasks.each do |task|
          multi.set(task_key(task["task_id"]), JSON.generate(task))
          multi.rpush(run_tasks_key(run_id), task["task_id"])
          multi.rpush(TASK_QUEUE_KEY, task["task_id"])
        end
        multi.rpush(RUN_QUEUE_KEY, run_id)
        multi.set(run_running_key(run_id), 0)
      end

      [:ok, run]
    rescue StandardError
      redis.del(active_key) if run_id
      raise
    end

    def active_run_concurrency_total
      scan_keys("run:*").reject { |key| key.end_with?(":tasks", ":running_tasks") }.sum do |key|
        run = get_json_by_key(key)
        run && %w[queued running].include?(run["status"]) ? run["max_concurrency"].to_i : 0
      end
    end

    def get_run(run_id)
      return nil unless run_id

      get_json_by_key(run_key(run_id))
    end

    def get_task(task_id)
      get_json_by_key(task_key(task_id))
    end

    def get_run_tasks(run_id)
      redis.lrange(run_tasks_key(run_id), 0, -1)
           .map { |id| get_task(id) }
           .compact
           .sort_by { |task| task["task_index"] }
    end

    def update_run(run)
      run = run.merge("updated_at" => utc_now)
      redis.set(run_key(run["run_id"]), JSON.generate(run))
    end

    def update_task(task)
      task = task.merge("updated_at" => utc_now)
      redis.set(task_key(task["task_id"]), JSON.generate(task))
    end

    def enqueue_task(task_id)
      redis.rpush(TASK_QUEUE_KEY, task_id)
    end

    def enqueue_retry_task(task_id)
      redis.lpush(TASK_QUEUE_KEY, task_id)
    end

    def pop_task_id(timeout = 1)
      result = redis.blpop(TASK_QUEUE_KEY, timeout: timeout)
      result&.last
    end

    def acquire_slots(run_id, max_concurrency, global_limit = Main.global_max_subtask_processes)
      script = <<~LUA
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
      LUA

      redis.eval(script, keys: [GLOBAL_RUNNING_KEY, run_running_key(run_id)], argv: [global_limit, max_concurrency]) == 1
    end

    def release_slots(run_id)
      script = <<~LUA
        local global_key = KEYS[1]
        local run_key = KEYS[2]
        local global_running = tonumber(redis.call('GET', global_key) or '0')
        local run_running = tonumber(redis.call('GET', run_key) or '0')
        if global_running > 0 then redis.call('DECR', global_key) end
        if run_running > 0 then redis.call('DECR', run_key) end
        return 1
      LUA

      redis.eval(script, keys: [GLOBAL_RUNNING_KEY, run_running_key(run_id)], argv: [])
    end

    def global_running_count
      redis.get(GLOBAL_RUNNING_KEY).to_i
    end

    def run_running_count(run_id)
      redis.get(run_running_key(run_id)).to_i
    end

    def summarize_running_tasks(run_id, global_limit = Main.global_max_subtask_processes)
      scan_keys("task:*").each_with_object({ "global_running" => 0, "run_running" => 0, "global_limit" => global_limit }) do |key, acc|
        task = get_json_by_key(key)
        next unless task && task["status"] == "running"

        acc["global_running"] += 1
        acc["run_running"] += 1 if task["run_id"] == run_id
      end
    end

    def summarize_run(run_id)
      counts = get_run_tasks(run_id).each_with_object(Hash.new(0)) { |task, acc| acc[task["status"]] += 1 }
      {
        "pending" => counts["pending"],
        "running" => counts["running"],
        "retrying" => counts["retrying"],
        "completed" => counts["completed"],
        "failed" => counts["failed"]
      }
    end

    def refresh_run_terminal_state(run_id)
      run = get_run(run_id)
      return unless run
      return if terminal_run_status?(run["status"])

      tasks = get_run_tasks(run_id)
      return if tasks.empty? || tasks.any? { |task| !terminal_task_status?(task["status"]) }

      failed = tasks.select { |task| task["status"] == "failed" }
      run = run.merge("finished_at" => utc_now)
      run = if failed.empty?
        run.merge("status" => "completed", "error" => nil, "message" => "Run completed successfully.")
      else
        plural = failed.length == 1 ? "" : "s"
        run.merge(
          "status" => "failed",
          "error" => "one_or_more_tasks_failed",
          "message" => "Run failed because #{failed.length} task#{plural} failed after retry."
        )
      end

      update_run(run)
      redis.del(active_seed_key(run["seed"]))
    end

    def mark_run_concurrency_released(run_id)
      run = get_run(run_id)
      return nil unless run && %w[completed failed].include?(run["status"]) && !run["concurrency_released"]

      update_run(run.merge("concurrency_released" => true))
      run["max_concurrency"].to_i
    end

    def mark_run_running(run_id)
      run = get_run(run_id)
      return unless run && run["status"] == "queued"

      now = utc_now
      redis.set(run_key(run_id), JSON.generate(run.merge("status" => "running", "started_at" => now, "updated_at" => now)))
    end

    def repair_running_state
      redis.set(GLOBAL_RUNNING_KEY, 0)
      scan_keys("run:*:running_tasks").each { |key| redis.set(key, 0) }
      scan_keys("task:*").each do |key|
        task = get_json_by_key(key)
        next unless task

        case task["status"]
        when "running"
          if task["attempt"].to_i < 2
            update_task(task.merge(
              "status" => "retrying",
              "error" => "unknown_error",
              "reason" => "worker_restart",
              "message" => "Task was interrupted by worker restart; retrying once."
            ))
            enqueue_retry_task(task["task_id"])
          else
            update_task(task.merge(
              "status" => "failed",
              "error" => "unknown_error",
              "reason" => "worker_restart",
              "message" => "Task failed because worker restarted during final attempt.",
              "finished_at" => utc_now
            ))
            refresh_run_terminal_state(task["run_id"])
          end
        when "retrying"
          enqueue_retry_task(task["task_id"])
        end
      end
    end

    def terminal_task_status?(status)
      %w[completed failed].include?(status)
    end

    def terminal_run_status?(status)
      %w[completed failed].include?(status)
    end

    def utc_now
      Time.now.utc.iso8601
    end

    def scan_keys(match)
      redis.scan_each(match: match).to_a
    end

    def active_run_for_seed(active_key)
      run_id = redis.get(active_key)
      return nil unless run_id

      run = get_run(run_id)
      return [run_id, run["status"]] if run && %w[queued running].include?(run["status"])

      redis.del(active_key)
      nil
    end

    def duplicate_active_seed(active_key)
      run_id = redis.get(active_key)
      run = get_run(run_id)
      [run_id || "unknown", run ? run["status"] : "queued"]
    end

    def get_json_by_key(key)
      raw = redis.get(key)
      raw && JSON.parse(raw)
    end

    def run_key(run_id)
      "run:#{run_id}"
    end

    def run_tasks_key(run_id)
      "run:#{run_id}:tasks"
    end

    def task_key(task_id)
      "task:#{task_id}"
    end

    def active_seed_key(seed)
      "seed:#{seed}:active_run"
    end

    def run_running_key(run_id)
      "run:#{run_id}:running_tasks"
    end
  end
end
