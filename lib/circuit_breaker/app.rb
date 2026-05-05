# frozen_string_literal: true

require "json"
require "rack"
require_relative "main"
require_relative "planner"
require_relative "storage"
require_relative "worker"

module CircuitBreaker
  class App
    def call(env)
      request = Rack::Request.new(env)

      case
      when request.post? && request.path_info == "/runs"
        create_run(request)
      when request.get? && (match = request.path_info.match(%r{\A/runs/([^/]+)\z}))
        get_run(match[1])
      when request.get? && (match = request.path_info.match(%r{\A/runs/([^/]+)/tasks\z}))
        get_tasks(match[1])
      else
        json(404, "detail" => "Not Found")
      end
    rescue JSON::ParserError
      json(422, "detail" => "request body must be valid JSON")
    end

    private

    def create_run(request)
      validation = validate_create_run(JSON.parse(request.body.read))
      return json(422, "detail" => validation[:detail]) unless validation[:ok]

      payload = validation[:payload]
      size = Main.post_input_values_size(payload)
      limit = Main.max_task_record_size_bytes
      if size > limit
        return json(413, {
          "error" => "post_input_too_large",
          "message" => "POST /runs input values are #{size} bytes, which exceeds the #{limit} byte limit.",
          "size" => size,
          "limit" => limit
        })
      end

      if payload[:max_concurrency] > Main.global_max_subtask_processes
        return json(422, "detail" => "max_concurrency must be less than or equal to #{Main.global_max_subtask_processes}")
      end

      unless Main.memory_is_available(Main.min_available_memory_mb)
        return json(503, {
          "error" => "insufficient_memory",
          "message" => "Run was not queued because available memory is below #{Main.min_available_memory_mb} MB.",
          "min_available_memory_mb" => Main.min_available_memory_mb
        })
      end

      reservation = Main.reserve_run_concurrency(payload[:max_concurrency])
      if reservation.is_a?(Array) && reservation.first == :capacity_exceeded
        _kind, requested, reserved, global_limit = reservation
        return json(409, {
          "error" => "global_concurrency_capacity_exceeded",
          "message" => "Cannot reserve #{requested} concurrency slots because #{reserved} of #{global_limit} are already reserved by active runs.",
          "requested" => requested,
          "reserved" => reserved,
          "limit" => global_limit
        })
      end

      result = Storage.create_run(payload)
      if result.first == :ok
        run = result[1]
        json(200, "run_id" => run["run_id"], "status" => run["status"])
      elsif result.first == :duplicate_active_seed
        Main.release_run_concurrency(payload[:max_concurrency])
        _kind, run_id, status = result
        json(409, {
          "error" => "duplicate_active_seed",
          "message" => "A run with seed #{payload[:seed]} is already queued or running.",
          "existing_run_id" => run_id,
          "existing_status" => status
        })
      else
        Main.release_run_concurrency(payload[:max_concurrency])
        json(500, "detail" => "failed to create run")
      end
    end

    def get_run(raw_run_id)
      run_id = Main.validate_run_id(raw_run_id)
      return json(422, "detail" => "run_id must be a valid UUID") unless run_id

      run = Storage.get_run(run_id)
      return json(404, "detail" => "Run not found") unless run

      json(200, {
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
    end

    def get_tasks(raw_run_id)
      run_id = Main.validate_run_id(raw_run_id)
      return json(422, "detail" => "run_id must be a valid UUID") unless run_id

      return json(404, "detail" => "Run not found") unless Storage.get_run(run_id)

      json(200, "tasks" => Storage.get_run_tasks(run_id))
    end

    def validate_create_run(params)
      required = %w[scenario count seed max_concurrency]
      missing = required - params.keys
      extra = params.keys - required

      return { ok: false, detail: missing.map { |field| "#{field} is required" } } unless missing.empty?
      return { ok: false, detail: extra.map { |field| "#{field} is not allowed" } } unless extra.empty?
      return { ok: false, detail: "scenario must be a string" } unless params["scenario"].is_a?(String)
      return { ok: false, detail: "scenario must not be blank" } if params["scenario"].strip.empty?
      return { ok: false, detail: "count must be an integer greater than or equal to 1" } unless params["count"].is_a?(Integer) && params["count"] >= 1
      return { ok: false, detail: "seed must be an integer" } unless params["seed"].is_a?(Integer)
      unless params["max_concurrency"].is_a?(Integer) && params["max_concurrency"] >= 1
        return { ok: false, detail: "max_concurrency must be an integer greater than or equal to 1" }
      end

      {
        ok: true,
        payload: {
          scenario: params["scenario"].strip,
          count: params["count"],
          seed: params["seed"],
          max_concurrency: params["max_concurrency"]
        }
      }
    end

    def json(status, body)
      [status, { "content-type" => "application/json" }, [JSON.generate(body)]]
    end
  end
end
