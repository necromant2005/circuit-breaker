# frozen_string_literal: true

ENV["REDIS_URL"] ||= "redis://localhost:6379/1"
ENV["TASK_DURATION_SCALE"] ||= "0.0"

require "minitest/autorun"
require "rack/mock"
require_relative "../lib/circuit_breaker"

module TestHelpers
  def setup
    CircuitBreaker::Storage.redis.flushdb
    CircuitBreaker::Main.reset_config!
    CircuitBreaker::Main.task_duration_scale = 0.0
  end

  def app
    @app ||= CircuitBreaker::App.new
  end

  def post_json(path, payload)
    Rack::MockRequest.new(app).post(path, "CONTENT_TYPE" => "application/json", input: JSON.generate(payload))
  end

  def get(path)
    Rack::MockRequest.new(app).get(path)
  end

  def json(response)
    JSON.parse(response.body)
  end

  def create_run(seed:, count:, max_concurrency:, scenario: "demo")
    CircuitBreaker::Storage.create_run({
      scenario: scenario,
      count: count,
      seed: seed,
      max_concurrency: max_concurrency
    })
  end
end
