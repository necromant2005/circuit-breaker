import Config

parse_float = fn value ->
  case Float.parse(value) do
    {number, ""} -> number
    _ -> raise ArgumentError, "expected a numeric float string, got: #{inspect(value)}"
  end
end

config :circuit_breaker,
  redis_url: System.get_env("REDIS_URL", "redis://localhost:6379/0"),
  port: String.to_integer(System.get_env("PORT", "8000")),
  worker_enabled: System.get_env("WORKER_ENABLED", "false") in ["1", "true", "yes"],
  task_duration_scale: parse_float.(System.get_env("TASK_DURATION_SCALE", "1.0"))
