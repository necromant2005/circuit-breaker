defmodule CircuitBreaker.Application do
  use Application

  alias CircuitBreaker.{Main, Storage, Worker}

  @impl true
  def start(_type, _args) do
    redis_url = Application.fetch_env!(:circuit_breaker, :redis_url)
    port = Application.fetch_env!(:circuit_breaker, :port)
    worker_enabled = Application.fetch_env!(:circuit_breaker, :worker_enabled)

    children =
      [
        {Redix, {redis_url, [name: Storage.redis_name()]}},
        {Plug.Cowboy, scheme: :http, plug: CircuitBreaker.Router, options: [port: port]},
        {Task, fn -> Main.reconcile_active_run_concurrency() end}
      ] ++ worker_children(worker_enabled)

    Supervisor.start_link(children, strategy: :one_for_one, name: CircuitBreaker.Supervisor)
  end

  defp worker_children(true), do: [Worker]
  defp worker_children(false), do: []
end
