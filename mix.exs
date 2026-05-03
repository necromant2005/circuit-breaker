defmodule CircuitBreaker.MixProject do
  use Mix.Project

  def project do
    [
      app: :circuit_breaker,
      version: "0.1.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {CircuitBreaker.Application, []}
    ]
  end

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:plug_cowboy, "~> 2.7"},
      {:redix, "~> 1.2"}
    ]
  end
end
