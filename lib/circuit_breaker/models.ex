defmodule CircuitBreaker.Models do
  @run_statuses ~w(queued running completed failed)
  @task_statuses ~w(pending running retrying completed failed)
  @planned_outcomes ~w(completed failed timeout)

  def run_statuses, do: @run_statuses
  def task_statuses, do: @task_statuses
  def planned_outcomes, do: @planned_outcomes

  def terminal_run_status?(status), do: status in ~w(completed failed)
  def terminal_task_status?(status), do: status in ~w(completed failed)
end
