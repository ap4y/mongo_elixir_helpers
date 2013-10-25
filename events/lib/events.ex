defmodule Events do
  use Application.Behaviour

  @max_connections 100
  @max_processes 10

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, _args) do
    Events.Supervisor.start_link
  end

  def run(pid // self) do
    pool = :resource_pool.new(:mongo.connect_factory(:localhost), @max_connections)
    (1..@max_processes)
    |> Enum.map(fn(idx) ->
      spawn(Events.Migration, :run, [ pid, pool, {}, idx - 1 ]) 
    end)
    |> monitor_process
    :resource_pool.close(pool)
  end

  defp monitor_process(processes) do
    receive do
      { :finished, pid, time } when length(processes) > 1 ->
        print_time(time)
        monitor_process(List.delete(processes, pid))
      { :finished, _pid, time } -> print_time(time)
    end
  end

  defp print_time(time), do: :io.format("~.2f~n", [time/1000000.0])

end
