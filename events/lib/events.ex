defmodule Events do
  use Application.Behaviour

  @max_connections 100
  @max_processes 1
  @host :"192.168.178.30"

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, _args) do
    Events.Supervisor.start_link
  end

  def main(host // @host, node // :"one@192.168.178.30") do
    pool = :resource_pool.new(:mongo.connect_factory(host), @max_connections)

    # Node.connect(node)
    # processes = Node.list
    # |> Enum.reduce([], fn(node, acc) ->
    #   acc ++ start_node(node, pool)
    # end)

    # processes
    # |> monitor_process

    start_processes(pool)
    |> monitor_process

    :resource_pool.close(pool)
  end

  # defp start_node(node, pool) do
  #   (1..@max_processes)
  #   |> Enum.map(fn(idx) ->
  #     Node.spawn(node, Events.Migration, :run, [ self, pool, {}, idx - 1 ])
  #   end)
  # end

  defp start_processes(pool) do
    (1..@max_processes)
    |> Enum.map(fn(idx) ->
      spawn(Events.Migration, :run, [ self, pool, {}, idx - 1 ])
    end)
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
