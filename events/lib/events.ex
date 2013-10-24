defmodule Events do
  use Application.Behaviour

  @max_connections 10

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, _args) do
    Events.Supervisor.start_link
  end

  def run do
    pool = :resource_pool.new(:mongo.connect_factory(:localhost), @max_connections)
  end

end
