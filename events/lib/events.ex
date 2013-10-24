defmodule Events do
  use Application.Behaviour

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, _args) do
    Events.Supervisor.start_link
  end

  def run do

  end

end
