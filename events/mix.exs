defmodule Events.Mixfile do
  use Mix.Project

  def project do
    [ app: :events,
      version: "0.0.1",
      elixir: "~> 0.10.3",
      deps: deps(Mix.env) ]
  end

  # Configuration for the OTP application
  def application do
    [
      applications: [ :mongodb ],
      mod: { Events, [] }
    ]
  end

  # Returns the list of dependencies in the format:
  # { :foobar, "~> 0.1", git: "https://github.com/elixir-lang/foobar.git" }
  defp deps(:dev) do
    [
      { :mongodb, github: "mururu/mongodb-erlang" }
    ]
  end

  defp deps(:test) do
    deps(:dev) ++
    [
      { :meck, github: "eproxus/meck" }
    ]
  end
end
