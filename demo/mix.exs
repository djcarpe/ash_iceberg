defmodule Demo.MixProject do
  use Mix.Project

  def project do
    [
      app: :demo,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Demo.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ash_iceberg, path: ".."},
      {:benchee, "~> 1.3"},
      {:jason, "~> 1.4"}
    ]
  end
end
