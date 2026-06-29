defmodule Demo.Application do
  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      # One DuckDB connection per catalog, kept alive for the process lifetime.
      Demo.Catalog
    ]

    opts = [strategy: :one_for_one, name: Demo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
