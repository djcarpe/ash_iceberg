defmodule Demo.Application do
  use Application

  @impl Application
  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT", "4000"))

    children = [
      # One DuckDB connection per catalog, kept alive for the process lifetime.
      Demo.Catalog,
      {Bandit, plug: Demo.Router, port: port},
      # Ensure the namespace/table exist in the REST catalog (retries until
      # the catalog is reachable, then exits).
      {Task, &ensure_table/0}
    ]

    opts = [strategy: :one_for_one, name: Demo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp ensure_table(attempt \\ 1) do
    Demo.setup_table!()
    IO.puts("Iceberg table ready.")
  rescue
    e ->
      if attempt >= 60 do
        IO.puts("Giving up on table setup: #{Exception.message(e)}")
      else
        IO.puts("Catalog not ready (attempt #{attempt}), retrying in 5s...")
        Process.sleep(5_000)
        ensure_table(attempt + 1)
      end
  end
end
