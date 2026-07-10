defmodule Demo do
  @moduledoc "Helpers used by demo and benchmark scripts."

  alias AshIceberg.Catalog.RestClient

  @event_types ~w[view click purchase share bookmark]

  # ── Table lifecycle ──────────────────────────────────────────────────────────

  @doc """
  Creates the `demo` namespace and the `events` table in the Iceberg REST catalog.
  Idempotent: safe to call on every startup.
  """
  def setup_table! do
    cfg = Demo.Catalog.config()

    # 1. Ensure namespace exists
    :ok = RestClient.ensure_namespace(cfg, "demo")
    IO.puts("  ✓ namespace 'demo' ready")

    # 2. Create the events table (409 = already exists → fine)
    schema = iceberg_schema()

    case RestClient.create_table(cfg, "demo", "events", schema) do
      {:ok, _} ->
        IO.puts("  ✓ table 'events' created")

      {:error, {409, _}} ->
        IO.puts("  ✓ table 'events' already exists")

      {:error, reason} ->
        raise "Failed to create table: #{inspect(reason)}"
    end

    # 3. Also ensure the table exists in DuckDB via the attached catalog
    # (CREATE TABLE IF NOT EXISTS is a no-op when the table is already there)
    ddl = """
    CREATE TABLE IF NOT EXISTS demo_catalog.demo.events (
      id          BIGINT,
      user_id     INTEGER,
      event_type  VARCHAR,
      value       DOUBLE,
      occurred_at TIMESTAMPTZ,
      metadata    VARCHAR
    )
    """

    case AshIceberg.Connection.query(Demo.Catalog, ddl) do
      {:ok, _} ->
        IO.puts("  ✓ DuckDB table reference verified")

      {:error, reason} ->
        IO.puts("  ⚠  DuckDB DDL skipped (#{inspect(reason)})")
        IO.puts("     Falling back to iceberg_scan() for all reads.")
    end

    :ok
  end

  # ── Record factory ───────────────────────────────────────────────────────────

  @doc "Build params for a random event."
  def random_event do
    %{
      # Ad-hoc creates start well above the sequential ids used by the seeder.
      id: 100_000_000_000 + :erlang.unique_integer([:positive, :monotonic]),
      user_id: :rand.uniform(10_000),
      event_type: Enum.random(@event_types),
      value: Float.round(:rand.uniform() * 500, 2),
      occurred_at: DateTime.utc_now(),
      metadata: %{"session" => Base.encode16(:crypto.strong_rand_bytes(4))}
    }
  end

  @doc "Create a single event via Ash."
  def create_one! do
    Demo.Event
    |> Ash.Changeset.for_create(:create, random_event())
    |> Ash.create!(domain: Demo.Domain)
  end

  @doc "Bulk-create `n` events in one Iceberg snapshot."
  def bulk_create!(n) do
    rows = Enum.map(1..n, fn _ -> random_event() end)

    Ash.bulk_create!(rows, Demo.Event, :create,
      domain: Demo.Domain,
      return_records?: false
    )
  end

  @doc "Read all events."
  def read_all! do
    Ash.read!(Demo.Event, domain: Demo.Domain)
  end

  @doc "Read events for a specific user."
  def read_by_user!(user_id) do
    Demo.Event
    |> Ash.Query.for_read(:by_user, %{user_id: user_id})
    |> Ash.read!(domain: Demo.Domain)
  end

  @doc "Read events of a specific type."
  def read_by_type!(type) do
    Demo.Event
    |> Ash.Query.for_read(:by_type, %{event_type: type})
    |> Ash.read!(domain: Demo.Domain)
  end

  # ── Private ──────────────────────────────────────────────────────────────────

  defp iceberg_schema do
    %{
      "type" => "struct",
      "schema-id" => 0,
      "identifier-field-ids" => [1],
      "fields" => [
        %{"id" => 1, "name" => "id", "required" => true, "type" => "long"},
        %{"id" => 2, "name" => "user_id", "required" => true, "type" => "int"},
        %{"id" => 3, "name" => "event_type", "required" => true, "type" => "string"},
        %{"id" => 4, "name" => "value", "required" => false, "type" => "double"},
        %{"id" => 5, "name" => "occurred_at", "required" => false, "type" => "timestamptz"},
        %{"id" => 6, "name" => "metadata", "required" => false, "type" => "string"}
      ]
    }
  end
end
