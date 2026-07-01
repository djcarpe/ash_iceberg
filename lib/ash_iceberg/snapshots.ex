defmodule AshIceberg.Snapshots do
  @moduledoc """
  Helpers for inspecting the Iceberg snapshot history of a resource's table.

  Iceberg stores every write as an immutable *snapshot*.  Reading snapshot
  history lets you audit what changed, pick a point-in-time for time travel,
  or drive incremental-read pipelines.

  ## Examples

      # List all snapshots for a resource
      AshIceberg.Snapshots.list(MyApp.Events)

      # Get the current (latest) snapshot
      AshIceberg.Snapshots.current(MyApp.Events)

      # Read data as it existed in a past snapshot
      MyApp.Events
      |> Ash.Query.set_context(%{ash_iceberg: %{snapshot_id: 1_234_567_890}})
      |> Ash.read!()

      # Read data as of a specific wall-clock time
      MyApp.Events
      |> Ash.Query.set_context(%{ash_iceberg: %{as_of: ~U[2025-01-01 00:00:00Z]}})
      |> Ash.read!()

  ## Snapshot fields

  Each snapshot map returned by `list/1` contains:

  | Key | Type | Description |
  |-----|------|-------------|
  | `"snapshot-id"` | integer | Unique snapshot identifier |
  | `"timestamp-ms"` | integer | Creation time (Unix milliseconds) |
  | `"timestamp"` | DateTime | Parsed creation time (added by this module) |
  | `"operation"` | string | `"append"`, `"overwrite"`, `"replace"`, `"delete"` |
  | `"summary"` | map | Catalog-provided metadata (row counts, file counts, etc.) |
  | `"manifest-list"` | string | Location of the manifest list file |
  """

  alias AshIceberg.{Catalog.RestClient, Connection, DataLayer.Info}

  @doc """
  List all snapshots for the Iceberg table backing `resource`.

  Prefers the REST catalog when one is configured; falls back to querying
  DuckDB's `iceberg_snapshots()` table function for warehouse-mode resources.

  Returns snapshots sorted oldest-first with an extra `"timestamp"` key
  containing the parsed `DateTime`.
  """
  @spec list(Ash.Resource.t()) :: {:ok, [map()]} | {:error, term()}
  def list(resource) do
    catalog = Info.catalog(resource)
    namespace = Info.namespace(resource)
    table = Info.table(resource)
    warehouse = Info.warehouse(resource)

    cond do
      catalog != nil ->
        cfg = catalog.config()
        case RestClient.list_snapshots(cfg, namespace, table) do
          {:ok, snaps} -> {:ok, Enum.map(snaps, &enrich/1)}
          err -> err
        end

      is_binary(warehouse) ->
        list_via_duckdb(resource, warehouse, namespace, table)

      true ->
        {:error, "Resource has no catalog or warehouse configured"}
    end
  end

  @doc """
  Returns the current (latest) snapshot, or `nil` when no snapshots exist.
  """
  @spec current(Ash.Resource.t()) :: {:ok, map() | nil} | {:error, term()}
  def current(resource) do
    case list(resource) do
      {:ok, []} -> {:ok, nil}
      {:ok, snaps} -> {:ok, List.last(snaps)}
      err -> err
    end
  end

  @doc """
  Returns the snapshot immediately preceding `snapshot_id`, or `nil` when there
  is no earlier snapshot.  Useful for computing incremental diffs.
  """
  @spec previous(Ash.Resource.t(), non_neg_integer()) :: {:ok, map() | nil} | {:error, term()}
  def previous(resource, snapshot_id) do
    case list(resource) do
      {:ok, snaps} ->
        result =
          snaps
          |> Enum.reverse()
          |> Enum.drop_while(&(&1["snapshot-id"] != snapshot_id))
          |> Enum.at(1)

        {:ok, result}

      err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp list_via_duckdb(resource, warehouse, namespace, table) do
    path =
      warehouse
      |> String.trim_trailing("/")
      |> then(&"#{&1}/#{namespace}/#{table}")

    sql = "SELECT * FROM iceberg_snapshots('#{path}') ORDER BY committed_at ASC"

    server =
      case Info.catalog(resource) do
        nil -> AshIceberg.Connection
        mod -> mod
      end

    case Connection.query(server, sql) do
      {:ok, rows} ->
        snaps =
          Enum.map(rows, fn row ->
            %{
              "snapshot-id" => row["snapshot_id"],
              "timestamp-ms" => row["committed_at"],
              "operation" => row["operation"],
              "summary" => %{},
              "manifest-list" => row["manifest_list"],
              "timestamp" => ms_to_datetime(row["committed_at"])
            }
          end)

        {:ok, snaps}

      {:error, _} = err ->
        err
    end
  end

  defp enrich(%{"timestamp-ms" => ms} = snap) do
    Map.put(snap, "timestamp", ms_to_datetime(ms))
  end

  defp enrich(snap), do: snap

  defp ms_to_datetime(nil), do: nil

  defp ms_to_datetime(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, dt} -> dt
      _ -> nil
    end
  end

  defp ms_to_datetime(_), do: nil
end
