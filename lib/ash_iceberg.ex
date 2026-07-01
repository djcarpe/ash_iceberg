defmodule AshIceberg do
  @moduledoc """
  Apache Iceberg data layer for Ash Framework.

  AshIceberg lets Ash resources be backed by Apache Iceberg tables.
  Queries are executed through DuckDB (with the `iceberg` extension),
  and table metadata is managed through an Iceberg REST Catalog or the
  local filesystem.

  ## Getting Started

  Add `:ash_iceberg` to your `mix.exs` dependencies, then define a
  catalog module and annotate each resource.

  ### Catalog Configuration

      defmodule MyApp.IcebergCatalog do
        use AshIceberg.Catalog, otp_app: :my_app
      end

      # config/config.exs
      config :my_app, MyApp.IcebergCatalog,
        type: :rest,
        uri: "http://localhost:8181",
        token: "my-bearer-token",
        warehouse: "s3://my-bucket/warehouse",
        aws_region: "us-east-1",
        aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY")

  ### Resource Definition

      defmodule MyApp.Events do
        use Ash.Resource, data_layer: AshIceberg.DataLayer

        iceberg do
          catalog MyApp.IcebergCatalog
          namespace "analytics"
          table "events"
        end

        attributes do
          uuid_primary_key :id
          attribute :user_id, :integer, allow_nil?: false
          attribute :event_type, :string
          attribute :occurred_at, AshIceberg.Types.TimestampTz
          attribute :payload, :map
        end

        actions do
          defaults [:create, :read]
        end
      end

  ### Filesystem (no catalog server)

  For local development or simple file-based usage, omit the catalog
  module and supply a `warehouse` path directly:

      iceberg do
        warehouse "/local/path/to/warehouse"
        namespace "analytics"
        table "events"
      end

  ## Architecture

  - **Query execution** — DuckDB with the `iceberg` extension runs all
    SELECT, INSERT, UPDATE, and DELETE statements against the table
    pointed to by the catalog or the warehouse path.
  - **Catalog** — The optional `AshIceberg.Catalog` behaviour wraps the
    Iceberg REST Catalog API (`/v1/…`) to load table metadata and commit
    new snapshots.
  - **Connections** — `AshIceberg.Connection` is a GenServer that owns
    one DuckDB in-memory database per catalog module, with the `iceberg`
    (and optionally `httpfs`) extension loaded and the catalog attached.

  ## Iceberg Write Support

  Iceberg supports row-level deletes and updates via V2 positional delete
  files. DuckDB 1.x exposes `INSERT`, `UPDATE`, and `DELETE` statements
  against Iceberg tables when the underlying catalog supports it.
  Resources that only need append semantics can set `can_update? false`
  and `can_destroy? false` in their data layer configuration.
  """

  def version, do: Mix.Project.config()[:version]

  @doc """
  List Iceberg snapshots for the table backing `resource`.

  Delegates to `AshIceberg.Snapshots.list/1`.

      AshIceberg.snapshots(MyApp.Events)
      #=> {:ok, [%{"snapshot-id" => 123, "timestamp" => ~U[...], ...}, ...]}
  """
  defdelegate snapshots(resource), to: AshIceberg.Snapshots, as: :list

  @doc """
  Return the current (latest) snapshot for `resource`'s table.

  Delegates to `AshIceberg.Snapshots.current/1`.
  """
  defdelegate current_snapshot(resource), to: AshIceberg.Snapshots, as: :current

  @doc """
  Compact the snapshot history for `resource`'s table.

  Every `bulk_create` or `create` commits one Iceberg snapshot. After many
  writes, DuckDB must open metadata for hundreds of snapshot files before
  it can plan a query. Compaction expires old snapshots via the REST catalog,
  dramatically reducing read latency.

  ## Options

    - `:keep` — number of recent snapshots to retain (default `1`)

  ## Example

      # After bulk loading, compact to a single snapshot for fast reads
      :ok = AshIceberg.compact_snapshots(MyApp.Events)

      # Keep the 5 most recent snapshots (useful for time-travel auditing)
      :ok = AshIceberg.compact_snapshots(MyApp.Events, keep: 5)

  Returns `:ok` on success, `{:error, reason}` if the catalog does not
  support the expire-snapshots endpoint.
  """
  @spec compact_snapshots(Ash.Resource.t(), keyword()) :: :ok | {:error, term()}
  def compact_snapshots(resource, opts \\ []) do
    keep = opts[:keep] || 1
    catalog = AshIceberg.DataLayer.Info.catalog(resource)
    namespace = AshIceberg.DataLayer.Info.namespace(resource)
    table = AshIceberg.DataLayer.Info.table(resource)

    if catalog == nil do
      {:error, "compact_snapshots requires a REST catalog (resource has no catalog configured)"}
    else
      cfg = catalog.config()

      case AshIceberg.Catalog.RestClient.expire_snapshots(cfg, namespace, table,
             older_than: DateTime.utc_now(),
             min_snapshots_to_keep: keep
           ) do
        {:ok, _} -> :ok
        {:error, _} = err -> err
      end
    end
  end
end
