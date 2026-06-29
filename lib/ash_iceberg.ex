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
end
