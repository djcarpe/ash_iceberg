defmodule AshIceberg.DataLayer.Info do
  @moduledoc """
  DSL section definitions and introspection helpers for the Iceberg data layer.
  """

  alias Spark.Dsl.{Extension, Section}

  @doc "Returns the `iceberg` Spark DSL section used in resource definitions."
  def iceberg do
    %Section{
      name: :iceberg,
      describe: "Iceberg-specific configuration for this Ash resource.",
      examples: [
        """
        iceberg do
          catalog MyApp.IcebergCatalog
          namespace "analytics"
          table "events"
        end
        """,
        """
        # Filesystem / no catalog server
        iceberg do
          warehouse "/data/warehouse"
          namespace "analytics"
          table "events"
        end
        """
      ],
      schema: [
        catalog: [
          type: :atom,
          doc: """
          The catalog module (created with `use AshIceberg.Catalog`).
          Supply either `catalog` or `warehouse`, not both.
          """
        ],
        namespace: [
          type: :string,
          doc: "Iceberg namespace (database / schema) containing the table.",
          required: true
        ],
        table: [
          type: :string,
          doc: "Iceberg table name.",
          required: true
        ],
        warehouse: [
          type: :string,
          doc: """
          Warehouse root path used when `catalog` is not set.
          Supports local paths and S3 URIs (`s3://bucket/prefix`).
          The table is expected at `<warehouse>/<namespace>/<table>/`.
          """
        ],
        can_update?: [
          type: :boolean,
          doc: "Allow UPDATE operations. Requires Iceberg V2 or a catalog that supports row-level updates.",
          default: true
        ],
        can_destroy?: [
          type: :boolean,
          doc: "Allow DELETE operations. Requires Iceberg V2 or a catalog that supports row-level deletes.",
          default: true
        ]
      ]
    }
  end

  @doc "Catalog module configured on `resource`, or `nil`."
  def catalog(resource), do: Extension.get_opt(resource, [:iceberg], :catalog, nil)

  @doc "Iceberg namespace configured on `resource`."
  def namespace(resource), do: Extension.get_opt(resource, [:iceberg], :namespace, nil, true)

  @doc "Iceberg table name configured on `resource`."
  def table(resource), do: Extension.get_opt(resource, [:iceberg], :table, nil, true)

  @doc "Warehouse path configured on `resource`, or `nil`."
  def warehouse(resource), do: Extension.get_opt(resource, [:iceberg], :warehouse, nil)

  @doc "Whether UPDATE is enabled for `resource`."
  def can_update?(resource), do: Extension.get_opt(resource, [:iceberg], :can_update?, true)

  @doc "Whether DELETE is enabled for `resource`."
  def can_destroy?(resource), do: Extension.get_opt(resource, [:iceberg], :can_destroy?, true)
end
