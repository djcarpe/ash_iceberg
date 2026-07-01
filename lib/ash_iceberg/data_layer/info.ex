defmodule AshIceberg.DataLayer.Info do
  @moduledoc """
  DSL section definitions and introspection helpers for the Iceberg data layer.
  """

  alias AshIceberg.Partition
  alias Spark.Dsl.{Extension, Section, Entity}

  @partition_entity %Entity{
    name: :partition,
    describe: """
    Define one field in the Iceberg partition spec for this table.

    Partitioning dramatically improves query performance for large tables by
    letting the Iceberg query engine skip files that cannot contain matching rows.

    ## Transforms

    | Value | Iceberg transform | Description |
    |-------|-------------------|-------------|
    | `:identity` | identity | Exact-match partitioning (default) |
    | `:year` | year | Partition by calendar year |
    | `:month` | month | Partition by calendar month |
    | `:day` | day | Partition by calendar day |
    | `:hour` | hour | Partition by clock hour |
    | `{:bucket, n}` | bucket[n] | Hash-distribute into `n` buckets |
    | `{:truncate, n}` | truncate[n] | Truncate string / int value to `n` |
    """,
    examples: [
      "partition :occurred_at, transform: :hour",
      "partition :user_id,     transform: {:bucket, 16}",
      "partition :region"
    ],
    args: [:field],
    schema: [
      field: [
        type: :atom,
        required: true,
        doc: "The resource attribute to partition by."
      ],
      transform: [
        type: :any,
        default: :identity,
        doc: """
        The partition transform to apply.
        One of `:identity`, `:year`, `:month`, `:day`, `:hour`,
        `{:bucket, n}`, or `{:truncate, n}`.
        """
      ]
    ],
    target: Partition
  }

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

          partition :occurred_at, transform: :hour
          partition :user_id,     transform: {:bucket, 16}
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
      entities: [@partition_entity],
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

  @doc """
  Returns the list of `%AshIceberg.Partition{}` entities declared on the resource.

  Returns `[]` when no `partition` blocks are defined.
  """
  def partitions(resource) do
    Extension.get_entities(resource, [:iceberg])
    |> Enum.filter(&is_struct(&1, Partition))
  end
end
