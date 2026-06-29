defmodule AshIceberg.DataLayer do
  @moduledoc """
  Ash data layer backed by Apache Iceberg.

  Uses DuckDB (via `duckdbex`) as the query engine and an optional Iceberg
  REST Catalog for table management.

  ## Resource DSL

      defmodule MyApp.Events do
        use Ash.Resource, data_layer: AshIceberg.DataLayer

        iceberg do
          catalog MyApp.IcebergCatalog   # use AshIceberg.Catalog
          namespace "analytics"
          table "events"
        end
      end

  ## Supported Capabilities

  | Capability | Supported |
  |------------|-----------|
  | `:read` | ✅ |
  | `:create` | ✅ |
  | `:update` | ✅ (configurable) |
  | `:destroy` | ✅ (configurable) |
  | `:bulk_create` | ✅ |
  | `:sort` | ✅ |
  | `:filter` | ✅ |
  | `:limit` | ✅ |
  | `:offset` | ✅ |
  | `:select` | ✅ |
  | `:aggregate` | ✅ |
  | `:distinct` | ✅ |
  | `:transact` | ❌ |
  | `:multitenancy` | ❌ |
  | `:upsert` | ❌ |
  """

  use Spark.Dsl.Extension,
    sections: [AshIceberg.DataLayer.Info.iceberg()],
    verifiers: [AshIceberg.DataLayer.Verifiers.RequireCatalogOrWarehouse]

  @behaviour Ash.DataLayer

  alias AshIceberg.{Query, QueryBuilder, Connection}
  alias AshIceberg.DataLayer.Info
  alias Ash.Resource

  # ---------------------------------------------------------------------------
  # Capability declarations
  # ---------------------------------------------------------------------------

  @impl Ash.DataLayer
  def can?(resource, :update), do: Info.can_update?(resource)
  def can?(resource, :destroy), do: Info.can_destroy?(resource)
  def can?(_resource, :create), do: true
  def can?(_resource, :read), do: true
  def can?(_resource, :bulk_create), do: true
  def can?(_resource, :sort), do: true
  def can?(_resource, :filter), do: true
  def can?(_resource, :limit), do: true
  def can?(_resource, :offset), do: true
  def can?(_resource, :select), do: true
  def can?(_resource, :aggregate), do: true
  def can?(_resource, :distinct), do: true
  def can?(_resource, :composite_primary_key), do: true
  def can?(_resource, :expression_calculation), do: true
  def can?(_resource, :transact), do: false
  def can?(_resource, :multitenancy), do: false
  def can?(_resource, :upsert), do: false
  def can?(_resource, :lateral_join), do: false
  def can?(_resource, _), do: false

  # ---------------------------------------------------------------------------
  # Query lifecycle
  # ---------------------------------------------------------------------------

  @impl Ash.DataLayer
  def resource_to_query(resource, domain) do
    %Query{
      resource: resource,
      domain: domain,
      catalog: Info.catalog(resource),
      namespace: Info.namespace(resource),
      table: Info.table(resource),
      warehouse: Info.warehouse(resource)
    }
  end

  @impl Ash.DataLayer
  def run_query(%Query{} = query, resource) do
    with {:ok, sql} <- QueryBuilder.build_select(query),
         {:ok, rows} <- exec(query, sql) do
      {:ok, rows_to_records(rows, resource)}
    end
  end

  @impl Ash.DataLayer
  def select(query, fields, _resource) do
    {:ok, %{query | select: fields}}
  end

  @impl Ash.DataLayer
  def filter(query, filter, _resource) do
    {:ok, %{query | filter: filter}}
  end

  @impl Ash.DataLayer
  def sort(query, sort, _resource) do
    {:ok, %{query | sort: sort}}
  end

  @impl Ash.DataLayer
  def limit(query, limit, _resource) do
    {:ok, %{query | limit: limit}}
  end

  @impl Ash.DataLayer
  def offset(query, offset, _resource) do
    {:ok, %{query | offset: offset}}
  end

  @impl Ash.DataLayer
  def distinct(query, distinct, _resource) do
    {:ok, %{query | distinct: distinct}}
  end

  @impl Ash.DataLayer
  def set_tenant(query, tenant, _resource) do
    {:ok, %{query | tenant: tenant}}
  end

  @impl Ash.DataLayer
  def add_aggregates(query, aggregates, _resource) do
    {:ok, %{query | aggregates: aggregates}}
  end

  # ---------------------------------------------------------------------------
  # Mutations
  # ---------------------------------------------------------------------------

  @impl Ash.DataLayer
  def create(resource, changeset) do
    attrs = build_attrs(changeset, resource)
    query = resource_to_query(resource, nil)

    with {:ok, sql} <- QueryBuilder.build_insert(query, attrs),
         {:ok, _rows} <- exec(query, sql) do
      # DuckDB Iceberg INSERT does not return the inserted row; reconstruct
      # the record from the changeset
      record =
        changeset.data
        |> Map.merge(attrs)
        |> to_struct(resource)

      {:ok, record}
    end
  end

  @impl Ash.DataLayer
  def update(resource, changeset) do
    pk_field = primary_key_field(resource)
    pk_value = Map.get(changeset.data, pk_field)
    changes = Map.new(changeset.attributes)
    query = resource_to_query(resource, nil)

    with {:ok, sql} <- QueryBuilder.build_update(query, pk_field, pk_value, changes),
         {:ok, _rows} <- exec(query, sql) do
      updated = Map.merge(changeset.data, changes)
      {:ok, updated}
    end
  end

  @impl Ash.DataLayer
  def destroy(resource, changeset) do
    pk_field = primary_key_field(resource)
    pk_value = Map.get(changeset.data, pk_field)
    query = resource_to_query(resource, nil)

    with {:ok, sql} <- QueryBuilder.build_delete(query, pk_field, pk_value),
         {:ok, _rows} <- exec(query, sql) do
      :ok
    end
  end

  @impl Ash.DataLayer
  def bulk_create(resource, stream, options) do
    # One INSERT per chunk → one Iceberg snapshot per chunk, which is far
    # more efficient than one snapshot per row.
    batch_size = options[:batch_size] || 5_000
    query = resource_to_query(resource, nil)

    stream
    |> Stream.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, []}, fn batch, {:ok, acc} ->
      rows = Enum.map(batch, &build_attrs(&1, resource))

      with {:ok, sql} <- QueryBuilder.build_bulk_insert(query, rows),
           {:ok, _} <- exec(query, sql) do
        records =
          Enum.map(batch, fn cs ->
            cs.data |> Map.merge(build_attrs(cs, resource)) |> to_struct(resource)
          end)

        {:cont, {:ok, acc ++ records}}
      else
        {:ok, :empty} -> {:cont, {:ok, acc}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Transaction stubs (Iceberg does not support multi-statement transactions)
  # ---------------------------------------------------------------------------

  @impl Ash.DataLayer
  def transaction(_resource, func, _timeout, _opts), do: func.()

  @impl Ash.DataLayer
  def rollback(_resource, value), do: {:error, value}

  @impl Ash.DataLayer
  def in_transaction?(_resource), do: false

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp exec(%Query{catalog: nil, warehouse: warehouse} = _query, sql)
       when is_binary(warehouse) do
    # No per-resource GenServer; use the global connection for warehouse queries.
    # In a real app, the supervision tree would include a catalog GenServer;
    # here we open an ad-hoc connection if none is registered.
    server = AshIceberg.Connection

    case Process.whereis(server) do
      nil -> {:error, "No AshIceberg.Connection process running. Start one in your supervision tree."}
      _pid -> Connection.query(server, sql)
    end
  end

  defp exec(%Query{catalog: catalog} = _query, sql) when not is_nil(catalog) do
    case Process.whereis(catalog) do
      nil -> {:error, "Catalog #{inspect(catalog)} is not running. Add it to your supervision tree."}
      _pid -> Connection.query(catalog, sql)
    end
  end

  defp rows_to_records(rows, resource) do
    attributes = Resource.Info.attributes(resource)
    attr_map = Map.new(attributes, &{to_string(&1.name), &1})

    Enum.map(rows, fn row ->
      row
      |> Enum.into(%{}, fn {col, value} ->
        attr = Map.get(attr_map, col)
        key = if attr, do: attr.name, else: String.to_existing_atom(col)
        cast_value = if attr, do: cast(value, attr.type), else: value
        {key, cast_value}
      end)
      |> to_struct(resource)
    end)
  end

  defp to_struct(map, resource) do
    struct(resource, map)
  end

  defp build_attrs(changeset, resource) do
    attributes = Resource.Info.attributes(resource)

    Map.new(attributes, fn attr ->
      value =
        Map.get(changeset.attributes, attr.name) ||
          Map.get(changeset.data, attr.name)

      {attr.name, value}
    end)
  end

  defp primary_key_field(resource) do
    case Resource.Info.primary_key(resource) do
      [field] -> field
      [field | _] -> field
      [] -> :id
    end
  end

  # Basic value casting from DuckDB native types to Elixir types
  defp cast(nil, _type), do: nil
  defp cast(value, Ash.Type.String), do: to_string(value)
  defp cast(value, Ash.Type.Integer) when is_integer(value), do: value
  defp cast(value, Ash.Type.Integer), do: String.to_integer(to_string(value))
  defp cast(value, Ash.Type.Float) when is_float(value), do: value
  defp cast(value, Ash.Type.Float), do: String.to_float(to_string(value))
  defp cast(value, Ash.Type.Boolean) when is_boolean(value), do: value
  defp cast(1, Ash.Type.Boolean), do: true
  defp cast(0, Ash.Type.Boolean), do: false

  defp cast(value, Ash.Type.UtcDatetime) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _} -> dt
      _ -> value
    end
  end

  defp cast(value, Ash.Type.Date) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, d} -> d
      _ -> value
    end
  end

  defp cast(value, Ash.Type.Map) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, map} -> map
      _ -> value
    end
  end

  defp cast(value, _type), do: value
end
