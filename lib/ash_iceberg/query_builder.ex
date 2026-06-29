defmodule AshIceberg.QueryBuilder do
  @moduledoc """
  Converts an `AshIceberg.Query` into executable SQL strings for DuckDB.

  The generated SQL uses DuckDB's `iceberg_scan()` function when a warehouse
  path is configured, or a catalog-qualified identifier when a catalog
  GenServer is in use.
  """

  alias AshIceberg.{Query, Filter}

  @doc """
  Builds a SELECT statement from `query`.

  Returns `{:ok, sql}` or `{:error, reason}`.
  """
  def build_select(%Query{} = query) do
    with {:ok, from_clause} <- build_from(query),
         {:ok, where_clause} <- build_where(query),
         {:ok, select_clause} <- build_select_clause(query),
         {:ok, order_clause} <- build_order(query),
         {:ok, limit_clause} <- build_limit(query) do
      parts =
        [
          "SELECT #{select_clause}",
          "FROM #{from_clause}",
          where_clause,
          order_clause,
          limit_clause
        ]
        |> Enum.reject(&is_nil/1)

      {:ok, Enum.join(parts, " ")}
    end
  end

  @doc """
  Builds an INSERT statement from a map of `attrs` for the given `query` target.
  """
  def build_insert(%Query{} = query, attrs) when is_map(attrs) do
    with {:ok, into_clause} <- build_into(query) do
      columns =
        attrs
        |> Map.keys()
        |> Enum.map(&~s("#{&1}"))
        |> Enum.join(", ")

      values =
        attrs
        |> Map.values()
        |> Enum.map(&format_value/1)
        |> Enum.join(", ")

      {:ok, "INSERT INTO #{into_clause} (#{columns}) VALUES (#{values})"}
    end
  end

  @doc """
  Builds an UPDATE statement for a single record identified by `pk_field = pk_value`.
  """
  def build_update(%Query{} = query, pk_field, pk_value, changes) when is_map(changes) do
    with {:ok, into_clause} <- build_into(query) do
      set_clause =
        changes
        |> Enum.map(fn {k, v} -> ~s("#{k}" = #{format_value(v)}) end)
        |> Enum.join(", ")

      pk_sql = ~s("#{pk_field}" = #{format_value(pk_value)})

      {:ok, "UPDATE #{into_clause} SET #{set_clause} WHERE #{pk_sql}"}
    end
  end

  @doc """
  Builds a single multi-row INSERT for a list of attribute maps.

  Iceberg creates one snapshot per statement, so batching rows into one INSERT
  is dramatically faster than individual calls to `build_insert/2`.
  """
  def build_bulk_insert(_query, []), do: {:ok, :empty}

  def build_bulk_insert(%Query{} = query, rows) when is_list(rows) do
    with {:ok, into_clause} <- build_into(query) do
      columns = rows |> hd() |> Map.keys() |> Enum.sort()
      columns_sql = Enum.map_join(columns, ", ", &~s["#{&1}"])

      values_sql =
        Enum.map_join(rows, ", ", fn row ->
          vals = Enum.map(columns, fn col -> format_value(Map.get(row, col)) end)
          "(#{Enum.join(vals, ", ")})"
        end)

      {:ok, "INSERT INTO #{into_clause} (#{columns_sql}) VALUES #{values_sql}"}
    end
  end

  @doc """
  Builds a DELETE statement for a single record identified by `pk_field = pk_value`.
  """
  def build_delete(%Query{} = query, pk_field, pk_value) do
    with {:ok, into_clause} <- build_into(query) do
      pk_sql = ~s("#{pk_field}" = #{format_value(pk_value)})
      {:ok, "DELETE FROM #{into_clause} WHERE #{pk_sql}"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # FROM clause — iceberg_scan() for warehouse mode, qualified name for catalog
  defp build_from(%Query{catalog: nil, warehouse: warehouse, namespace: ns, table: tbl})
       when is_binary(warehouse) do
    path = table_path(warehouse, ns, tbl)
    {:ok, "iceberg_scan('#{path}')"}
  end

  defp build_from(%Query{catalog: catalog, namespace: ns, table: tbl}) when not is_nil(catalog) do
    catalog_name = AshIceberg.Catalog.catalog_name(catalog)
    {:ok, "#{catalog_name}.#{ns}.#{tbl}"}
  end

  defp build_from(_), do: {:error, "No catalog or warehouse configured on query"}

  # INTO clause (same as FROM but without iceberg_scan wrapping for DML)
  defp build_into(%Query{catalog: nil, warehouse: warehouse, namespace: ns, table: tbl})
       when is_binary(warehouse) do
    path = table_path(warehouse, ns, tbl)
    # DuckDB supports DML on `iceberg_scan()` in newer versions; fall back to path
    {:ok, "iceberg_scan('#{path}')"}
  end

  defp build_into(%Query{catalog: catalog, namespace: ns, table: tbl}) when not is_nil(catalog) do
    catalog_name = AshIceberg.Catalog.catalog_name(catalog)
    {:ok, "#{catalog_name}.#{ns}.#{tbl}"}
  end

  defp build_into(_), do: {:error, "No catalog or warehouse configured on query"}

  defp table_path(warehouse, namespace, table) do
    warehouse = String.trim_trailing(warehouse, "/")
    "#{warehouse}/#{namespace}/#{table}"
  end

  defp build_select_clause(%Query{select: [], aggregates: []}) do
    {:ok, "*"}
  end

  defp build_select_clause(%Query{select: [], aggregates: aggs}) when aggs != [] do
    cols =
      Enum.map(aggs, fn agg ->
        build_aggregate_sql(agg)
      end)

    {:ok, Enum.join(cols, ", ")}
  end

  defp build_select_clause(%Query{select: fields, aggregates: []}) do
    cols = Enum.map_join(fields, ", ", &~s("#{&1}"))
    {:ok, cols}
  end

  defp build_select_clause(%Query{select: fields, aggregates: aggs}) do
    base = Enum.map_join(fields, ", ", &~s("#{&1}"))
    agg_sql = Enum.map_join(aggs, ", ", &build_aggregate_sql/1)
    {:ok, "#{base}, #{agg_sql}"}
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :count, name: name}) do
    ~s[COUNT(*) AS "#{name}"]
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :sum, field: field, name: name}) do
    ~s[SUM("#{field}") AS "#{name}"]
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :avg, field: field, name: name}) do
    ~s[AVG("#{field}") AS "#{name}"]
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :min, field: field, name: name}) do
    ~s[MIN("#{field}") AS "#{name}"]
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :max, field: field, name: name}) do
    ~s[MAX("#{field}") AS "#{name}"]
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :first, field: field, name: name}) do
    ~s[first("#{field}") AS "#{name}"]
  end

  defp build_aggregate_sql(%Ash.Query.Aggregate{kind: :list, field: field, name: name}) do
    ~s[LIST("#{field}") AS "#{name}"]
  end

  defp build_aggregate_sql(agg) do
    ~s[COUNT(*) AS "#{agg.name}"]
  end

  defp build_where(%Query{filter: nil}), do: {:ok, nil}

  defp build_where(%Query{filter: filter}) do
    case Filter.to_sql(filter) do
      {:ok, nil} -> {:ok, nil}
      {:ok, sql} -> {:ok, "WHERE #{sql}"}
      err -> err
    end
  end

  defp build_order(%Query{sort: []}), do: {:ok, nil}

  defp build_order(%Query{sort: sort}) do
    clause =
      Enum.map_join(sort, ", ", fn
        {field, :asc} -> ~s("#{field}" ASC)
        {field, :desc} -> ~s("#{field}" DESC)
        field when is_atom(field) -> ~s("#{field}" ASC)
      end)

    {:ok, "ORDER BY #{clause}"}
  end

  defp build_limit(%Query{limit: nil, offset: nil}), do: {:ok, nil}

  defp build_limit(%Query{limit: limit, offset: nil}) when is_integer(limit),
    do: {:ok, "LIMIT #{limit}"}

  defp build_limit(%Query{limit: nil, offset: offset}) when is_integer(offset),
    do: {:ok, "LIMIT 18446744073709551615 OFFSET #{offset}"}

  defp build_limit(%Query{limit: limit, offset: offset}),
    do: {:ok, "LIMIT #{limit} OFFSET #{offset}"}

  # Value formatting for SQL literals
  defp format_value(nil), do: "NULL"
  defp format_value(true), do: "true"
  defp format_value(false), do: "false"
  defp format_value(v) when is_integer(v), do: to_string(v)
  defp format_value(v) when is_float(v), do: to_string(v)
  defp format_value(%Decimal{} = d), do: Decimal.to_string(d, :normal)
  defp format_value(%DateTime{} = dt), do: "'#{DateTime.to_iso8601(dt)}'"
  defp format_value(%Date{} = d), do: "'#{Date.to_iso8601(d)}'"
  defp format_value(%Time{} = t), do: "'#{Time.to_iso8601(t)}'"
  defp format_value(v) when is_binary(v), do: "'#{escape(v)}'"
  defp format_value(v) when is_atom(v), do: "'#{escape(to_string(v))}'"

  defp format_value(v) when is_map(v) do
    case Jason.encode(v) do
      {:ok, json} -> "'#{escape(json)}'"
      _ -> "'{}'"
    end
  end

  defp format_value(v) when is_list(v) do
    case Jason.encode(v) do
      {:ok, json} -> "'#{escape(json)}'"
      _ -> "'[]'"
    end
  end

  defp format_value(v), do: "'#{escape(inspect(v))}'"

  defp escape(s) do
    s
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "''")
  end
end
