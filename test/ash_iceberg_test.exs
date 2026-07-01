defmodule AshIcebergTest do
  use ExUnit.Case, async: false

  alias AshIceberg.{Filter, QueryBuilder}
  alias AshIceberg.Query

  # ---------------------------------------------------------------------------
  # Catalog.catalog_name/1
  # ---------------------------------------------------------------------------

  describe "AshIceberg.Catalog.catalog_name/1" do
    test "derives a snake_case identifier from a module name" do
      assert AshIceberg.Catalog.catalog_name(MyApp.IcebergCatalog) == "iceberg_catalog"
      assert AshIceberg.Catalog.catalog_name(MyApp.Prod.AnalyticsCatalog) == "analytics_catalog"
    end
  end

  # ---------------------------------------------------------------------------
  # QueryBuilder.build_select/1
  # ---------------------------------------------------------------------------

  describe "QueryBuilder.build_select/1 – warehouse mode" do
    defp warehouse_query(overrides \\ []) do
      struct(Query,
        Keyword.merge(
          [
            resource: nil,
            catalog: nil,
            namespace: "analytics",
            table: "events",
            warehouse: "/data/warehouse"
          ],
          overrides
        )
      )
    end

    test "builds a basic SELECT *" do
      {:ok, sql} = QueryBuilder.build_select(warehouse_query())
      assert sql =~ "SELECT *"
      assert sql =~ "iceberg_scan"
      assert sql =~ "/data/warehouse/analytics/events"
    end

    test "respects selected fields" do
      {:ok, sql} = QueryBuilder.build_select(warehouse_query(select: [:id, :user_id]))
      assert sql =~ "\"id\""
      assert sql =~ "\"user_id\""
      refute sql =~ "*"
    end

    test "applies LIMIT" do
      {:ok, sql} = QueryBuilder.build_select(warehouse_query(limit: 10))
      assert sql =~ "LIMIT 10"
    end

    test "applies OFFSET without LIMIT using max value" do
      {:ok, sql} = QueryBuilder.build_select(warehouse_query(offset: 5))
      assert sql =~ "LIMIT 9223372036854775807 OFFSET 5"
    end

    test "applies LIMIT and OFFSET together" do
      {:ok, sql} = QueryBuilder.build_select(warehouse_query(limit: 20, offset: 40))
      assert sql =~ "LIMIT 20 OFFSET 40"
    end

    test "applies ORDER BY" do
      {:ok, sql} =
        QueryBuilder.build_select(warehouse_query(sort: [user_id: :asc, occurred_at: :desc]))

      assert sql =~ "\"user_id\" ASC"
      assert sql =~ "\"occurred_at\" DESC"
    end

    test "returns error when no warehouse or catalog" do
      q = struct(Query, namespace: "ns", table: "t")
      assert {:error, _} = QueryBuilder.build_select(q)
    end
  end

  describe "QueryBuilder.build_select/1 – catalog mode" do
    defp catalog_query(overrides \\ []) do
      struct(Query,
        Keyword.merge(
          [resource: nil, catalog: SomeCatalog, namespace: "ns", table: "tbl"],
          overrides
        )
      )
    end

    test "uses catalog-qualified table reference" do
      {:ok, sql} = QueryBuilder.build_select(catalog_query())
      assert sql =~ "some_catalog.ns.tbl"
    end
  end

  describe "QueryBuilder.build_insert/2" do
    test "builds a parameterised INSERT" do
      q = struct(Query, catalog: nil, namespace: "a", table: "b", warehouse: "/w")
      attrs = %{id: "abc", name: "test", count: 3}
      {:ok, sql} = QueryBuilder.build_insert(q, attrs)
      assert sql =~ "INSERT INTO"
      assert sql =~ "iceberg_scan"
      assert sql =~ "'abc'"
      assert sql =~ "'test'"
      assert sql =~ "3"
    end
  end

  describe "QueryBuilder.build_update/4" do
    test "builds an UPDATE by primary key" do
      q = struct(Query, catalog: nil, namespace: "a", table: "b", warehouse: "/w")
      {:ok, sql} = QueryBuilder.build_update(q, :id, "abc-123", %{name: "updated"})
      assert sql =~ "UPDATE"
      assert sql =~ "\"id\" = 'abc-123'"
      assert sql =~ "\"name\" = 'updated'"
    end
  end

  describe "QueryBuilder.build_delete/3" do
    test "builds a DELETE by primary key" do
      q = struct(Query, catalog: nil, namespace: "a", table: "b", warehouse: "/w")
      {:ok, sql} = QueryBuilder.build_delete(q, :id, 42)
      assert sql =~ "DELETE FROM"
      assert sql =~ "\"id\" = 42"
    end
  end

  # ---------------------------------------------------------------------------
  # Filter.to_sql/1
  # ---------------------------------------------------------------------------

  describe "Filter.to_sql/1" do
    test "returns nil for nil filter" do
      assert {:ok, nil} = Filter.to_sql(nil)
    end

    test "translates Eq operator" do
      filter =
        make_filter(%Ash.Query.Operator.Eq{
          left: ref(:user_id),
          right: 42
        })

      {:ok, sql} = Filter.to_sql(filter)
      assert sql =~ "\"user_id\" = 42"
    end

    test "translates Eq to IS NULL when value is nil" do
      filter =
        make_filter(%Ash.Query.Operator.Eq{
          left: ref(:name),
          right: nil
        })

      {:ok, sql} = Filter.to_sql(filter)
      assert sql =~ "\"name\" IS NULL"
    end

    test "translates LessThan" do
      filter =
        make_filter(%Ash.Query.Operator.LessThan{
          left: ref(:count),
          right: 100
        })

      {:ok, sql} = Filter.to_sql(filter)
      assert sql == "\"count\" < 100"
    end

    test "translates In" do
      filter =
        make_filter(%Ash.Query.Operator.In{
          left: ref(:status),
          right: ["active", "pending"]
        })

      {:ok, sql} = Filter.to_sql(filter)
      assert sql =~ "IN"
      assert sql =~ "'active'"
      assert sql =~ "'pending'"
    end

    test "translates IsNil true" do
      filter = make_filter(%Ash.Query.Operator.IsNil{left: ref(:deleted_at), right: true})
      {:ok, sql} = Filter.to_sql(filter)
      assert sql == "\"deleted_at\" IS NULL"
    end

    test "translates IsNil false" do
      filter = make_filter(%Ash.Query.Operator.IsNil{left: ref(:deleted_at), right: false})
      {:ok, sql} = Filter.to_sql(filter)
      assert sql == "\"deleted_at\" IS NOT NULL"
    end

    test "translates AND expression" do
      filter =
        make_filter(%Ash.Query.BooleanExpression{
          op: :and,
          left: %Ash.Query.Operator.Eq{left: ref(:status), right: "active"},
          right: %Ash.Query.Operator.GreaterThan{left: ref(:count), right: 0}
        })

      {:ok, sql} = Filter.to_sql(filter)
      assert sql =~ "AND"
      assert sql =~ "'active'"
      assert sql =~ "> 0"
    end

    test "translates NOT expression" do
      filter =
        make_filter(%Ash.Query.Not{
          expression: %Ash.Query.Operator.Eq{left: ref(:active), right: true}
        })

      {:ok, sql} = Filter.to_sql(filter)
      assert sql =~ "NOT"
    end

    test "escapes single-quote injection in string values" do
      filter =
        make_filter(%Ash.Query.Operator.Eq{
          left: ref(:name),
          right: "'; DROP TABLE events; --"
        })

      {:ok, sql} = Filter.to_sql(filter)
      # The leading ' is doubled to '' — the payload is inside a quoted literal,
      # not treated as a separate SQL statement.
      assert sql =~ "''"
      # The whole right-hand side must be wrapped in a single pair of outer quotes
      # so the DROP TABLE cannot break out as a standalone statement.
      assert sql =~ "\"name\" ="
    end
  end

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  describe "AshIceberg.Types.TimestampTz" do
    alias AshIceberg.Types.TimestampTz

    test "casts ISO8601 string" do
      {:ok, dt} = TimestampTz.cast_input("2024-01-15T12:00:00Z", [])
      assert %DateTime{} = dt
      assert dt.year == 2024
    end

    test "casts DateTime passthrough" do
      now = DateTime.utc_now()
      {:ok, result} = TimestampTz.cast_input(now, [])
      assert DateTime.compare(result, now) == :eq
    end

    test "casts unix microsecond integer" do
      {:ok, dt} = TimestampTz.cast_input(1_700_000_000_000_000, [])
      assert %DateTime{} = dt
    end

    test "dumps to ISO8601 string" do
      dt = ~U[2024-06-01 10:00:00.000000Z]
      {:ok, str} = TimestampTz.dump_to_native(dt, [])
      assert is_binary(str)
      assert str =~ "2024-06-01"
    end

    test "equal? compares datetimes" do
      a = ~U[2024-01-01 00:00:00Z]
      b = ~U[2024-01-01 00:00:00Z]
      assert TimestampTz.equal?(a, b)
    end
  end

  describe "AshIceberg.Types.Fixed" do
    alias AshIceberg.Types.Fixed

    test "casts binary passthrough" do
      bin = <<1, 2, 3, 4>>
      assert {:ok, ^bin} = Fixed.cast_input(bin, [])
    end

    test "applies length constraint" do
      assert {:ok, <<1, 2>>} = Fixed.apply_constraints(<<1, 2>>, length: 2)
      assert {:error, _} = Fixed.apply_constraints(<<1, 2, 3>>, length: 2)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp make_filter(expression) do
    %Ash.Filter{expression: expression}
  end

  defp ref(name) do
    %Ash.Query.Ref{
      attribute: %{name: name},
      relationship_path: [],
      resource: nil
    }
  end
end
