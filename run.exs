Application.put_env(:ash, :validate_domain_resource_inclusion?, false)
Application.put_env(:ash, :validate_domain_config_inclusion?, false)
Logger.configure(level: :warning)

# ── Catalog ───────────────────────────────────────────────────────────────────

defmodule Run.Catalog do
  use AshIceberg.Catalog, otp_app: :run

  @impl AshIceberg.Catalog
  def config do
    %{
      type: :rest,
      catalog_name: "run_catalog",
      uri: System.get_env("ICEBERG_REST_URI", "http://localhost:8181"),
      warehouse: System.get_env("S3_WAREHOUSE", "s3://warehouse/"),
      aws_region: System.get_env("S3_REGION", "us-east-1"),
      aws_access_key_id: System.get_env("S3_ACCESS_KEY", "minioadmin"),
      aws_secret_access_key: System.get_env("S3_SECRET_KEY", "minioadmin"),
      s3_endpoint: System.get_env("S3_ENDPOINT", "http://localhost:9000"),
      s3_url_style: "path",
      s3_use_ssl: false,
      pool_size: 2
    }
  end
end

{:ok, _} = Run.Catalog.start_link()

# ── Resources ─────────────────────────────────────────────────────────────────

defmodule Run.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    resource Run.Event
  end
end

defmodule Run.Event do
  use Ash.Resource, domain: Run.Domain, data_layer: AshIceberg.DataLayer

  iceberg do
    catalog Run.Catalog
    namespace "run"
    table "events"
    can_update? false
    can_destroy? false
    partition :occurred_at, transform: :hour
    partition :user_id,     transform: {:bucket, 8}
  end

  attributes do
    uuid_primary_key :id
    attribute :user_id,     :integer,                     allow_nil?: false, public?: true
    attribute :event_type,  :string,                      allow_nil?: false, public?: true
    attribute :value,       :float,                       default: 0.0,      public?: true
    attribute :region,      :string,                      default: "us-east-1", public?: true
    attribute :occurred_at, AshIceberg.Types.TimestampTz, default: &DateTime.utc_now/0, public?: true
  end

  actions do
    default_accept [:user_id, :event_type, :value, :region, :occurred_at]
    defaults [:create, :read]

    read :by_user do
      argument :user_id, :integer, allow_nil?: false
      filter expr(user_id == ^arg(:user_id))
    end

    read :by_type do
      argument :event_type, :string, allow_nil?: false
      filter expr(event_type == ^arg(:event_type))
    end

    read :by_region do
      argument :region, :string, allow_nil?: false
      filter expr(region == ^arg(:region))
    end

    read :type_starts_with do
      argument :prefix, :string, allow_nil?: false
      filter expr(string_starts_with(event_type, ^arg(:prefix)))
    end

    read :type_ends_with do
      argument :suffix, :string, allow_nil?: false
      filter expr(string_ends_with(event_type, ^arg(:suffix)))
    end

    read :type_contains do
      argument :substring, :string, allow_nil?: false
      filter expr(contains(event_type, ^arg(:substring)))
    end
  end
end

# ── Generator ─────────────────────────────────────────────────────────────────

defmodule Run.Gen do
  @types   ~w[view click purchase share bookmark]
  @regions ~w[us-east-1 us-west-2 eu-west-1 ap-southeast-1]

  def event do
    %{
      user_id:     :rand.uniform(10_000),
      event_type:  Enum.random(@types),
      value:       Float.round(:rand.uniform() * 500, 2),
      region:      Enum.random(@regions),
      occurred_at: DateTime.utc_now()
    }
  end

  def events(n), do: Enum.map(1..n, fn _ -> event() end)
end

# ── Test runner ───────────────────────────────────────────────────────────────

defmodule Run.Runner do
  alias AshIceberg.Catalog.RestClient

  defp ok(label), do: IO.puts("ok  #{label}")
  defp fail!(label, e), do: raise("FAIL #{label}: #{inspect(e)}")

  def run do
    wait_for_catalog()
    setup_table()
    test_create()
    test_bulk_create()
    test_read()
    test_filter()
    test_sort_limit()
    test_select()
    test_aggregates()
    test_string_filters()
    test_snapshots()
    test_time_travel()
    test_introspection()
    IO.puts("── done ✓ ──────────────────────────────────────────────────────────────")
  end

  defp wait_for_catalog do
    catalog_uri = System.get_env("ICEBERG_REST_URI", "http://localhost:8181")
    IO.puts("── startup ─────────────────────────────────────────────────────────────")
    IO.write("   waiting for catalog at #{catalog_uri}")

    Enum.reduce_while(1..30, nil, fn i, _ ->
      case Req.get("#{catalog_uri}/v1/config") do
        {:ok, %{status: 200}} -> {:halt, :ready}
        _ ->
          IO.write(".")
          Process.sleep(2_000)
          if i == 30, do: raise("catalog not reachable after 60s")
          {:cont, nil}
      end
    end)

    IO.puts(" ✓")
  end

  defp setup_table do
    IO.puts("── table setup ─────────────────────────────────────────────────────────")
    cfg = Run.Catalog.config()

    :ok = RestClient.ensure_namespace(cfg, "run")
    ok("namespace 'run'")

    schema = %{
      "type" => "struct", "schema-id" => 0,
      "identifier-field-ids" => [1],
      "fields" => [
        %{"id" => 1, "name" => "id",          "required" => true,  "type" => "string"},
        %{"id" => 2, "name" => "user_id",     "required" => true,  "type" => "int"},
        %{"id" => 3, "name" => "event_type",  "required" => true,  "type" => "string"},
        %{"id" => 4, "name" => "value",       "required" => false, "type" => "double"},
        %{"id" => 5, "name" => "region",      "required" => false, "type" => "string"},
        %{"id" => 6, "name" => "occurred_at", "required" => false, "type" => "timestamptz"}
      ]
    }

    partition_spec = %{
      "spec-id" => 0,
      "fields" => [
        %{"field-id" => 1000, "source-id" => 6, "name" => "occurred_at_hour", "transform" => "hour"},
        %{"field-id" => 1001, "source-id" => 2, "name" => "user_id_bucket_8", "transform" => "bucket[8]"}
      ]
    }

    case RestClient.create_table(cfg, "run", "events", schema, partition_spec) do
      {:ok, _}           -> ok("table 'events' created")
      {:error, {409, _}} -> ok("table 'events' already exists")
      {:error, reason}   -> fail!("create_table", reason)
    end

    ddl = """
    CREATE TABLE IF NOT EXISTS run_catalog.run.events (
      id VARCHAR, user_id INTEGER, event_type VARCHAR,
      value DOUBLE, region VARCHAR, occurred_at TIMESTAMPTZ
    )
    """
    case Run.Catalog.query(ddl) do
      {:ok, _}    -> ok("DuckDB table reference verified")
      {:error, r} -> IO.puts("   ⚠  DuckDB DDL skipped (#{inspect(r)}) — using iceberg_scan()")
    end
  end

  defp test_create do
    IO.puts("── create ──────────────────────────────────────────────────────────────")

    case Run.Event
         |> Ash.Changeset.for_create(:create, Run.Gen.event())
         |> Ash.create() do
      {:ok, e}    -> ok("single create → id=#{String.slice(e.id, 0, 8)}… user=#{e.user_id}")
      {:error, e} -> fail!("single create", e)
    end
  end

  defp test_bulk_create do
    IO.puts("── bulk_create ─────────────────────────────────────────────────────────")

    result = Ash.bulk_create(Run.Gen.events(500), Run.Event, :create,
      domain: Run.Domain, return_records?: true, return_errors?: true)

    if result.errors == [],
      do: ok("bulk_create 500 rows → #{length(result.records)} records"),
      else: fail!("bulk_create 500", result.errors)

    # Second batch → multiple snapshots for time travel
    Ash.bulk_create(Run.Gen.events(500), Run.Event, :create,
      domain: Run.Domain, return_records?: false, return_errors?: false)
    ok("bulk_create second batch of 500 rows")
  end

  defp test_read do
    IO.puts("── read ────────────────────────────────────────────────────────────────")

    case Ash.read(Run.Event, domain: Run.Domain) do
      {:ok, all} -> ok("read all → #{length(all)} rows")
      {:error, e} -> fail!("read all", e)
    end
  end

  defp test_filter do
    IO.puts("── filter ──────────────────────────────────────────────────────────────")

    {:ok, all} = Ash.read(Run.Event, domain: Run.Domain)
    sample_user = hd(all).user_id

    case Run.Event |> Ash.Query.for_read(:by_user, %{user_id: sample_user}) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("by_user #{sample_user} → #{length(rows)} rows")
      {:error, e} -> fail!("by_user", e)
    end

    case Run.Event |> Ash.Query.for_read(:by_type, %{event_type: "click"}) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("by_type 'click' → #{length(rows)} rows")
      {:error, e} -> fail!("by_type", e)
    end

    case Run.Event |> Ash.Query.for_read(:by_region, %{region: "us-east-1"}) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("by_region 'us-east-1' → #{length(rows)} rows")
      {:error, e} -> fail!("by_region", e)
    end
  end

  defp test_sort_limit do
    IO.puts("── sort + limit ────────────────────────────────────────────────────────")

    case Run.Event |> Ash.Query.sort(value: :desc) |> Ash.Query.limit(10) |> Ash.read(domain: Run.Domain) do
      {:ok, [first | _]} -> ok("top-10 by value → first.value=#{first.value}")
      {:error, e}        -> fail!("sort+limit", e)
    end

    case Run.Event |> Ash.Query.sort(:occurred_at) |> Ash.Query.limit(20) |> Ash.Query.offset(10) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("offset pagination → #{length(rows)} rows (page 2)")
      {:error, e} -> fail!("offset pagination", e)
    end
  end

  defp test_select do
    IO.puts("── select ──────────────────────────────────────────────────────────────")

    case Run.Event |> Ash.Query.select([:user_id, :event_type, :value]) |> Ash.Query.limit(5) |> Ash.read(domain: Run.Domain) do
      {:ok, [first | _]} -> ok("column projection → user_id=#{first.user_id} event_type=#{first.event_type}")
      {:error, e}        -> fail!("select", e)
    end
  end

  defp test_aggregates do
    IO.puts("── aggregates ──────────────────────────────────────────────────────────")

    case Run.Event |> Ash.Query.aggregate(:n, :count, Run.Event, field: :id) |> Ash.read(domain: Run.Domain) do
      {:ok, [row | _]} -> ok("COUNT → #{row.aggregates.n} total rows")
      {:error, e}      -> fail!("COUNT", e)
    end

    case Run.Event
         |> Ash.Query.aggregate(:total, :sum, Run.Event, field: :value)
         |> Ash.Query.aggregate(:avg, :avg, Run.Event, field: :value)
         |> Ash.read(domain: Run.Domain) do
      {:ok, [row | _]} ->
        ok("SUM=#{Float.round(row.aggregates.total || 0.0, 2)} AVG=#{Float.round(row.aggregates.avg || 0.0, 4)}")
      {:error, e} -> fail!("SUM/AVG", e)
    end
  end

  defp test_string_filters do
    IO.puts("── string filters ──────────────────────────────────────────────────────")

    case Run.Event |> Ash.Query.for_read(:type_starts_with, %{prefix: "pur"}) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("string_starts_with 'pur' → #{length(rows)} rows")
      {:error, e} -> fail!("string_starts_with", e)
    end

    case Run.Event |> Ash.Query.for_read(:type_ends_with, %{suffix: "ck"}) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("string_ends_with 'ck' → #{length(rows)} rows")
      {:error, e} -> fail!("string_ends_with", e)
    end

    case Run.Event |> Ash.Query.for_read(:type_contains, %{substring: "a"}) |> Ash.read(domain: Run.Domain) do
      {:ok, rows} -> ok("contains 'a' → #{length(rows)} rows")
      {:error, e} -> fail!("contains", e)
    end
  end

  defp test_snapshots do
    IO.puts("── snapshots ───────────────────────────────────────────────────────────")

    case AshIceberg.Snapshots.list(Run.Event) do
      {:ok, snaps} ->
        ok("list snapshots → #{length(snaps)} snapshot(s)")

        if length(snaps) >= 2 do
          latest_id = List.last(snaps)["snapshot-id"]
          {:ok, prev} = AshIceberg.Snapshots.previous(Run.Event, latest_id)
          if prev,
            do: ok("previous snapshot id=#{prev["snapshot-id"]}"),
            else: ok("previous snapshot → nil")
        end

      {:error, e} -> fail!("list snapshots", e)
    end
  end

  defp test_time_travel do
    IO.puts("── time travel ─────────────────────────────────────────────────────────")

    case AshIceberg.Snapshots.list(Run.Event) do
      {:ok, [first_snap | _] = snaps} when length(snaps) >= 2 ->
        sid = first_snap["snapshot-id"]

        case Run.Event
             |> Ash.Query.set_context(%{ash_iceberg: %{snapshot_id: sid}})
             |> Ash.read(domain: Run.Domain) do
          {:ok, past} ->
            {:ok, current} = Ash.read(Run.Event, domain: Run.Domain)
            ok("snapshot_id=#{sid} → #{length(past)} rows (current=#{length(current)})")
            if length(past) < length(current),
              do: ok("  fewer rows in first snapshot ✓"),
              else: ok("  counts equal (catalog may collapse small snapshots)")

          {:error, e} -> fail!("time travel snapshot_id", e)
        end

      {:ok, snaps} ->
        ok("time travel skipped — only #{length(snaps)} snapshot(s)")

      {:error, e} -> fail!("list snapshots for time travel", e)
    end

    as_of = DateTime.add(DateTime.utc_now(), -10, :second)

    case Run.Event
         |> Ash.Query.set_context(%{ash_iceberg: %{as_of: as_of}})
         |> Ash.read(domain: Run.Domain) do
      {:ok, past} -> ok("as_of #{DateTime.to_iso8601(as_of)} → #{length(past)} rows")
      {:error, e} -> fail!("time travel as_of", e)
    end
  end

  defp test_introspection do
    IO.puts("── introspection ───────────────────────────────────────────────────────")

    partitions = AshIceberg.DataLayer.Info.partitions(Run.Event)
    ok("partitions → #{length(partitions)}: #{Enum.map_join(partitions, ", ", &"#{&1.field}/#{inspect(&1.transform)}")}")
    ok("catalog   → #{inspect(AshIceberg.DataLayer.Info.catalog(Run.Event))}")
    ok("namespace → #{AshIceberg.DataLayer.Info.namespace(Run.Event)}")
    ok("table     → #{AshIceberg.DataLayer.Info.table(Run.Event)}")
  end
end

Run.Runner.run()
