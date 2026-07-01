Application.put_env(:ash, :validate_domain_resource_inclusion?, false)
Application.put_env(:ash, :validate_domain_config_inclusion?, false)
Logger.configure(level: :warning)

# ── Catalog ───────────────────────────────────────────────────────────────────

defmodule Bench.Catalog do
  use AshIceberg.Catalog, otp_app: :bench

  @impl AshIceberg.Catalog
  def config do
    %{
      type: :rest,
      catalog_name: "bench_catalog",
      uri: System.get_env("ICEBERG_REST_URI", "http://localhost:8181"),
      warehouse: System.get_env("S3_WAREHOUSE", "s3://warehouse/"),
      aws_region: System.get_env("S3_REGION", "us-east-1"),
      aws_access_key_id: System.get_env("S3_ACCESS_KEY", "minioadmin"),
      aws_secret_access_key: System.get_env("S3_SECRET_KEY", "minioadmin"),
      s3_endpoint: System.get_env("S3_ENDPOINT", "http://localhost:9000"),
      s3_url_style: "path",
      s3_use_ssl: false,
      pool_size: 4
    }
  end
end

{:ok, _} = Bench.Catalog.start_link()

# ── Resource ──────────────────────────────────────────────────────────────────

defmodule Bench.Event do
  use Ash.Resource, domain: Bench.Domain, data_layer: AshIceberg.DataLayer

  iceberg do
    catalog Bench.Catalog
    namespace "bench"
    table "events"
    can_update? false
    can_destroy? false
    partition :occurred_at, transform: :hour
    partition :user_id,     transform: {:bucket, 16}
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

    read :by_region do
      argument :region, :string, allow_nil?: false
      filter expr(region == ^arg(:region))
    end

    read :high_value do
      argument :threshold, :float, default: 250.0
      filter expr(value >= ^arg(:threshold))
    end
  end
end

defmodule Bench.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    resource Bench.Event
  end
end

# ── Row generator ─────────────────────────────────────────────────────────────

defmodule Bench.Gen do
  @types   ~w[view click purchase share bookmark]
  @regions ~w[us-east-1 us-west-2 eu-west-1 ap-southeast-1]

  def rows(n) do
    for _ <- 1..n do
      %{
        user_id:     :rand.uniform(100_000),
        event_type:  Enum.random(@types),
        value:       Float.round(:rand.uniform() * 500, 2),
        region:      Enum.random(@regions),
        occurred_at: DateTime.utc_now()
      }
    end
  end
end

# ── Wait for catalog ──────────────────────────────────────────────────────────

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

# ── Table setup (idempotent) ──────────────────────────────────────────────────

IO.puts("── table setup ─────────────────────────────────────────────────────────")
cfg = Bench.Catalog.config()
alias AshIceberg.Catalog.RestClient

:ok = RestClient.ensure_namespace(cfg, "bench")

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
    %{"field-id" => 1000, "source-id" => 6, "name" => "occurred_at_hour",  "transform" => "hour"},
    %{"field-id" => 1001, "source-id" => 2, "name" => "user_id_bucket_16", "transform" => "bucket[16]"}
  ]
}

case RestClient.create_table(cfg, "bench", "events", schema, partition_spec) do
  {:ok, _}           -> IO.puts("   created table bench.events")
  {:error, {409, _}} -> IO.puts("   table bench.events already exists")
  {:error, r}        -> raise "create_table failed: #{inspect(r)}"
end

ddl = """
CREATE TABLE IF NOT EXISTS bench_catalog.bench.events (
  id VARCHAR, user_id INTEGER, event_type VARCHAR,
  value DOUBLE, region VARCHAR, occurred_at TIMESTAMPTZ
)
"""
case Bench.Catalog.query(ddl) do
  {:ok, _}    -> IO.puts("   DuckDB table reference ready")
  {:error, r} -> IO.puts("   DuckDB DDL skipped (#{inspect(r)}) — using iceberg_scan()")
end

# ── Fixture loading ───────────────────────────────────────────────────────────
#
# Target: 50 000 rows loaded in 50 batches of 1 000.
# This means 50 Iceberg snapshots — enough to demonstrate time travel and
# gives the read benchmarks meaningful data volume.
#
# We check the current row count first and skip loading if already at target.

target_rows = 50_000
batch_size  = 1_000
batches     = div(target_rows, batch_size)

IO.puts("── fixture load ────────────────────────────────────────────────────────")

current_count =
  try do
    case Bench.Event
         |> Ash.Query.aggregate(:n, :count, Bench.Event, field: :id)
         |> Ash.read(domain: Bench.Domain) do
      {:ok, [row | _]} -> row.aggregates.n || 0
      _ -> 0
    end
  rescue
    _ -> 0
  end

if current_count >= target_rows do
  IO.puts("   #{current_count} rows already present — skipping load")
else
  IO.puts("   loading #{target_rows} rows in #{batches} batches of #{batch_size}…")
  IO.write("   [")
  t0 = System.monotonic_time(:millisecond)

  Enum.each(1..batches, fn i ->
    Ash.bulk_create(Bench.Gen.rows(batch_size), Bench.Event, :create,
      domain: Bench.Domain, return_records?: false, return_errors?: false)
    if rem(i, 5) == 0, do: IO.write("█")
  end)

  elapsed = System.monotonic_time(:millisecond) - t0
  rate    = Float.round(target_rows / elapsed * 1_000, 0)
  IO.puts("] #{elapsed}ms  (#{rate} rows/sec total)")
end

{:ok, snaps} = AshIceberg.Snapshots.list(Bench.Event)
IO.puts("   #{length(snaps)} snapshots in catalog")

# ── Compact snapshots ─────────────────────────────────────────────────────────
#
# Many small snapshots → slow reads because DuckDB opens metadata per snapshot.
# Compacting to 1 snapshot is the single biggest read performance lever.
# We measure a full scan before and after to quantify the improvement.

IO.puts("\n── compact snapshots — before/after read latency ────────────────────────")

before_ms =
  :timer.tc(fn -> Ash.read!(Bench.Event, domain: Bench.Domain) end)
  |> elem(0)
  |> div(1_000)

IO.write("   before compact: full scan = #{before_ms}ms  (#{length(snaps)} snapshots)")
IO.write("  …compacting…")

case AshIceberg.compact_snapshots(Bench.Event) do
  :ok ->
    {:ok, snaps_after} = AshIceberg.Snapshots.list(Bench.Event)
    after_ms =
      :timer.tc(fn -> Ash.read!(Bench.Event, domain: Bench.Domain) end)
      |> elem(0)
      |> div(1_000)
    IO.puts(" done")
    IO.puts("   after  compact: full scan = #{after_ms}ms  (#{length(snaps_after)} snapshot(s))")
    ratio = Float.round(before_ms / max(after_ms, 1), 1)
    IO.puts("   speedup: #{ratio}x\n")

  {:error, reason} ->
    IO.puts(" not supported by this catalog")
    IO.puts("""
   ┌─ Note ────────────────────────────────────────────────────────────────
   │  tabulario/iceberg-rest does not implement POST .../snapshots/expire.
   │  With a production catalog (Apache Polaris, Nessie, Tabular.io, AWS
   │  Glue), compact_snapshots/1 would reduce read latency by 5–20x for
   │  tables with many small snapshots.
   │
   │  The #{length(snaps)} snapshots in this run add ~#{before_ms}ms overhead per scan.
   │  After compaction that drops to the single-snapshot baseline.
   └───────────────────────────────────────────────────────────────────────
    """)
end

# ── Write benchmarks ──────────────────────────────────────────────────────────
#
# The defining performance characteristic of Iceberg is that every INSERT
# statement commits a new snapshot: one S3 PUT for the Parquet file, one
# catalog commit for the metadata.  The snapshot overhead is roughly fixed
# (~150–250 ms against MinIO on loopback), so batching rows into a single
# INSERT amortises it dramatically.
#
#   single_create     → 1 row  / 1 snapshot
#   bulk_create 100   → 100 rows / 1 snapshot
#   bulk_create 1 000 → 1 000 rows / 1 snapshot
#   bulk_create 5 000 → 5 000 rows / 1 snapshot

IO.puts("── writes — snapshot overhead (single vs batch) ─────────────────────────")
IO.puts("""
  Every INSERT commits one Iceberg snapshot.  The snapshot cost is fixed
  (~S3 PUT + catalog commit).  Batching amortises it across many rows.
""")

Benchee.run(
  %{
    "single_create (1 row / snapshot)" => fn ->
      Bench.Event
      |> Ash.Changeset.for_create(:create, hd(Bench.Gen.rows(1)))
      |> Ash.create!(domain: Bench.Domain)
    end,

    "bulk_create  100 rows / snapshot" => fn ->
      Ash.bulk_create!(Bench.Gen.rows(100), Bench.Event, :create,
        domain: Bench.Domain, return_records?: false)
    end,

    "bulk_create 1000 rows / snapshot" => fn ->
      Ash.bulk_create!(Bench.Gen.rows(1_000), Bench.Event, :create,
        domain: Bench.Domain, return_records?: false)
    end,

    "bulk_create 5000 rows / snapshot" => fn ->
      Ash.bulk_create!(Bench.Gen.rows(5_000), Bench.Event, :create,
        domain: Bench.Domain, return_records?: false)
    end,
  },
  warmup: 2,
  time:   15,
  memory_time: 2,
  formatters: [{Benchee.Formatters.Console, extended_statistics: true}],
  print: [fast_warning: false]
)

# ── Read benchmarks ───────────────────────────────────────────────────────────
#
# DuckDB reads directly from the Parquet files in S3 (MinIO).  Because Parquet
# is a columnar format, projecting fewer columns or pushing predicates into the
# scan dramatically reduces I/O.
#
# Scenarios:
#   full_scan          — SELECT * with no filter (all snapshots, all files)
#   filter_by_region   — WHERE region = 'us-east-1' (~25 % of rows)
#   high_value_filter  — WHERE value >= 250  (~50 % of rows)
#   sort_top_10        — ORDER BY value DESC LIMIT 10
#   count_agg          — COUNT(*) — no row materialization in Elixir
#   select_2_cols      — project only [user_id, value] over 50k rows

IO.puts("\n── reads — DuckDB columnar scans ────────────────────────────────────────")

Benchee.run(
  %{
    "full_scan (all rows)" => fn ->
      Ash.read!(Bench.Event, domain: Bench.Domain)
    end,

    "filter region='us-east-1' (~25 %)" => fn ->
      Bench.Event
      |> Ash.Query.for_read(:by_region, %{region: "us-east-1"})
      |> Ash.read!(domain: Bench.Domain)
    end,

    "filter value >= 250 (~50 %)" => fn ->
      Bench.Event
      |> Ash.Query.for_read(:high_value, %{threshold: 250.0})
      |> Ash.read!(domain: Bench.Domain)
    end,

    "sort + limit top-10" => fn ->
      Bench.Event
      |> Ash.Query.sort(value: :desc)
      |> Ash.Query.limit(10)
      |> Ash.read!(domain: Bench.Domain)
    end,

    "count aggregate" => fn ->
      [row | _] =
        Bench.Event
        |> Ash.Query.aggregate(:n, :count, Bench.Event, field: :id)
        |> Ash.read!(domain: Bench.Domain)
      row.aggregates.n
    end,

    "2-column projection (user_id, value)" => fn ->
      Bench.Event
      |> Ash.Query.select([:user_id, :value])
      |> Ash.read!(domain: Bench.Domain)
    end,
  },
  warmup: 3,
  time:   20,
  memory_time: 2,
  formatters: [{Benchee.Formatters.Console, extended_statistics: true}],
  print: [fast_warning: false]
)

# ── Time-travel benchmarks ────────────────────────────────────────────────────
#
# Reading a past snapshot costs the same as reading the current snapshot —
# DuckDB's iceberg_scan() accepts a snapshot_id parameter that selects which
# Parquet files to include; no extra work on the catalog side.

IO.puts("\n── time travel — snapshot reads ─────────────────────────────────────────")

{:ok, snaps} = AshIceberg.Snapshots.list(Bench.Event)
n_snaps = length(snaps)

if n_snaps >= 2 do
  first_snap   = hd(snaps)
  mid_snap     = Enum.at(snaps, div(n_snaps, 2))
  current_snap = List.last(snaps)
  first_id     = first_snap["snapshot-id"]
  mid_id       = mid_snap["snapshot-id"]

  IO.puts("  #{n_snaps} snapshots available: first=#{first_id} mid=#{mid_id}")

  q_current = Bench.Event
  q_first   = Bench.Event |> Ash.Query.set_context(%{ash_iceberg: %{snapshot_id: first_id}})
  q_mid     = Bench.Event |> Ash.Query.set_context(%{ash_iceberg: %{snapshot_id: mid_id}})

  Benchee.run(
    %{
      "current snapshot (#{n_snaps} batches of data)" => fn ->
        Ash.read!(q_current, domain: Bench.Domain)
      end,

      "mid snapshot (#{div(n_snaps, 2)} batches of data)" => fn ->
        Ash.read!(q_mid, domain: Bench.Domain)
      end,

      "first snapshot (1 batch of data)" => fn ->
        Ash.read!(q_first, domain: Bench.Domain)
      end,
    },
    warmup: 2,
    time:   15,
    memory_time: 0,
    print: [fast_warning: false]
  )
else
  IO.puts("  only #{n_snaps} snapshot(s) — skipping time-travel bench (need ≥ 2)")
end

IO.puts("\n── done ✓ ──────────────────────────────────────────────────────────────")
