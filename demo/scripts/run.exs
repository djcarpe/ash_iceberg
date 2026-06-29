#!/usr/bin/env elixir
#
# run.exs — end-to-end demo + benchmarks for AshIceberg
#
# Executed inside the Demo Mix application (Demo.Application is already
# started, so Demo.Catalog / DuckDB connection is live).
#
# Sections:
#   1. Wait for the Iceberg REST catalog to become reachable
#   2. Create namespace + table (idempotent)
#   3. CRUD demo: single inserts, reads, filters
#   4. Benchmarks with Benchee
# ─────────────────────────────────────────────────────────────────────────────

require Logger

separator = fn label ->
  IO.puts("\n#{String.duplicate("─", 72)}")
  IO.puts("  #{label}")
  IO.puts(String.duplicate("─", 72))
end

# ─── 1. Wait for the Iceberg REST Catalog ─────────────────────────────────────
separator.("Waiting for Iceberg REST Catalog…")

catalog_uri = System.get_env("ICEBERG_REST_URI", "http://localhost:8181")

Stream.repeatedly(fn ->
  case Req.get("#{catalog_uri}/v1/config") do
    {:ok, %{status: 200}} ->
      IO.puts("  ✓ Catalog reachable at #{catalog_uri}")
      :ready

    _ ->
      IO.puts("  … not yet ready, retrying in 3s")
      Process.sleep(3_000)
      :wait
  end
end)
|> Stream.drop_while(&(&1 == :wait))
|> Enum.take(1)

# ─── 2. Table setup ───────────────────────────────────────────────────────────
separator.("Setting up Iceberg table")
Demo.setup_table!()

# ─── 3. CRUD Demo ─────────────────────────────────────────────────────────────
separator.("Ash CRUD Demo")

# Single create
IO.puts("\n[create] inserting one event…")
event = Demo.create_one!()
IO.puts("  → created #{event.id} | user=#{event.user_id} type=#{event.event_type} value=#{event.value}")

# Bulk create 500 rows (one Iceberg snapshot)
IO.puts("\n[bulk_create] inserting 500 events in one batch…")
t0 = System.monotonic_time(:millisecond)
Demo.bulk_create!(500)
elapsed = System.monotonic_time(:millisecond) - t0
IO.puts("  → done in #{elapsed}ms  (#{Float.round(500 / elapsed * 1000, 0)} rows/sec)")

# Another bulk for read variety
Demo.bulk_create!(500)
IO.puts("  → inserted another 500 events (total ≈ 1001)")

# Read all
IO.puts("\n[read] reading all events…")
t0 = System.monotonic_time(:millisecond)
all = Demo.read_all!()
elapsed = System.monotonic_time(:millisecond) - t0
IO.puts("  → #{length(all)} events in #{elapsed}ms")

# Filtered read
sample_uid = hd(all).user_id
IO.puts("\n[read :by_user] user_id=#{sample_uid}…")
t0 = System.monotonic_time(:millisecond)
user_events = Demo.read_by_user!(sample_uid)
elapsed = System.monotonic_time(:millisecond) - t0
IO.puts("  → #{length(user_events)} events in #{elapsed}ms")

# Type filter
IO.puts("\n[read :by_type] event_type='click'…")
t0 = System.monotonic_time(:millisecond)
clicks = Demo.read_by_type!("click")
elapsed = System.monotonic_time(:millisecond) - t0
IO.puts("  → #{length(clicks)} click events in #{elapsed}ms")

# ─── 4. Benchmarks ────────────────────────────────────────────────────────────
separator.("Benchmarks  (this takes ~60s)")

IO.puts("""

  ┌──────────────────────────────────────────────────────────────────┐
  │  Benchmark scenarios                                             │
  │                                                                  │
  │  single_create      — one Ash.create! → one Iceberg snapshot     │
  │  bulk_create_100    — 100 rows in one INSERT → one snapshot       │
  │  bulk_create_1000   — 1 000 rows in one INSERT → one snapshot    │
  │  read_all           — SELECT * (DuckDB scans all Parquet files)   │
  │  read_by_user       — WHERE user_id = ?  (predicate pushdown)    │
  │  read_by_type       — WHERE event_type = ?                       │
  └──────────────────────────────────────────────────────────────────┘

  NOTE: Iceberg write latency is dominated by S3 PUT + catalog commit.
  Batch writes amortise that cost dramatically.
""")

# Pre-pick a user_id that has events to ensure the filtered read returns rows
sample_for_bench = hd(Demo.read_all!()).user_id

Benchee.run(
  %{
    "single_create" => fn ->
      Demo.Event
      |> Ash.Changeset.for_create(:create, Demo.random_event())
      |> Ash.create!(domain: Demo.Domain)
    end,
    "bulk_create_100" => fn ->
      rows = Enum.map(1..100, fn _ -> Demo.random_event() end)

      Ash.bulk_create!(rows, Demo.Event, :create,
        domain: Demo.Domain,
        return_records?: false
      )
    end,
    "bulk_create_1000" => fn ->
      rows = Enum.map(1..1_000, fn _ -> Demo.random_event() end)

      Ash.bulk_create!(rows, Demo.Event, :create,
        domain: Demo.Domain,
        return_records?: false
      )
    end,
    "read_all" => fn ->
      Ash.read!(Demo.Event, domain: Demo.Domain)
    end,
    "read_by_user" => fn ->
      Demo.Event
      |> Ash.Query.for_read(:by_user, %{user_id: sample_for_bench})
      |> Ash.read!(domain: Demo.Domain)
    end,
    "read_by_type" => fn ->
      Demo.Event
      |> Ash.Query.for_read(:by_type, %{event_type: "click"})
      |> Ash.read!(domain: Demo.Domain)
    end
  },
  time: 10,
  warmup: 2,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console,
     extended_statistics: true,
     comparison: true}
  ],
  print: [
    benchmarking: true,
    configuration: true,
    fast_warning: false
  ]
)

separator.("Done!")
IO.puts("""

  MinIO Console  → http://localhost:9001  (minioadmin / minioadmin)
  Iceberg Catalog → http://localhost:8181/v1/namespaces

  Run again:  docker compose run --rm ash-demo
""")
