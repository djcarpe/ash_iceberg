# ──────────────────────────────────────────────────────────────────────────────
# seed.exs — bulk-load synthetic events into the Iceberg table.
#
# All data is generated *inside* DuckDB (range() + hash() + random()) and
# inserted straight into the attached Iceberg catalog table, so Elixir never
# materialises rows. One INSERT per batch = one Iceberg snapshot per batch.
#
# Rows are generated in occurred_at order, so every Parquet data file covers a
# narrow time slice — file-level min/max stats then let readers skip most
# files on time-range queries.
#
# Environment:
#   SEED_ROWS     total rows              (default 1_000_000_000)
#   SEED_BATCH    rows per INSERT         (default 10_000_000)
#   SEED_DAYS     time window in days     (default 365)
#   SEED_START    ISO8601 window start    (default 2025-07-09T00:00:00Z)
#   SEED_PAUSE_MS pause between batches   (default 2000) — keeps sustained IO
#                 below what the single-host hypervisor tolerates
#
# Restart-safe: on startup it counts existing rows and resumes from there.
# ──────────────────────────────────────────────────────────────────────────────

defmodule Seed do
  @table "demo_catalog.demo.events"
  @event_types ~w[view click purchase share bookmark login logout search]

  def run do
    rows = env_int("SEED_ROWS", 1_000_000_000)
    batch = env_int("SEED_BATCH", 10_000_000)
    days = env_int("SEED_DAYS", 365)
    pause_ms = env_int("SEED_PAUSE_MS", 2_000)
    start_iso = System.get_env("SEED_START", "2025-07-09T00:00:00Z")

    {:ok, start_dt, 0} = DateTime.from_iso8601(start_iso)
    epoch_start = DateTime.to_unix(start_dt)
    window_seconds = days * 86_400
    step = window_seconds / rows

    IO.puts("Seeding #{fmt(rows)} rows in batches of #{fmt(batch)} (#{days}-day window)")

    wait_for_table()
    tune_duckdb()

    done = current_count()
    IO.puts("Existing rows: #{fmt(done)}")

    if done >= rows do
      IO.puts("Already fully seeded — nothing to do.")
    else
      seed_from(done, rows, batch, epoch_start, step, pause_ms)
    end
  end

  defp seed_from(done, rows, batch, epoch_start, step, pause_ms) do
    t0 = System.monotonic_time(:millisecond)
    total_batches = ceil((rows - done) / batch)

    done
    |> Stream.unfold(fn
      from when from >= rows -> nil
      from -> {{from, min(from + batch, rows)}, min(from + batch, rows)}
    end)
    |> Stream.with_index(1)
    |> Enum.each(fn {{from, to}, n} ->
      bt0 = System.monotonic_time(:millisecond)
      insert_batch(from, to, epoch_start, step)
      bt = System.monotonic_time(:millisecond) - bt0
      elapsed = System.monotonic_time(:millisecond) - t0
      rate = (to - done) * 1000 / max(elapsed, 1)
      eta_min = (rows - to) / max(rate, 1) / 60

      IO.puts(
        "batch #{n}/#{total_batches}  rows #{fmt(from)}→#{fmt(to)}  " <>
          "#{Float.round(bt / 1000, 1)}s  avg #{fmt(round(rate))} rows/s  ETA #{Float.round(eta_min, 1)} min"
      )

      Process.sleep(pause_ms)
    end)

    IO.puts("Seed complete: #{fmt(current_count())} rows.")
  end

  defp insert_batch(from, to, epoch_start, step) do
    types_sql = "['" <> Enum.join(@event_types, "','") <> "']"

    sql = """
    INSERT INTO #{@table} (id, user_id, event_type, value, occurred_at, metadata)
    SELECT
      i                                                          AS id,
      CAST((i * 2654435761) % 10000000 AS INTEGER)               AS user_id,
      (#{types_sql})[1 + CAST(hash(i) % #{length(@event_types)} AS INTEGER)] AS event_type,
      round(random() * 500, 2)                                   AS value,
      to_timestamp(#{epoch_start} + i * #{step})                 AS occurred_at,
      NULL                                                       AS metadata
    FROM range(#{from}, #{to}) t(i)
    """

    case AshIceberg.Connection.query(Demo.Catalog, sql) do
      {:ok, _} -> :ok
      {:error, reason} -> raise "INSERT failed: #{inspect(reason)}"
    end
  end

  # Connection.query returns rows already fetched as a list of maps.
  defp current_count do
    case AshIceberg.Connection.query(Demo.Catalog, "SELECT count(*) AS n FROM #{@table}") do
      {:ok, [%{"n" => n}]} when is_integer(n) -> n
      {:ok, [row]} when is_map(row) -> row |> Map.values() |> List.first() || 0
      _ -> 0
    end
  end

  defp tune_duckdb do
    for pragma <- ["SET memory_limit='4GB'", "SET threads=2", "SET preserve_insertion_order=false"] do
      AshIceberg.Connection.query(Demo.Catalog, pragma)
    end
  end

  defp wait_for_table(attempt \\ 1) do
    Demo.setup_table!()
  rescue
    e ->
      if attempt >= 120 do
        reraise e, __STACKTRACE__
      else
        IO.puts("Catalog not ready (attempt #{attempt}), retrying in 5s...")
        Process.sleep(5_000)
        wait_for_table(attempt + 1)
      end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      v -> String.to_integer(v)
    end
  end

  defp fmt(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.replace(~r/(\d)(?=(\d{3})+$)/, "\\1_")
  end
end

Seed.run()
