FROM elixir:1.19

# ── system deps ──────────────────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git curl && \
    rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

# ── copy ash_iceberg library into /ash_iceberg ────────────────────────────────
# The demo's mix.exs points to ".." (i.e. /ash_iceberg) via a path dependency.
WORKDIR /ash_iceberg
COPY lib        ./lib
COPY mix.exs    ./mix.exs
COPY mix.lock   ./mix.lock

# ── copy the demo application into /ash_iceberg/demo ─────────────────────────
WORKDIR /ash_iceberg/demo
COPY demo/mix.exs   ./mix.exs
COPY demo/config    ./config
COPY demo/lib       ./lib
COPY demo/scripts   ./scripts

# ── fetch + compile (includes duckdbex NIF build) ────────────────────────────
RUN mix deps.get
RUN MIX_ENV=prod mix compile

# ── pre-install DuckDB Iceberg + httpfs extensions ───────────────────────────
# This step runs during docker build so the container starts without internet.
RUN mix run --no-halt -e '
  {:ok, db}   = Duckdbex.open()
  {:ok, conn} = Duckdbex.connection(db)
  {:ok, _}    = Duckdbex.query(conn, "INSTALL iceberg")
  {:ok, _}    = Duckdbex.query(conn, "INSTALL httpfs")
  IO.puts("DuckDB extensions pre-installed.")
'

CMD ["mix", "run", "scripts/run.exs"]
