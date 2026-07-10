# otp-27 pinned: the vendored duckdbex NIF tarball targets NIF 2.17.
FROM elixir:1.19-otp-27

# ── system deps ──────────────────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git curl && \
    rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV=prod

# ── copy ash_iceberg library into /ash_iceberg ────────────────────────────────
# The demo's mix.exs points to ".." (i.e. /ash_iceberg) via a path dependency.
WORKDIR /ash_iceberg
COPY lib        ./lib
COPY mix.exs    ./mix.exs
COPY mix.lock   ./mix.lock

# ── copy the demo application into /ash_iceberg/demo ─────────────────────────
WORKDIR /ash_iceberg/demo
COPY demo/mix.exs   ./mix.exs
COPY demo/mix.lock  ./mix.lock
COPY demo/config    ./config
COPY demo/lib       ./lib

# ── fetch + compile (includes duckdbex NIF build) ────────────────────────────
# Pre-seed the elixir_make cache so the duckdbex NIF is not downloaded from
# GitHub's CDN at build time (checksum-verified against the hex package).
COPY demo/nif-cache/ /root/.cache/elixir_make/
RUN mix deps.get
RUN mix compile

# ── pre-install DuckDB Iceberg + httpfs extensions ───────────────────────────
# This step runs during docker build so the container starts without internet.
RUN mix run -e '{:ok, db} = Duckdbex.open(); {:ok, conn} = Duckdbex.connection(db); {:ok, _} = Duckdbex.query(conn, "INSTALL iceberg"); {:ok, _} = Duckdbex.query(conn, "INSTALL httpfs"); IO.puts("DuckDB extensions pre-installed.")'

# Scripts are interpreted at runtime — copied last so editing them never
# invalidates the compile layers above.
COPY demo/scripts   ./scripts

EXPOSE 4000

# Serves /gql and /gql/playground. Seed jobs override this with:
#   mix run scripts/seed.exs
CMD ["mix", "run", "--no-halt"]
