# AshIceberg

An [Apache Iceberg](https://iceberg.apache.org) data layer for the [Ash Framework](https://ash-hq.org). Query and manage Iceberg tables through DuckDB (with the Iceberg extension) backed by an Iceberg REST Catalog or a filesystem catalog.

```
Ash.Query / Ash.create / Ash.bulk_create
        │
        ▼
AshIceberg.DataLayer
        │
        ├── AshIceberg.Catalog   ──▶  REST Catalog (or filesystem)
        │   (table management)         table metadata / schema
        │
        └── DuckDB (duckdbex)    ──▶  Iceberg table files (S3 / local)
            with Iceberg extension      Parquet scan + predicate pushdown
```

## Features

- Full Ash data layer: read, create, update, destroy, bulk_create
- Filter and sort pushdown via DuckDB's Iceberg extension
- REST Catalog integration (Nessie, Polaris, Tabular, etc.) and filesystem fallback
- Configurable per-resource catalog, namespace, and table name
- Docker Compose setup included for local development

## Getting Started

### Requirements

- Elixir ~> 1.14
- DuckDB with the Iceberg extension (installed automatically via `duckdbex`)
- An Iceberg REST Catalog **or** a local filesystem catalog

### Install

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:ash_iceberg, "~> 0.1"},
    {:ash, "~> 3.0"},
    {:duckdbex, "~> 0.3"},
    {:req, "~> 0.5"},
    {:jason, "~> 1.4"},
    {:decimal, "~> 2.0"}
  ]
end
```

Then run:

```bash
mix deps.get
```

### Start a local Iceberg catalog (optional)

A Docker Compose file is included for running an Iceberg REST Catalog locally:

```bash
docker-compose up
```

This starts an Iceberg REST Catalog on `http://localhost:8181`.

### Define a catalog

```elixir
defmodule MyApp.IcebergCatalog do
  use AshIceberg.Catalog

  catalog do
    type :rest
    uri "http://localhost:8181"
    warehouse "s3://my-bucket/warehouse"
  end
end
```

For a local filesystem catalog:

```elixir
defmodule MyApp.LocalCatalog do
  use AshIceberg.Catalog

  catalog do
    type :filesystem
    warehouse "/var/data/iceberg-warehouse"
  end
end
```

### Define a resource

```elixir
defmodule MyApp.Analytics.Event do
  use Ash.Resource,
    domain: MyApp.Analytics,
    data_layer: AshIceberg.DataLayer

  iceberg do
    catalog MyApp.IcebergCatalog
    namespace "analytics"
    table "events"
  end

  attributes do
    uuid_primary_key :id
    attribute :user_id,    :string,         allow_nil?: false
    attribute :event_type, :string,         allow_nil?: false
    attribute :occurred_at, :utc_datetime,  allow_nil?: false
    attribute :payload,    :map
  end

  actions do
    defaults [:read, :create, :destroy]
  end
end
```

### Use it

```elixir
# Create records
Ash.create!(MyApp.Analytics.Event, %{
  user_id: "u_123",
  event_type: "page_view",
  occurred_at: DateTime.utc_now()
})

# Bulk ingest
Ash.bulk_create!(MyApp.Analytics.Event, events, :create)

# Filter and sort via DuckDB predicate pushdown
MyApp.Analytics.Event
|> Ash.Query.filter(event_type == "purchase" and occurred_at > ^cutoff)
|> Ash.Query.sort(occurred_at: :desc)
|> Ash.Query.limit(100)
|> Ash.read!()
```

### Run the demo

The `demo/` directory contains a runnable benchmark and CRUD walkthrough. It requires the local Iceberg REST Catalog (see above).

```bash
cd demo
mix deps.get
mix run -e "Demo.run()"
```

## Project structure

```
lib/ash_iceberg/
├── catalog/          # REST and filesystem catalog clients
├── data_layer/       # Ash data layer implementation
├── connection.ex     # DuckDB connection management
├── filter.ex         # Ash filter → DuckDB SQL translation
├── query_builder.ex  # Query construction with predicate pushdown
└── types/            # Custom Ash types (TimestampTz, Fixed)

demo/                 # Example app with benchmarks and CRUD demos
docker-compose.yml    # Local Iceberg REST Catalog for development
```

## License

MIT
