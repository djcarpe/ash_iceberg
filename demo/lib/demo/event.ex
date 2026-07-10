defmodule Demo.Event do
  @moduledoc """
  An analytics event stored in an Iceberg table.

  Because Iceberg is an append-optimised format, UPDATE and DESTROY are
  disabled.  All writes go through the :create and :bulk_create actions.

  The primary key is a sequential BIGINT rather than a UUID string so that a
  billion-row table stays compact in Parquet (delta-encoded integers instead
  of 36-byte random strings).
  """

  use Ash.Resource,
    domain: Demo.Domain,
    data_layer: AshIceberg.DataLayer,
    extensions: [AshGraphql.Resource]

  iceberg do
    catalog Demo.Catalog
    namespace "demo"
    table "events"
    # Iceberg V2 row-level deletes are supported but the demo stays append-only
    # to highlight the format's strengths.
    can_update? false
    can_destroy? false
  end

  graphql do
    type :event

    # occurred_at is a custom Ash type; expose it as the standard DateTime scalar.
    attribute_types occurred_at: :datetime

    queries do
      get :event, :read
      list :events, :sample
      list :events_by_user, :by_user
      list :events_by_type, :by_type
      list :events_in_range, :in_time_range
      list :top_events, :top_values
      list :events_by_type_prefix, :by_type_prefix
    end
  end

  attributes do
    attribute :id, :integer do
      primary_key? true
      allow_nil? false
      writable? true
      public? true
    end

    attribute :user_id, :integer do
      allow_nil? false
      public? true
    end

    attribute :event_type, :string do
      allow_nil? false
      constraints max_length: 64
      public? true
    end

    attribute :value, :float, default: 0.0, public?: true
    attribute :occurred_at, AshIceberg.Types.TimestampTz, default: &DateTime.utc_now/0, public?: true
    attribute :metadata, :map, default: %{}, public?: true
  end

  actions do
    defaults [:create, :read]

    # All list actions use keyset (cursor) pagination with an on-demand
    # count(*) — GraphQL exposes first/after/before/last args plus a `count`
    # field on the page.
    read :sample do
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :by_user do
      argument :user_id, :integer, allow_nil?: false
      filter expr(user_id == ^arg(:user_id))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :by_type do
      argument :event_type, :string, allow_nil?: false
      filter expr(event_type == ^arg(:event_type))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    read :top_values do
      argument :limit, :integer, default: 10
      prepare build(sort: [value: :desc], limit: 10)
    end

    # Time-range filter — file-level min/max stats let DuckDB skip whole
    # Parquet files outside the queried window.
    read :in_time_range do
      argument :from, :utc_datetime, allow_nil?: false
      argument :to, :utc_datetime, allow_nil?: false
      filter expr(occurred_at >= ^arg(:from) and occurred_at <= ^arg(:to))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end

    # Retrieve events matching a prefix on event_type (uses LIKE pushdown)
    read :by_type_prefix do
      argument :prefix, :string, allow_nil?: false
      filter expr(string_starts_with(event_type, ^arg(:prefix)))
      pagination keyset?: true, countable: true, default_limit: 50, max_page_size: 1000
    end
  end
end
