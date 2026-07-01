defmodule Demo.Event do
  @moduledoc """
  An analytics event stored in an Iceberg table.

  Because Iceberg is an append-optimised format, UPDATE and DESTROY are
  disabled.  All writes go through the :create and :bulk_create actions.

  The table is partitioned by hour(occurred_at) so that DuckDB can skip
  entire Parquet files when filtering on time ranges.
  """

  use Ash.Resource,
    domain: Demo.Domain,
    data_layer: AshIceberg.DataLayer

  iceberg do
    catalog Demo.Catalog
    namespace "demo"
    table "events"
    # Iceberg V2 row-level deletes are supported but the demo stays append-only
    # to highlight the format's strengths.
    can_update? false
    can_destroy? false

    # Partition by hour of occurred_at — Iceberg will skip files that cannot
    # contain the queried time window.
    partition :occurred_at, transform: :hour
    # Distribute user_id writes across 16 buckets to avoid hot-spotting.
    partition :user_id, transform: {:bucket, 16}
  end

  attributes do
    uuid_primary_key :id

    attribute :user_id, :integer do
      allow_nil? false
    end

    attribute :event_type, :string do
      allow_nil? false
      constraints max_length: 64
    end

    attribute :value, :float, default: 0.0
    attribute :occurred_at, AshIceberg.Types.TimestampTz, default: &DateTime.utc_now/0
    attribute :metadata, :map, default: %{}
  end

  actions do
    defaults [:create, :read]

    read :by_user do
      argument :user_id, :integer, allow_nil?: false
      filter expr(user_id == ^arg(:user_id))
    end

    read :by_type do
      argument :event_type, :string, allow_nil?: false
      filter expr(event_type == ^arg(:event_type))
    end

    read :top_values do
      argument :limit, :integer, default: 10
      prepare build(sort: [value: :desc], limit: 10)
    end

    # Time-range filter — partition pruning kicks in here
    read :in_time_range do
      argument :from, AshIceberg.Types.TimestampTz, allow_nil?: false
      argument :to, AshIceberg.Types.TimestampTz, allow_nil?: false

      filter expr(occurred_at >= ^arg(:from) and occurred_at <= ^arg(:to))
    end

    # Retrieve events matching a prefix on event_type (uses LIKE pushdown)
    read :by_type_prefix do
      argument :prefix, :string, allow_nil?: false
      filter expr(string_starts_with(event_type, ^arg(:prefix)))
    end
  end
end
