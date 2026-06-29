defmodule Demo.Event do
  @moduledoc """
  An analytics event stored in an Iceberg table.

  Because Iceberg is an append-optimised format, UPDATE and DESTROY are
  disabled.  All writes go through the :create and :bulk_create actions.
  """

  use Ash.Resource,
    domain: Demo.Domain,
    data_layer: AshIceberg.DataLayer

  iceberg do
    catalog Demo.Catalog
    namespace "demo"
    table "events"
    # Iceberg V2 row-level deletes are supported but we keep the demo append-only
    # for simplicity and to highlight the format's strengths.
    can_update? false
    can_destroy? false
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
  end
end
