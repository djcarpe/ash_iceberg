defmodule AshIceberg.Partition do
  @moduledoc """
  Represents one field in an Iceberg partition spec.

  Built via the `partition` entity in the `iceberg` DSL block:

      iceberg do
        catalog MyCatalog
        namespace "analytics"
        table "events"

        partition :occurred_at, transform: :hour
        partition :user_id,     transform: {:bucket, 16}
        partition :region                              # defaults to :identity
      end

  ## Transforms

  | Value | Iceberg transform | Description |
  |-------|-------------------|-------------|
  | `:identity` | identity | Exact-match partitioning (default) |
  | `:year` | year | Partition by year of a date/timestamp |
  | `:month` | month | Partition by month |
  | `:day` | day | Partition by day |
  | `:hour` | hour | Partition by hour |
  | `{:bucket, n}` | bucket[n] | Hash `field` into `n` buckets |
  | `{:truncate, n}` | truncate[n] | Truncate string/int to `n` chars/digits |
  """

  defstruct [:field, :__spark_metadata__, transform: :identity]

  @type transform ::
          :identity
          | :year
          | :month
          | :day
          | :hour
          | {:bucket, pos_integer()}
          | {:truncate, pos_integer()}

  @type t :: %__MODULE__{
          field: atom(),
          transform: transform()
        }

  @doc "Converts the transform to its Iceberg REST spec representation."
  def to_iceberg_spec(%__MODULE__{field: field, transform: transform}) do
    %{
      "source-id" => to_string(field),
      "name" => partition_name(field, transform),
      "transform" => transform_string(transform)
    }
  end

  defp partition_name(field, :identity), do: to_string(field)
  defp partition_name(field, :year), do: "#{field}_year"
  defp partition_name(field, :month), do: "#{field}_month"
  defp partition_name(field, :day), do: "#{field}_day"
  defp partition_name(field, :hour), do: "#{field}_hour"
  defp partition_name(field, {:bucket, n}), do: "#{field}_bucket_#{n}"
  defp partition_name(field, {:truncate, n}), do: "#{field}_trunc_#{n}"

  defp transform_string(:identity), do: "identity"
  defp transform_string(:year), do: "year"
  defp transform_string(:month), do: "month"
  defp transform_string(:day), do: "day"
  defp transform_string(:hour), do: "hour"
  defp transform_string({:bucket, n}), do: "bucket[#{n}]"
  defp transform_string({:truncate, n}), do: "truncate[#{n}]"
end
