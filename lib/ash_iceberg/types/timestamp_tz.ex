defmodule AshIceberg.Types.TimestampTz do
  @moduledoc """
  Ash type for the Iceberg `timestamptz` (timestamp with time zone) type.

  Stored as microseconds-precision UTC in Iceberg; Elixir side is `DateTime`.

  ## Usage

      attribute :occurred_at, AshIceberg.Types.TimestampTz
  """

  use Ash.Type

  @impl Ash.Type
  def storage_type(_constraints), do: :utc_datetime_usec

  @impl Ash.Type
  def cast_input(nil, _), do: {:ok, nil}

  def cast_input(%DateTime{} = dt, _), do: {:ok, DateTime.truncate(dt, :microsecond)}

  def cast_input(value, _) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _} -> {:ok, DateTime.truncate(dt, :microsecond)}
      {:error, _} -> {:error, "cannot parse #{inspect(value)} as a datetime"}
    end
  end

  def cast_input(value, _) when is_integer(value) do
    case DateTime.from_unix(value, :microsecond) do
      {:ok, dt} -> {:ok, dt}
      {:error, _} -> {:error, "cannot convert integer #{value} to datetime"}
    end
  end

  def cast_input(value, _), do: {:error, "cannot cast #{inspect(value)} to TimestampTz"}

  @impl Ash.Type
  def cast_stored(nil, _), do: {:ok, nil}

  def cast_stored(%DateTime{} = dt, _), do: {:ok, dt}

  def cast_stored(value, constraints) when is_binary(value) do
    cast_input(value, constraints)
  end

  def cast_stored(value, constraints) when is_integer(value) do
    cast_input(value, constraints)
  end

  def cast_stored(value, _), do: {:error, "cannot cast #{inspect(value)} from storage"}

  @impl Ash.Type
  def dump_to_native(nil, _), do: {:ok, nil}

  def dump_to_native(%DateTime{} = dt, _) do
    {:ok, DateTime.to_iso8601(DateTime.truncate(dt, :microsecond))}
  end

  def dump_to_native(value, _), do: {:error, "cannot dump #{inspect(value)} to native"}

  @impl Ash.Type
  def equal?(a, b), do: DateTime.compare(a, b) == :eq
end
