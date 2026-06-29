defmodule AshIceberg.Types.Fixed do
  @moduledoc """
  Ash type for the Iceberg `fixed[n]` (fixed-length byte array) type.

  Stored as a binary in Elixir. The maximum byte length `n` is enforced
  via the `:length` constraint.

  ## Usage

      attribute :checksum, AshIceberg.Types.Fixed,
        constraints: [length: 16]
  """

  use Ash.Type

  @impl Ash.Type
  def storage_type(_constraints), do: :binary

  @impl Ash.Type
  def constraints do
    [
      length: [
        type: :pos_integer,
        doc: "Fixed byte length for the Iceberg fixed[n] type.",
        required: true
      ]
    ]
  end

  @impl Ash.Type
  def apply_constraints(value, constraints) do
    n = constraints[:length]

    if is_nil(n) or byte_size(value) == n do
      {:ok, value}
    else
      {:error, "expected exactly #{n} bytes, got #{byte_size(value)}"}
    end
  end

  @impl Ash.Type
  def cast_input(nil, _), do: {:ok, nil}

  def cast_input(value, _) when is_binary(value), do: {:ok, value}

  def cast_input(value, _) when is_list(value) do
    {:ok, :erlang.list_to_binary(value)}
  end

  def cast_input(value, _) do
    {:error, "cannot cast #{inspect(value)} to Fixed bytes"}
  end

  @impl Ash.Type
  def cast_stored(nil, _), do: {:ok, nil}
  def cast_stored(value, _) when is_binary(value), do: {:ok, value}
  def cast_stored(value, _), do: {:error, "cannot cast #{inspect(value)} from storage"}

  @impl Ash.Type
  def dump_to_native(nil, _), do: {:ok, nil}
  def dump_to_native(value, _) when is_binary(value), do: {:ok, value}
  def dump_to_native(value, _), do: {:error, "cannot dump #{inspect(value)} to native"}
end
