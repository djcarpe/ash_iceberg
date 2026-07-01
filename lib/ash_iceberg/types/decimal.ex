defmodule AshIceberg.Types.Decimal do
  @moduledoc """
  Ash type for the Iceberg `decimal(P, S)` (fixed-precision decimal) type.

  Backed by `Decimal` on the Elixir side.  Precision and scale are specified
  via constraints and are used when generating the Iceberg schema (e.g. via
  `mix ash_iceberg.create_table`).

  ## Usage

      attribute :price, AshIceberg.Types.Decimal do
        constraints precision: 18, scale: 4
      end
  """

  use Ash.Type

  @impl Ash.Type
  def storage_type(_constraints), do: :decimal

  @impl Ash.Type
  def constraints do
    [
      precision: [
        type: :pos_integer,
        doc: "Total number of digits (1–38). Default: 18.",
        default: 18
      ],
      scale: [
        type: :non_neg_integer,
        doc: "Digits after the decimal point. Default: 6.",
        default: 6
      ]
    ]
  end

  @impl Ash.Type
  def apply_constraints(value, constraints) do
    precision = constraints[:precision] || 18
    scale = constraints[:scale] || 6

    max_val = Decimal.new(Integer.pow(10, precision - scale))

    cond do
      Decimal.compare(Decimal.abs(value), max_val) == :gt ->
        {:error,
         message: "value exceeds precision #{precision}/scale #{scale}",
         value: value}

      true ->
        {:ok, Decimal.round(value, scale)}
    end
  end

  @impl Ash.Type
  def cast_input(nil, _), do: {:ok, nil}

  def cast_input(%Decimal{} = d, _), do: {:ok, d}

  def cast_input(value, _) when is_float(value) do
    {:ok, Decimal.from_float(value)}
  end

  def cast_input(value, _) when is_integer(value) do
    {:ok, Decimal.new(value)}
  end

  def cast_input(value, _) when is_binary(value) do
    case Decimal.parse(value) do
      {d, ""} -> {:ok, d}
      _ -> {:error, "cannot parse #{inspect(value)} as Decimal"}
    end
  end

  def cast_input(value, _),
    do: {:error, "cannot cast #{inspect(value)} to Decimal"}

  @impl Ash.Type
  def cast_stored(nil, _), do: {:ok, nil}
  def cast_stored(%Decimal{} = d, _), do: {:ok, d}

  def cast_stored(value, constraints) when is_binary(value) or is_integer(value) or is_float(value) do
    cast_input(value, constraints)
  end

  def cast_stored(value, _),
    do: {:error, "cannot cast #{inspect(value)} from storage"}

  @impl Ash.Type
  def dump_to_native(nil, _), do: {:ok, nil}
  def dump_to_native(%Decimal{} = d, _), do: {:ok, d}

  def dump_to_native(value, _),
    do: {:error, "cannot dump #{inspect(value)} to native"}

  @impl Ash.Type
  def equal?(%Decimal{} = a, %Decimal{} = b), do: Decimal.equal?(a, b)
  def equal?(_, _), do: false

  @doc """
  Returns the Iceberg type string for a given precision/scale pair.
  Used by `mix ash_iceberg.create_table`.
  """
  def iceberg_type(precision \\ 18, scale \\ 6), do: "decimal(#{precision}, #{scale})"
end
