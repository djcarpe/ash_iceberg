defmodule AshIceberg.Filter do
  @moduledoc """
  Translates an `Ash.Filter` expression tree into a SQL WHERE clause string
  suitable for DuckDB / Iceberg queries.

  Unsupported expression types return `{:error, reason}`.
  """

  @doc """
  Converts an `Ash.Filter` to a SQL string, or `nil` if the filter is nil/empty.

  Returns `{:ok, sql_string | nil}` or `{:error, reason}`.
  """
  def to_sql(nil), do: {:ok, nil}

  def to_sql(%Ash.Filter{expression: nil}), do: {:ok, nil}

  def to_sql(%Ash.Filter{expression: expr}) do
    expr_to_sql(expr)
  end

  # Boolean AND / OR
  defp expr_to_sql(%Ash.Query.BooleanExpression{op: op, left: left, right: right}) do
    with {:ok, left_sql} <- expr_to_sql(left),
         {:ok, right_sql} <- expr_to_sql(right) do
      op_str = if op == :and, do: "AND", else: "OR"
      {:ok, "(#{left_sql} #{op_str} #{right_sql})"}
    end
  end

  # NOT
  defp expr_to_sql(%Ash.Query.Not{expression: inner}) do
    with {:ok, inner_sql} <- expr_to_sql(inner) do
      {:ok, "NOT (#{inner_sql})"}
    end
  end

  # Eq
  defp expr_to_sql(%Ash.Query.Operator.Eq{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left),
         {:ok, r} <- ref_or_value(right) do
      if r == "NULL", do: {:ok, "#{l} IS NULL"}, else: {:ok, "#{l} = #{r}"}
    end
  end

  # NotEq
  defp expr_to_sql(%Ash.Query.Operator.NotEq{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left),
         {:ok, r} <- ref_or_value(right) do
      if r == "NULL", do: {:ok, "#{l} IS NOT NULL"}, else: {:ok, "#{l} != #{r}"}
    end
  end

  # LessThan
  defp expr_to_sql(%Ash.Query.Operator.LessThan{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left),
         {:ok, r} <- ref_or_value(right) do
      {:ok, "#{l} < #{r}"}
    end
  end

  # GreaterThan
  defp expr_to_sql(%Ash.Query.Operator.GreaterThan{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left),
         {:ok, r} <- ref_or_value(right) do
      {:ok, "#{l} > #{r}"}
    end
  end

  # LessThanOrEqual
  defp expr_to_sql(%Ash.Query.Operator.LessThanOrEqual{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left),
         {:ok, r} <- ref_or_value(right) do
      {:ok, "#{l} <= #{r}"}
    end
  end

  # GreaterThanOrEqual
  defp expr_to_sql(%Ash.Query.Operator.GreaterThanOrEqual{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left),
         {:ok, r} <- ref_or_value(right) do
      {:ok, "#{l} >= #{r}"}
    end
  end

  # In
  defp expr_to_sql(%Ash.Query.Operator.In{left: left, right: right}) do
    with {:ok, l} <- ref_or_value(left) do
      values =
        right
        |> Enum.map(&format_value/1)
        |> Enum.join(", ")

      {:ok, "#{l} IN (#{values})"}
    end
  end

  # IsNil
  defp expr_to_sql(%Ash.Query.Operator.IsNil{left: left, right: true}) do
    with {:ok, l} <- ref_or_value(left) do
      {:ok, "#{l} IS NULL"}
    end
  end

  defp expr_to_sql(%Ash.Query.Operator.IsNil{left: left, right: false}) do
    with {:ok, l} <- ref_or_value(left) do
      {:ok, "#{l} IS NOT NULL"}
    end
  end

  # Substring (LIKE)
  defp expr_to_sql(%Ash.Query.Function.Contains{arguments: [left, right]}) do
    with {:ok, l} <- ref_or_value(left) do
      term = format_like_string(right)
      {:ok, "#{l} LIKE #{term}"}
    end
  end

  defp expr_to_sql(unknown) do
    {:error, "Unsupported filter expression: #{inspect(unknown)}"}
  end

  # Ref → quoted column name
  defp ref_or_value(%Ash.Query.Ref{attribute: %{name: name}}) do
    {:ok, quote_ident(to_string(name))}
  end

  defp ref_or_value(value), do: {:ok, format_value(value)}

  defp format_value(nil), do: "NULL"
  defp format_value(true), do: "true"
  defp format_value(false), do: "false"
  defp format_value(value) when is_integer(value), do: to_string(value)
  defp format_value(value) when is_float(value), do: to_string(value)
  defp format_value(%Decimal{} = d), do: Decimal.to_string(d, :normal)

  defp format_value(%DateTime{} = dt),
    do: "'#{DateTime.to_iso8601(dt)}'"

  defp format_value(%Date{} = d), do: "'#{Date.to_iso8601(d)}'"
  defp format_value(%Time{} = t), do: "'#{Time.to_iso8601(t)}'"

  defp format_value(value) when is_binary(value), do: "'#{escape_string(value)}'"

  defp format_value(value) when is_atom(value), do: "'#{escape_string(to_string(value))}'"

  defp format_value(value) when is_list(value) do
    items = Enum.map_join(value, ", ", &format_value/1)
    "[#{items}]"
  end

  defp format_value(value), do: "'#{escape_string(inspect(value))}'"

  defp format_like_string(value) when is_binary(value) do
    escaped = String.replace(value, "'", "''")
    "'%#{escaped}%'"
  end

  defp format_like_string(%Ash.Query.Ref{attribute: %{name: name}}), do: quote_ident(to_string(name))

  defp escape_string(s) do
    s
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "''")
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
end
