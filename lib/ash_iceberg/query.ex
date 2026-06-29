defmodule AshIceberg.Query do
  @moduledoc """
  Accumulator struct for an in-flight Ash query against an Iceberg table.

  Built up by the `Ash.DataLayer` callbacks and converted to SQL by
  `AshIceberg.QueryBuilder` when `run_query/2` is called.
  """

  defstruct [
    # Ash context
    :resource,
    :domain,
    :tenant,
    # Iceberg target
    :catalog,
    :namespace,
    :table,
    :warehouse,
    # Query clauses
    select: [],
    filter: nil,
    sort: [],
    limit: nil,
    offset: nil,
    aggregates: [],
    calculations: [],
    distinct: false
  ]

  @type t :: %__MODULE__{
          resource: Ash.Resource.t(),
          domain: Ash.Domain.t() | nil,
          tenant: term(),
          catalog: module() | nil,
          namespace: String.t(),
          table: String.t(),
          warehouse: String.t() | nil,
          select: [atom()],
          filter: Ash.Filter.t() | nil,
          sort: [{atom(), :asc | :desc}],
          limit: pos_integer() | nil,
          offset: non_neg_integer() | nil,
          aggregates: [Ash.Query.Aggregate.t()],
          calculations: [Ash.Query.Calculation.t()],
          distinct: boolean()
        }
end
