defmodule AshIceberg.Query do
  @moduledoc """
  Accumulator struct for an in-flight Ash query against an Iceberg table.

  Built up by the `Ash.DataLayer` callbacks and converted to SQL by
  `AshIceberg.QueryBuilder` when `run_query/2` is called.

  ## Time travel

  Iceberg tables keep an immutable snapshot history. Pass time-travel
  parameters through the Ash query context to read data as of a past point:

      MyResource
      |> Ash.Query.set_context(%{ash_iceberg: %{snapshot_id: 1_234_567_890}})
      |> Ash.read!()

      # or by wall-clock time
      MyResource
      |> Ash.Query.set_context(%{ash_iceberg: %{as_of: ~U[2025-01-01 00:00:00Z]}})
      |> Ash.read!()
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
    # Routing: the GenServer to execute queries against when catalog is nil
    # (used when a catalog's DuckDB ATTACH failed and we fall back to iceberg_scan)
    :connection,
    # Time travel (Iceberg snapshot access)
    :snapshot_id,
    :as_of,
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
          connection: module() | nil,
          snapshot_id: non_neg_integer() | nil,
          as_of: DateTime.t() | nil,
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
