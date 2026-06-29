defmodule AshIceberg.DataLayer.Verifiers do
  @moduledoc false

  defmodule RequireCatalogOrWarehouse do
    @moduledoc false
    use Spark.Dsl.Verifier

    alias AshIceberg.DataLayer.Info

    @impl Spark.Dsl.Verifier
    def verify(dsl_state) do
      resource = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)
      catalog = Info.catalog(resource)
      warehouse = Info.warehouse(resource)

      cond do
        catalog != nil and warehouse != nil ->
          {:error,
           Spark.Error.DslError.exception(
             module: resource,
             path: [:iceberg],
             message: "supply either `catalog` or `warehouse`, not both"
           )}

        catalog == nil and warehouse == nil ->
          {:error,
           Spark.Error.DslError.exception(
             module: resource,
             path: [:iceberg],
             message: "either `catalog` or `warehouse` must be set"
           )}

        true ->
          :ok
      end
    end
  end
end
