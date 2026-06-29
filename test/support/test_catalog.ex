defmodule AshIceberg.Test.Catalog do
  @moduledoc false
  use AshIceberg.Catalog, otp_app: :ash_iceberg

  @impl AshIceberg.Catalog
  def config do
    %{
      type: :filesystem,
      path: System.tmp_dir!() |> Path.join("ash_iceberg_test_warehouse"),
      catalog_name: "test_iceberg"
    }
  end
end
