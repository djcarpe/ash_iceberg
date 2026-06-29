defmodule Demo.Catalog do
  @moduledoc """
  Iceberg catalog for the demo application.

  All settings are read from environment variables so the same Docker image
  can point at different catalog endpoints without rebuilding.
  """

  use AshIceberg.Catalog, otp_app: :demo

  @impl AshIceberg.Catalog
  def config do
    %{
      type: :rest,
      uri: env("ICEBERG_REST_URI", "http://localhost:8181"),
      warehouse: env("S3_WAREHOUSE", "s3://warehouse/"),
      aws_region: env("S3_REGION", "us-east-1"),
      aws_access_key_id: env("S3_ACCESS_KEY", "minioadmin"),
      aws_secret_access_key: env("S3_SECRET_KEY", "minioadmin"),
      # MinIO requires path-style URLs and plain HTTP
      s3_endpoint: env("S3_ENDPOINT", "http://localhost:9000"),
      s3_url_style: "path",
      s3_use_ssl: false,
      # This name is used for the DuckDB ATTACH alias AND table references.
      catalog_name: "demo_catalog"
    }
  end

  defp env(key, default), do: System.get_env(key, default)
end
