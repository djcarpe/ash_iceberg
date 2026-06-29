defmodule AshIceberg.Catalog.RestClient do
  @moduledoc """
  HTTP client for the Iceberg REST Catalog API (v1).

  Reference: https://iceberg.apache.org/docs/latest/api/rest/
  """

  @doc "Load table metadata from the REST catalog."
  @spec load_table(map(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def load_table(config, namespace, table) do
    get(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables/#{table}")
  end

  @doc "List all tables in a namespace."
  @spec list_tables(map(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_tables(config, namespace) do
    case get(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables") do
      {:ok, %{"identifiers" => ids}} ->
        {:ok, Enum.map(ids, & &1["name"])}

      other ->
        other
    end
  end

  @doc "Create a new Iceberg table."
  @spec create_table(map(), String.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def create_table(config, namespace, table, schema) do
    body = %{
      "name" => table,
      "schema" => schema,
      "write-order" => %{"order-id" => 0, "fields" => []}
    }

    post(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables", body)
  end

  @doc """
  Commit a set of metadata updates to an existing table (e.g. append snapshot).

  `requirements` and `updates` follow the Iceberg REST commit spec.
  """
  @spec commit_table(map(), String.t(), String.t(), [map()], [map()]) ::
          {:ok, map()} | {:error, term()}
  def commit_table(config, namespace, table, requirements, updates) do
    body = %{"requirements" => requirements, "updates" => updates}
    post(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables/#{table}", body)
  end

  @doc "Drop an Iceberg table."
  @spec drop_table(map(), String.t(), String.t()) :: :ok | {:error, term()}
  def drop_table(config, namespace, table) do
    delete(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables/#{table}")
  end

  @doc "Create a namespace (must not already exist)."
  @spec create_namespace(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def create_namespace(config, namespace) do
    parts = String.split(namespace, ".")
    body = %{"namespace" => parts, "properties" => %{}}
    post(config, "/v1/namespaces", body)
  end

  @doc "Create a namespace if it does not already exist."
  @spec ensure_namespace(map(), String.t()) :: :ok | {:error, term()}
  def ensure_namespace(config, namespace) do
    case create_namespace(config, namespace) do
      {:ok, _} -> :ok
      {:error, {409, _}} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc "List all namespaces (optionally nested under a parent)."
  @spec list_namespaces(map(), String.t() | nil) :: {:ok, [String.t()]} | {:error, term()}
  def list_namespaces(config, parent \\ nil) do
    path =
      if parent,
        do: "/v1/namespaces?parent=#{URI.encode(parent)}",
        else: "/v1/namespaces"

    case get(config, path) do
      {:ok, %{"namespaces" => ns}} ->
        {:ok, Enum.map(ns, &List.last/1)}

      other ->
        other
    end
  end

  # ---------------------------------------------------------------------------
  # HTTP helpers
  # ---------------------------------------------------------------------------

  defp get(config, path) do
    url = "#{config.uri}#{path}"

    case Req.get(url, headers: auth_headers(config)) do
      {:ok, %{status: 200, body: body}} -> {:ok, body}
      {:ok, %{status: 404}} -> {:error, :not_found}
      {:ok, %{status: status, body: body}} -> {:error, {status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp post(config, path, body) do
    url = "#{config.uri}#{path}"

    case Req.post(url, json: body, headers: auth_headers(config)) do
      {:ok, %{status: s, body: resp}} when s in [200, 201] -> {:ok, resp}
      {:ok, %{status: status, body: body}} -> {:error, {status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete(config, path) do
    url = "#{config.uri}#{path}"

    case Req.delete(url, headers: auth_headers(config)) do
      {:ok, %{status: s}} when s in [200, 204] -> :ok
      {:ok, %{status: status, body: body}} -> {:error, {status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp auth_headers(%{token: token}) when is_binary(token) and token != "" do
    [{"Authorization", "Bearer #{token}"}]
  end

  defp auth_headers(_), do: []

  # Iceberg REST encodes multi-level namespaces with 0x1F (unit separator)
  defp encode_namespace(namespace) when is_binary(namespace) do
    namespace
    |> String.split(".")
    |> Enum.join(<<0x1F>>)
    |> URI.encode()
  end
end
