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

  @doc """
  Create a new Iceberg table.

  `partition_spec` is optional; when `nil` or omitted the table is unpartitioned.
  """
  @spec create_table(map(), String.t(), String.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, term()}
  def create_table(config, namespace, table, schema, partition_spec \\ nil) do
    body =
      %{
        "name" => table,
        "schema" => schema,
        "write-order" => %{"order-id" => 0, "fields" => []}
      }
      |> maybe_put("partition-spec", partition_spec)

    post(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables", body)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

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

  @doc """
  Load the full raw table metadata document from the REST catalog.

  Returns the parsed JSON body (includes `metadata-location`, `metadata`,
  `config`, etc.).
  """
  @spec load_table_metadata(map(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def load_table_metadata(config, namespace, table) do
    load_table(config, namespace, table)
  end

  @doc """
  List snapshots for a table by reading its metadata from the REST catalog.

  Returns a list of snapshot maps in chronological order (oldest first).
  Each map contains at minimum:
  - `"snapshot-id"` — unique integer ID
  - `"timestamp-ms"` — Unix milliseconds
  - `"operation"` — `"append"`, `"overwrite"`, `"replace"`, or `"delete"`
  - `"summary"` — map of arbitrary metadata
  """
  @spec list_snapshots(map(), String.t(), String.t()) ::
          {:ok, [map()]} | {:error, term()}
  def list_snapshots(config, namespace, table) do
    case load_table(config, namespace, table) do
      {:ok, %{"metadata" => %{"snapshots" => snapshots}}} ->
        ordered = Enum.sort_by(snapshots, & &1["timestamp-ms"])
        {:ok, ordered}

      {:ok, _other} ->
        {:ok, []}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Apply one or more schema updates to an existing table via the REST commit API.

  `updates` is a list of Iceberg table update maps.  Common shapes:

  **Add a column**

      %{
        "action" => "add-column",
        "parent" => nil,                    # nil for top-level
        "name" => "new_col",
        "type" => "string",
        "doc" => "optional docstring",
        "required" => false
      }

  **Remove a column** (by field-id or name)

      %{"action" => "remove-field", "path" => ["column_name"]}

  **Rename a column**

      %{"action" => "rename-field", "path" => ["old_name"], "new-name" => "new_name"}

  **Update nullability**

      %{"action" => "make-column-optional", "path" => ["column_name"]}

  Returns `{:ok, response_map}` on success.
  """
  @spec evolve_schema(map(), String.t(), String.t(), [map()]) ::
          {:ok, map()} | {:error, term()}
  def evolve_schema(config, namespace, table, updates) when is_list(updates) do
    schema_updates = Enum.map(updates, fn u -> Map.put_new(u, "action-type", "update-schema") end)
    commit_table(config, namespace, table, [], schema_updates)
  end

  @doc """
  Expire snapshots via the REST catalog.

  Accepts either a `DateTime` (legacy positional form) or a keyword list:
    - `:older_than` — expire snapshots committed before this `DateTime`
    - `:min_snapshots_to_keep` — always keep at least this many recent snapshots

  Not all catalog implementations support this endpoint; returns
  `{:error, {404, _}}` when unsupported.
  """
  @spec expire_snapshots(map(), String.t(), String.t(), DateTime.t() | keyword()) ::
          {:ok, map()} | {:error, term()}
  def expire_snapshots(config, namespace, table, %DateTime{} = older_than) do
    expire_snapshots(config, namespace, table, older_than: older_than)
  end

  def expire_snapshots(config, namespace, table, opts) when is_list(opts) do
    body =
      %{}
      |> then(fn b ->
        case opts[:older_than] do
          %DateTime{} = dt -> Map.put(b, "older-than-ms", DateTime.to_unix(dt, :millisecond))
          nil -> b
        end
      end)
      |> then(fn b ->
        case opts[:min_snapshots_to_keep] do
          n when is_integer(n) -> Map.put(b, "min-snapshots-to-keep", n)
          nil -> b
        end
      end)

    post(config, "/v1/namespaces/#{encode_namespace(namespace)}/tables/#{table}/snapshots/expire", body)
  end

  @doc """
  Update table properties (arbitrary key-value pairs stored in table metadata).

      RestClient.set_table_properties(cfg, "analytics", "events",
        %{"write.target-file-size-bytes" => "134217728"}
      )
  """
  @spec set_table_properties(map(), String.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def set_table_properties(config, namespace, table, properties) when is_map(properties) do
    update = %{
      "action" => "set-properties",
      "action-type" => "update-properties",
      "updates" => properties
    }

    commit_table(config, namespace, table, [], [update])
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
