defmodule AshIceberg.Connection do
  @moduledoc """
  GenServer that owns a DuckDB in-memory database with the `iceberg`
  (and optionally `httpfs`) extension loaded and the catalog attached.

  One connection process is started per catalog module. Queries are
  serialised through it; for higher concurrency, run multiple instances
  under a pool supervisor.
  """

  use GenServer
  require Logger

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Execute a SQL string against the catalog connection. Returns `{:ok, rows}` or `{:error, reason}`."
  @spec query(GenServer.server(), String.t()) :: {:ok, [map()]} | {:error, term()}
  def query(conn, sql) do
    GenServer.call(conn, {:query, sql}, :infinity)
  end

  @doc "Returns true if the DuckDB catalog ATTACH succeeded for this connection."
  @spec catalog_attached?(GenServer.server()) :: boolean()
  def catalog_attached?(conn) do
    GenServer.call(conn, :catalog_attached?, :infinity)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    catalog_module = Keyword.fetch!(opts, :catalog)
    config = catalog_module.config()

    case open_and_configure(config) do
      {:ok, db, conn, catalog_attached?} ->
        {:ok, %{db: db, conn: conn, catalog: catalog_module, config: config, catalog_attached: catalog_attached?}}

      {:error, reason} ->
        Logger.error("[AshIceberg.Connection] Failed to open DuckDB: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:query, sql}, _from, %{conn: conn} = state) do
    result = execute(conn, sql)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call(:catalog_attached?, _from, %{catalog_attached: attached} = state) do
    {:reply, attached, state}
  end

  @impl GenServer
  def terminate(_reason, _state) do
    # DuckDB DB/connection NIFs are cleaned up by the garbage collector.
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp open_and_configure(config) do
    with {:ok, db} <- Duckdbex.open(),
         {:ok, conn} <- Duckdbex.connection(db),
         :ok <- load_iceberg(conn),
         :ok <- configure_performance(conn, config),
         :ok <- maybe_setup_s3(conn, config) do
      catalog_attached? = attach_catalog(conn, config)
      {:ok, db, conn, catalog_attached?}
    end
  end

  defp configure_performance(conn, config) do
    threads = config[:duckdb_threads] || System.schedulers_online()
    cmds = ["SET threads TO #{threads}"]

    cmds =
      case config[:duckdb_memory_limit] do
        nil -> cmds
        limit -> cmds ++ ["SET memory_limit='#{limit}'"]
      end

    Enum.each(cmds, fn sql ->
      case run(conn, sql) do
        {:ok, _} -> :ok
        {:error, e} -> Logger.warning("[AshIceberg.Connection] Performance config skipped: #{inspect(e)}")
      end
    end)

    :ok
  end

  defp load_iceberg(conn) do
    with {:ok, _} <- run(conn, "INSTALL icu"),
         {:ok, _} <- run(conn, "LOAD icu"),
         {:ok, _} <- run(conn, "INSTALL core_functions"),
         {:ok, _} <- run(conn, "LOAD core_functions"),
         {:ok, _} <- run(conn, "INSTALL iceberg"),
         {:ok, _} <- run(conn, "LOAD iceberg") do
      :ok
    end
  end

  defp maybe_setup_s3(conn, %{warehouse: w} = config) when is_binary(w) do
    if String.starts_with?(w, "s3://") do
      setup_s3(conn, config)
    else
      :ok
    end
  end

  defp maybe_setup_s3(conn, %{type: :rest, uri: _uri} = config) do
    # REST catalog may still write to S3
    if config[:aws_access_key_id] do
      setup_s3(conn, config)
    else
      :ok
    end
  end

  defp maybe_setup_s3(_conn, _config), do: :ok

  defp setup_s3(conn, config) do
    region = config[:aws_region] || "us-east-1"
    key = config[:aws_access_key_id] || ""
    secret = config[:aws_secret_access_key] || ""

    base = [
      "INSTALL httpfs",
      "LOAD httpfs",
      "SET s3_region='#{region}'",
      "SET s3_access_key_id='#{key}'",
      "SET s3_secret_access_key='#{secret}'"
    ]

    # Extra settings for custom S3-compatible endpoints (e.g. MinIO)
    extra =
      case config[:s3_endpoint] do
        nil ->
          []

        endpoint ->
          %{host: host, port: port} = URI.parse(endpoint)
          ep = if port, do: "#{host}:#{port}", else: host
          use_ssl = Map.get(config, :s3_use_ssl, true)
          url_style = config[:s3_url_style] || "vhost"

          [
            "SET s3_endpoint='#{ep}'",
            "SET s3_use_ssl=#{use_ssl}",
            "SET s3_url_style='#{url_style}'"
          ]
      end

    Enum.reduce_while(base ++ extra, :ok, fn sql, _ ->
      case run(conn, sql) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  # Attempts to ATTACH the catalog into DuckDB. Returns true if successful,
  # false if the extension doesn't support the ATTACH variant (e.g. older builds
  # that lack REST ATTACH). When false, QueryBuilder uses iceberg_scan() instead.
  defp attach_catalog(conn, %{type: :rest, uri: uri} = config) do
    catalog_name = catalog_name_from_config(config)

    auth_clause =
      cond do
        config[:token]  -> ", TOKEN '#{config[:token]}'"
        config[:secret] -> ", SECRET '#{config[:secret]}'"
        true            -> ", AUTHORIZATION_TYPE 'none'"
      end

    # DuckDB v1.5+ uses ENDPOINT option; older builds accept the URL as first arg.
    # Try ENDPOINT form first, fall back to positional-URL form.
    sql_endpoint = "ATTACH '' AS #{catalog_name} (TYPE ICEBERG, ENDPOINT '#{uri}'#{auth_clause})"
    sql_positional = "ATTACH '#{uri}' AS #{catalog_name} (TYPE ICEBERG#{auth_clause})"

    result =
      case run(conn, sql_endpoint) do
        {:ok, _} = ok -> ok
        {:error, _}   -> run(conn, sql_positional)
      end

    case result do
      {:ok, _} ->
        true

      {:error, reason} ->
        Logger.warning(
          "[AshIceberg.Connection] Could not attach REST catalog (#{inspect(reason)}). " <>
            "Queries will use iceberg_scan() with warehouse path instead."
        )

        false
    end
  end

  defp attach_catalog(conn, %{type: :filesystem, path: path} = config) do
    catalog_name = catalog_name_from_config(config)
    sql = "ATTACH '#{path}' AS #{catalog_name} (TYPE ICEBERG)"

    case run(conn, sql) do
      {:ok, _} ->
        true

      {:error, reason} ->
        Logger.warning(
          "[AshIceberg.Connection] Could not attach filesystem catalog: #{inspect(reason)}"
        )

        false
    end
  end

  defp attach_catalog(_conn, _config), do: false

  defp catalog_name_from_config(%{catalog_name: name}) when is_binary(name), do: name
  defp catalog_name_from_config(_), do: "iceberg_catalog"

  defp run(conn, sql) do
    case Duckdbex.query(conn, sql) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute(conn, sql) do
    case Duckdbex.query(conn, sql) do
      {:ok, result} ->
        rows = rows_to_maps(result)
        {:ok, rows}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp rows_to_maps(result) do
    columns = Duckdbex.columns(result)
    rows = Duckdbex.fetch_all(result)

    Enum.map(rows, fn row ->
      columns
      |> Enum.zip(row)
      |> Map.new(fn {col, val} -> {col, val} end)
    end)
  end
end
