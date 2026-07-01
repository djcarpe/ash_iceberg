defmodule Mix.Tasks.AshIceberg.CreateTable do
  @shortdoc "Create Iceberg tables for one or more Ash resources"

  @moduledoc """
  Creates Iceberg tables (namespace + table) for the given Ash resource modules.

  Reads each resource's `iceberg` DSL configuration to determine the catalog,
  namespace, table name, attribute types, and partition spec, then calls the
  Iceberg REST Catalog API to create any missing namespaces and tables.

  ## Usage

      mix ash_iceberg.create_table MyApp.Events MyApp.Users

  ## Behaviour

  * The task is **idempotent** — it skips tables that already exist (HTTP 409).
  * Namespace creation is always attempted first; existing namespaces are skipped.
  * Attribute types are mapped from Ash types to Iceberg primitive types.
  * Partition specs declared with the `partition` entity are included.

  ## Options

  * `--otp-app` / `-a` — OTP application to start before creating tables
    (ensures the catalog config is loaded).  Defaults to the `:app` value in
    `mix.exs`.

  ## Type mapping

  | Ash type | Iceberg type |
  |----------|--------------|
  | `:string`, `:uuid`, `:atom` | `string` |
  | `:integer` | `int` |
  | `:float` | `double` |
  | `:boolean` | `boolean` |
  | `:date` | `date` |
  | `:time` | `time` |
  | `:utc_datetime`, `:utc_datetime_usec`, `:naive_datetime_usec` | `timestamptz` |
  | `:naive_datetime` | `timestamp` |
  | `:decimal` | `decimal(18, 6)` |
  | `:map`, `:term` | `string` (JSON-encoded) |
  | `AshIceberg.Types.TimestampTz` | `timestamptz` |
  | `AshIceberg.Types.Fixed` | `fixed[16]` |
  | `AshIceberg.Types.Decimal` | `decimal(p, s)` (from constraints) |
  """

  use Mix.Task

  alias AshIceberg.{Catalog.RestClient, DataLayer.Info, Partition}
  alias Ash.Resource

  @requirements ["app.config"]

  @impl Mix.Task
  def run(args) do
    {opts, resource_strings, _} =
      OptionParser.parse(args, strict: [otp_app: :string], aliases: [a: :otp_app])

    if resource_strings == [] do
      Mix.shell().error("Usage: mix ash_iceberg.create_table MyApp.Resource1 [MyApp.Resource2 ...]")
      exit({:shutdown, 1})
    end

    _otp_app = opts[:otp_app]

    # Ensure the app is started so runtime config is available.
    Mix.Task.run("app.start")

    Enum.each(resource_strings, fn resource_string ->
      resource = Module.concat([resource_string])
      create_for_resource(resource)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp create_for_resource(resource) do
    catalog = Info.catalog(resource)
    namespace = Info.namespace(resource)
    table = Info.table(resource)

    if catalog == nil do
      Mix.shell().info(
        "  [skip] #{inspect(resource)} uses `warehouse` mode — " <>
          "Iceberg tables must be created externally."
      )

      return(:ok)
    end

    cfg = catalog.config()

    Mix.shell().info("\n[#{inspect(resource)}] → #{namespace}.#{table}")

    # 1. Namespace
    case RestClient.ensure_namespace(cfg, namespace) do
      :ok ->
        Mix.shell().info("  ✓ namespace '#{namespace}'")

      {:error, reason} ->
        Mix.raise("Failed to ensure namespace '#{namespace}': #{inspect(reason)}")
    end

    # 2. Schema
    schema = build_schema(resource)
    partitions = build_partition_spec(resource)

    # 3. Table
    case RestClient.create_table(cfg, namespace, table, schema, partitions) do
      {:ok, _} ->
        Mix.shell().info("  ✓ table '#{table}' created")

      {:error, {409, _}} ->
        Mix.shell().info("  ✓ table '#{table}' already exists (skipped)")

      {:error, reason} ->
        Mix.raise("Failed to create table '#{namespace}.#{table}': #{inspect(reason)}")
    end
  end

  defp build_schema(resource) do
    attributes = Resource.Info.attributes(resource)
    pk_ids = primary_key_field_ids(resource, attributes)

    fields =
      attributes
      |> Enum.with_index(1)
      |> Enum.map(fn {attr, idx} ->
        %{
          "id" => idx,
          "name" => to_string(attr.name),
          "required" => !attr.allow_nil?,
          "type" => ash_type_to_iceberg(attr.type, attr.constraints || [])
        }
      end)

    %{
      "type" => "struct",
      "schema-id" => 0,
      "identifier-field-ids" => pk_ids,
      "fields" => fields
    }
  end

  defp primary_key_field_ids(resource, attributes) do
    pk_names = Resource.Info.primary_key(resource) |> MapSet.new()

    attributes
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {attr, idx} ->
      if MapSet.member?(pk_names, attr.name), do: [idx], else: []
    end)
  end

  defp build_partition_spec(resource) do
    partitions = Info.partitions(resource)

    if partitions == [] do
      %{"spec-id" => 0, "fields" => []}
    else
      attributes = Resource.Info.attributes(resource)
      attr_id_map = attributes |> Enum.with_index(1) |> Map.new(fn {a, i} -> {a.name, i} end)

      fields =
        partitions
        |> Enum.with_index(1000)
        |> Enum.map(fn {%Partition{field: field, transform: transform}, part_id} ->
          source_id = Map.get(attr_id_map, field, 0)

          %{
            "field-id" => part_id,
            "source-id" => source_id,
            "name" => Partition.to_iceberg_spec(%Partition{field: field, transform: transform})["name"],
            "transform" => transform_string(transform)
          }
        end)

      %{"spec-id" => 0, "fields" => fields}
    end
  end

  defp transform_string(:identity), do: "identity"
  defp transform_string(:year), do: "year"
  defp transform_string(:month), do: "month"
  defp transform_string(:day), do: "day"
  defp transform_string(:hour), do: "hour"
  defp transform_string({:bucket, n}), do: "bucket[#{n}]"
  defp transform_string({:truncate, n}), do: "truncate[#{n}]"
  defp transform_string(other), do: to_string(other)

  defp ash_type_to_iceberg(Ash.Type.String, _), do: "string"
  defp ash_type_to_iceberg(Ash.Type.Atom, _), do: "string"
  defp ash_type_to_iceberg(Ash.Type.UUID, _), do: "uuid"
  defp ash_type_to_iceberg(Ash.Type.Integer, _), do: "int"
  defp ash_type_to_iceberg(Ash.Type.Float, _), do: "double"
  defp ash_type_to_iceberg(Ash.Type.Boolean, _), do: "boolean"
  defp ash_type_to_iceberg(Ash.Type.Date, _), do: "date"
  defp ash_type_to_iceberg(Ash.Type.Time, _), do: "time"
  defp ash_type_to_iceberg(Ash.Type.UtcDatetime, _), do: "timestamptz"
  defp ash_type_to_iceberg(Ash.Type.UtcDatetimeUsec, _), do: "timestamptz"
  defp ash_type_to_iceberg(Ash.Type.NaiveDatetime, _), do: "timestamp"
  defp ash_type_to_iceberg(Ash.Type.NaiveDatetimeUsec, _), do: "timestamptz"
  defp ash_type_to_iceberg(Ash.Type.Decimal, constraints) do
    p = constraints[:precision] || 18
    s = constraints[:scale] || 6
    "decimal(#{p}, #{s})"
  end
  defp ash_type_to_iceberg(AshIceberg.Types.TimestampTz, _), do: "timestamptz"
  defp ash_type_to_iceberg(AshIceberg.Types.Fixed, constraints) do
    n = constraints[:length] || 16
    "fixed[#{n}]"
  end
  defp ash_type_to_iceberg(AshIceberg.Types.Decimal, constraints) do
    p = constraints[:precision] || 18
    s = constraints[:scale] || 6
    "decimal(#{p}, #{s})"
  end
  defp ash_type_to_iceberg(Ash.Type.Map, _), do: "string"
  defp ash_type_to_iceberg(Ash.Type.Term, _), do: "string"
  defp ash_type_to_iceberg({:array, inner}, constraints) do
    "list<#{ash_type_to_iceberg(inner, constraints)}>"
  end
  defp ash_type_to_iceberg(_, _), do: "string"

  defp return(value), do: value
end
