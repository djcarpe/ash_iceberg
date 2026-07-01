defmodule Mix.Tasks.AshIceberg.Gen.Catalog do
  @shortdoc "Generate an AshIceberg catalog module"

  @moduledoc """
  Generates a boilerplate `AshIceberg.Catalog` module and config stanza.

  ## Usage

      mix ash_iceberg.gen.catalog MyApp.IcebergCatalog

  ## Options

  * `--type` — catalog backend: `rest` (default) or `filesystem`
  * `--namespace` — default namespace to suggest in the generated module (default: `"default"`)
  * `--otp-app` / `-a` — OTP app name for `config/config.exs` (default: inferred from `mix.exs`)

  ## Examples

      # REST catalog (Polaris, Nessie, Tabular, etc.)
      mix ash_iceberg.gen.catalog MyApp.IcebergCatalog --type rest

      # Local filesystem catalog (dev / testing)
      mix ash_iceberg.gen.catalog MyApp.DevCatalog --type filesystem

  The generated files follow the same pattern as the official Ash generators:
  one module file under `lib/` and a config snippet printed to stdout for you
  to paste into `config/config.exs` (or `config/runtime.exs` for secrets).
  """

  use Mix.Task

  @impl Mix.Task
  def run(args) do
    {opts, module_strings, _} =
      OptionParser.parse(args,
        strict: [type: :string, namespace: :string, otp_app: :string],
        aliases: [a: :otp_app]
      )

    case module_strings do
      [] ->
        Mix.shell().error("Usage: mix ash_iceberg.gen.catalog MyApp.MyCatalog [--type rest|filesystem]")
        exit({:shutdown, 1})

      [module_string | _] ->
        generate(module_string, opts)
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp generate(module_string, opts) do
    catalog_type = opts[:type] || "rest"
    namespace = opts[:namespace] || "default"
    otp_app = opts[:otp_app] || infer_otp_app()
    module_name = Module.concat([module_string])

    # Derive the file path from the module name
    file_path =
      module_name
      |> Module.split()
      |> Enum.map(&Macro.underscore/1)
      |> Path.join()
      |> then(&"lib/#{&1}.ex")

    if File.exists?(file_path) do
      Mix.shell().info("  [skip] #{file_path} already exists")
    else
      content = catalog_module(module_name, catalog_type, otp_app)
      File.mkdir_p!(Path.dirname(file_path))
      File.write!(file_path, content)
      Mix.shell().info("  created #{file_path}")
    end

    # Print config snippet
    Mix.shell().info("""

    #{IO.ANSI.yellow()}Add this to config/config.exs (or config/runtime.exs for secrets):#{IO.ANSI.reset()}

    #{config_snippet(module_name, catalog_type, namespace, otp_app)}

    #{IO.ANSI.yellow()}Then add the catalog to your supervision tree:#{IO.ANSI.reset()}

        # lib/my_app/application.ex
        children = [
          #{inspect(module_name)}
        ]

    #{IO.ANSI.yellow()}And create your tables:#{IO.ANSI.reset()}

        mix ash_iceberg.create_table MyApp.MyResource
    """)
  end

  defp catalog_module(module_name, "filesystem", _otp_app) do
    """
    defmodule #{inspect(module_name)} do
      @moduledoc "Filesystem Iceberg catalog."

      use AshIceberg.Catalog, otp_app: #{inspect(infer_otp_app() |> String.to_atom())}
    end
    """
  end

  defp catalog_module(module_name, _type, _otp_app) do
    """
    defmodule #{inspect(module_name)} do
      @moduledoc "Iceberg REST catalog."

      use AshIceberg.Catalog, otp_app: #{inspect(infer_otp_app() |> String.to_atom())}
    end
    """
  end

  defp config_snippet(module_name, "rest", namespace, otp_app) do
    """
        config #{inspect(String.to_atom(otp_app))}, #{inspect(module_name)},
          type: :rest,
          uri: System.get_env("ICEBERG_REST_URI", "http://localhost:8181"),
          warehouse: System.get_env("ICEBERG_WAREHOUSE", "s3://my-bucket/warehouse/"),
          aws_region: System.get_env("AWS_REGION", "us-east-1"),
          aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
          aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY")

        # Default namespace for resources: "#{namespace}"
    """
  end

  defp config_snippet(module_name, "filesystem", namespace, otp_app) do
    """
        config #{inspect(String.to_atom(otp_app))}, #{inspect(module_name)},
          type: :filesystem,
          path: "/path/to/local/warehouse"

        # Default namespace for resources: "#{namespace}"
    """
  end

  defp infer_otp_app do
    Mix.Project.config()[:app] |> to_string()
  rescue
    _ -> "my_app"
  end
end
