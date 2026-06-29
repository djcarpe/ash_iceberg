defmodule AshIceberg.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/ash-project/ash_iceberg"

  def project do
    [
      app: :ash_iceberg,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      description: description(),
      source_url: @source_url,
      homepage_url: @source_url,
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Ash Framework
      {:ash, "~> 3.0"},
      {:spark, "~> 2.0"},

      # DuckDB for query execution (Iceberg extension)
      {:duckdbex, "~> 0.3"},

      # REST Catalog HTTP client
      {:req, "~> 0.5"},

      # Utilities
      {:jason, "~> 1.4"},
      {:decimal, "~> 2.0"},

      # Dev / test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      name: "AshIceberg",
      source_ref: "v#{@version}",
      extras: [
        "README.md": [filename: "readme", title: "Home"],
        "CHANGELOG.md": [filename: "changelog", title: "Changelog"]
      ],
      groups_for_modules: [
        "Data Layer": [
          AshIceberg.DataLayer,
          AshIceberg.DataLayer.Info
        ],
        Catalog: [
          AshIceberg.Catalog,
          AshIceberg.Catalog.RestClient
        ],
        Connection: [
          AshIceberg.Connection
        ],
        Types: [
          AshIceberg.Types.TimestampTz,
          AshIceberg.Types.Fixed
        ]
      ]
    ]
  end

  defp description do
    """
    Apache Iceberg data layer for Ash Framework. Provides integration between Ash
    resources and Iceberg tables via DuckDB and the Iceberg REST Catalog.
    """
  end

  defp package do
    [
      name: :ash_iceberg,
      maintainers: ["Ash Framework Team"],
      licenses: ["MIT"],
      links: %{GitHub: @source_url},
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG*)
    ]
  end
end
