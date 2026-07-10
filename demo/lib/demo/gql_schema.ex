defmodule Demo.GqlSchema do
  @moduledoc "Absinthe schema exposing Demo.Domain via AshGraphql."

  use Absinthe.Schema
  use AshGraphql, domains: [Demo.Domain]

  query do
    # Ash queries are injected by AshGraphql
  end
end
