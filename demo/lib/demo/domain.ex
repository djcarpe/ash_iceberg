defmodule Demo.Domain do
  use Ash.Domain, extensions: [AshGraphql.Domain]

  resources do
    resource Demo.Event
  end
end
