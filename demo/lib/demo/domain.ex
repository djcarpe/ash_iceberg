defmodule Demo.Domain do
  use Ash.Domain

  resources do
    resource Demo.Event
  end
end
