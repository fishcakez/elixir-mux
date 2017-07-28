defmodule Mux.Application do
  @moduledoc false

  use Application

  def start(_, _) do
    children = [{Registry, [name: Mux.Server, keys: :duplicate]}]
    Supervisor.start_link(children, [strategy: :one_for_one])
  end
end
