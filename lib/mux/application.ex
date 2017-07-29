defmodule Mux.Application do
  @moduledoc false

  use Application

  def start(_, _) do
    children = [registry(Mux.Server, :unique),
                registry(Mux.Server.Pool, :unique),
                registry(Mux.Server.Socket, :unique),
                registry(Mux.ServerSession, :duplicate)]
    Supervisor.start_link(children, [strategy: :one_for_one])

  end

  defp registry(name, keys) do
    child_opts = [id: Module.concat(name, Registry)]
    Supervisor.child_spec({Registry, [name: name, keys: keys]}, child_opts)
  end
end
