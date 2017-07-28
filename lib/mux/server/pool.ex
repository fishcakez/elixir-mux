defmodule Mux.Server.Pool do
  @moduledoc false

  @behaviour :acceptor_pool

  def child_spec({module, arg, pool_opts}) do
    child = %{id: __MODULE__,
              start: {__MODULE__, :start_link, [module, arg, pool_opts]},
              type: :supervisor}
    Supervisor.child_spec(child, [])
  end

  def start_link(module, arg, opts),
    do: :acceptor_pool.start_link(__MODULE__, {module, arg, opts})

  def init({module, arg, opts}) do
    {restart, opts} = Keyword.pop(opts, :restart, :temporary)
    {grace, opts} = Keyword.pop(opts, :grace, 5_000)
    {max_restarts, opts} = Keyword.pop(opts, :max_restarts, 3)
    {max_seconds, opts} = Keyword.pop(opts, :max_seconds, 5)

    flags = %{intensity: max_restarts, period: max_seconds}

    spec = %{id: module,
             start: {Mux.Server.Manager, {module, arg, opts}, []},
             type: :supervisor,
             restart: restart,
             grace: grace}

    {:ok, {flags, [spec]}}
  end
end
