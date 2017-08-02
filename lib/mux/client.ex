defmodule Mux.Client do
  @moduledoc """
  Client pool for a Mux client.

  This modules defines a TCP Mux client pool.
  """

  use Supervisor

  @type option ::
    Mux.ClientSession.option |
    {:spawn_opt, [:proc_lib.spawn_option]} |
    {:address, :inet.socket_address() | :inet.hostname()} |
    {:port, :inet.port_number} |
    {:socket_opt, [:gen_tcp.connect_option]} |
    {:connect_timeout, timeout} |
    {:connect_interval, timeout} |
    {:grace, timeout}

  @manager_options [:socket_opt, :connect_timeout, :connect_interval, :grace]

  @spec sync_dispatch(Mux.Packet.dest, Mux.Packet.dest_table, body :: binary,
        timeout) :: Mux.ClientSession.result
  def sync_dispatch(dest, tab, body, timeout \\ 5_000) do
    # should use dest table to alter destination
    name = {:via, Mux.Client.Dispatcher, dest}
    Mux.ClientSession.sync_dispatch(name, dest, tab, body, timeout)
  end

  @spec whereis(Mux.Packet.dest, Mux.Packet.dest_table) :: pid | nil
  def whereis(dest, _tab) do
    # should use dest table to alter destination
    case Mux.Client.Dispatcher.whereis_name(dest) do
      pid when is_pid(pid) ->
        pid
      :undefined ->
        nil
    end
  end

  @spec child_spec({module, Mux.Packet.dest, any, [option]}) ::
    Supervisor.child_spec
  def child_spec({module, dest, arg, opts}) do
    %{id: {dest, __MODULE__},
      start: {__MODULE__, :start_link, [module, dest, arg, opts]},
      type: :supervisor}
  end

  @spec start_link(module, Mux.Packet.dest, arg :: any, [option]) ::
    Supervisor.on_start
  def start_link(module, dest, arg, opts) when is_binary(dest) do
    Supervisor.start_link(__MODULE__, {module, dest, arg, opts})
  end

  @doc false
  def init({module, dest, arg, opts}) do
    # only register on init and not code change
    if Registry.keys(__MODULE__, self()) == [] do
      Registry.register(__MODULE__, dest, module)
    end
    {addr, opts} = pop!(opts, :address)
    {port, opts} = pop!(opts, :port)
    {man_opts, pool_opts} = Keyword.split(opts, @manager_options)
    # manager depends on pool and pool children should exit with/after manager,
    # later we should try to pause in termination of manager to gracefully
    # drain client so manager should be started second and shutdown first
    children = [{Mux.Client.Pool, {module, dest, arg, pool_opts}},
                {Mux.Client.Manager, {dest, addr, port, man_opts}}]
    sup_opts = [strategy: :one_for_all, max_restarts: 0]
    Supervisor.init(children, sup_opts)
  end

  defp pop!(opts, key) do
    case Keyword.pop(opts, key) do
      {nil, _opts} ->
        raise "connect socket #{key} not specified"
      {_value, _opts} = result ->
        result
    end
  end
end
