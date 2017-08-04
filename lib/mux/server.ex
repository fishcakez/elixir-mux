defmodule Mux.Server do
  @moduledoc """
  Server pool for a Mux server.

  This modules defines a TCP Mux server pool.
  """

  use Supervisor

  @type option ::
    Mux.ServerSession.option |
    {:max_restarts, non_neg_integer} |
    {:max_seconds, pos_integer} |
    {:restart, Supervisor.restart} |
    {:grace, timeout} |
    {:spawn_opt, [:proc_lib.spawn_option]} |
    {:port, :inet.port_number} |
    {:socket_opt, [:gen_tcp.listen_option]} |
    {:handshake, {module, any}} |
    {:lease_interval, timeout} |
    {:lease_alarms, [any()]} |
    {:drain_alarms, [any()]}

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
    name = {:via, Registry, {__MODULE__, dest}}
    Supervisor.start_link(__MODULE__, {module, dest, arg, opts}, [name: name])
  end

  @doc false
  def init({module, dest, arg, opts}) do
    {_, _} = Registry.update_value(__MODULE__, dest, fn _ -> module end)
    {port, opts} = pop_port!(opts)
    {sock_opts, pool_opts} = Keyword.split(opts, [:socket_opt])
    children = [{Mux.Server.Pool, {module, dest, arg, pool_opts}},
                {Mux.Server.Socket, {dest, port, sock_opts}}]
    sup_opts = [strategy: :rest_for_one, max_restarts: 0]
    Supervisor.init(children, sup_opts)
  end

  defp pop_port!(opts) do
    case Keyword.pop(opts, :port) do
      {nil, _opts} ->
        raise "listen socket port not specified"
      {_port, _opts} = result ->
        result
    end
  end
end
