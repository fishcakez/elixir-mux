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
    {:restart, :supervisor.restart} |
    {:grace, timeout} |
    {:spawn_opt, [:proc_lib.spawn_option]} |
    {:port, :inet.port_number} |
    {:socket_opt, [:gen_tcp.listen_option]}

  @spec start_link(module, Mux.Packet.dest, arg :: any, [option]) ::
    Supervisor.on_start
  def start_link(module, dest, arg, opts) when is_binary(dest) do
    Supervisor.start_link(__MODULE__, {module, dest, arg, opts}, [])
  end

  @doc false
  def init({module, dest, arg, opts}) do
    {:ok, _} = Registry.register(Mux.Server, dest, module)
    {port, opts} = pop_port!(opts)
    {sock_opts, pool_opts} = Keyword.split(opts, [:socket_opt])
    children = [{Mux.Server.Pool, {module, arg, pool_opts}},
                {Mux.Server.Socket, {port, sock_opts}}]
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
