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
    {:handshake, {module, any}} |
    {:presentation, {module, any}} |
    {:grace, timeout}

  @type result ::
    {:ok, response :: any} |
    {:error, %Mux.ApplicationError{}} |
    :nack |
    {:error, %Mux.ServerError{}}


  @manager_options [:socket_opt, :connect_timeout, :connect_interval,
                    :drain_alarms, :grace]

  @spec sync_dispatch(Mux.Packet.dest, Mux.Packet.dest_table, request :: any,
        timeout) :: result
  def sync_dispatch(dest, tab, request, timeout \\ 5_000) do
    # should use dest table to alter destination
    with {:ok, pid, {present, state}} <- Mux.Client.Dispatcher.lookup(dest),
         {:ok, body} = apply(present, :encode, [request, state]),
         {:ok, receive_body}
          <- Mux.ClientSession.sync_dispatch(pid, dest, tab, body, timeout) do
        {:ok, _} = apply(present, :decode, [receive_body, state])
    end
  end

  @spec whereis(Mux.Packet.dest, Mux.Packet.dest_table) :: pid | nil
  def whereis(dest, _tab) do
    # should use dest table to alter destination
    case Mux.Client.Dispatcher.lookup(dest) do
      {:ok, pid, _} ->
        pid
      :nack ->
        nil
    end
  end

  @spec child_spec({Mux.Packet.dest, [option]}) :: Supervisor.child_spec
  def child_spec({dest, opts}) do
    %{id: {dest, __MODULE__},
      start: {__MODULE__, :start_link, [dest, opts]},
      type: :supervisor}
  end

  @spec start_link(Mux.Packet.dest, [option]) :: Supervisor.on_start
  def start_link(dest, opts) when is_binary(dest) do
    Supervisor.start_link(__MODULE__, {dest, opts})
  end

  @doc false
  def init({dest, opts}) do
    {addr, opts} = pop!(opts, :address)
    {port, opts} = pop!(opts, :port)
    # only register on init and not code change
    if Registry.keys(__MODULE__, self()) == [] do
      Registry.register(__MODULE__, dest, {addr, port})
    end
    {man_opts, pool_opts} = Keyword.split(opts, @manager_options)
    children = [{Mux.Client.Pool, {dest, pool_opts}},
                {Mux.Client.Manager, {dest, addr, port, man_opts}}]
    sup_opts = [strategy: :one_for_all, max_restarts: 0]
    Supervisor.init(children, sup_opts)
  end

  defp pop!(opts, key) do
    case Keyword.pop(opts, key) do
      {nil, _opts} ->
        raise ArgumentError, "connect socket #{key} not specified"
      {_value, _opts} = result ->
        result
    end
  end
end
