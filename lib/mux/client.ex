defmodule Mux.Client do
  @moduledoc """
  Client pool for a Mux client.

  This modules defines a TCP Mux client pool.
  """

  use Supervisor

  @type option ::
    Mux.Connection.option |
    {:spawn_opt, [:proc_lib.spawn_option]} |
    {:address, :inet.socket_address() | :inet.hostname()} |
    {:port, :inet.port_number} |
    {:socket_opt, [:gen_tcp.connect_option]} |
    {:connect_timeout, timeout} |
    {:connect_interval, timeout} |
    {:handshake_timeout, timeout} |
    {:session, {module, any}} |
    {:presentation, {module, any}} |
    {:grace, timeout}

  @type sync_timeout ::
    timeout |
    {:clean_timeout, timeout} |
    {:dirty_timeout, timeout}

  @type result ::
    {:ok, response :: any} |
    {:error, %Mux.ApplicationError{}} |
    :nack |
    {:error, %Mux.ServerError{}}

  @discarded_message "process discarded"
  @manager_options [:socket_opt, :connect_timeout, :connect_interval,
                    :drain_alarms, :grace]

  @spec sync_dispatch(Mux.Packet.dest, Mux.Packet.dest_table, request :: any,
        sync_timeout) :: result
  def sync_dispatch(dest, tab, request, timeout \\ 5_000) do
    # should use dest table to alter destination
    with {:ok, pid, {present, state}} <- Mux.Client.Dispatcher.lookup(dest),
         {:ok, body} = apply(present, :encode, [request, state]),
         {:ok, receive_body} <- sync_dispatch(pid, dest, tab, body, timeout) do
      {:ok, _} = apply(present, :decode, [receive_body, state])
    end
  end

  @spec dispatch(Mux.Packet.dest, Mux.Packet.dest_table, request :: any) ::
    {pid, reference}
  def dispatch(dest, tab, request) do
    args = [self(), Mux.Context.get(), dest, tab, request]
    {pid, ref} = result = spawn_monitor(__MODULE__, :init_it, args)
    send(pid, {:enter_loop, self(), ref})
    result
  end

  @spec async_cancel(pid, reference, why :: String.t) :: :ok
  def async_cancel(pid, ref, why \\ @discarded_message) do
    send(pid, {:async_cancel, ref, why})
    :ok
  end

  @spec cancel(pid, reference, why :: String.t, timeout) ::
    :ok | {:error, :not_found}
  def cancel(pid, ref, why \\ @discarded_message, timeout \\ 5_000) do
    mon = Process.monitor(pid)
    send(pid, {:cancel, {self(), mon}, ref, why})
    receive do
      {^mon, result} ->
        Process.demonitor(mon, [:flush])
        result
      {:DOWN, ^mon, _, _, normal} when normal in [:normal, :noproc] ->
        # the process likely handled the result before this request
        {:error, :not_found}
      {:DOWN, ^mon, _, _, reason} ->
        exit({reason, {__MODULE__, :cancel, [pid, ref, why, timeout]}})
    after
      timeout ->
        Process.demonitor(mon, [:flush])
        exit({:timeout, {__MODULE__, :cancel, [pid, ref, why, timeout]}})
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

  defp sync_dispatch(pid, dest, tab, body, timeout) do
    Mux.Client.Connection.sync_dispatch(pid, dest, tab, body, timeout)
  catch
    :exit, {normal, _} when normal in [:normal, :noproc] ->
      # never hit network
      :nack
  end

  @doc false
  def init_it(parent, ctx, dest, tab, request),
    do: Mux.Context.bind(ctx, fn -> init_it(parent, dest, tab, request) end)

  defp init_it(parent, dest, tab, request) do
    mon = Process.monitor(parent)
    receive do
      {:enter_loop, ^parent, tag} ->
        enter_loop({parent, tag}, mon, dest, tab, request)
      {:DOWN, ^mon, _, _, _} ->
        # never hit network
        exit(:normal)
    end
  end

  defp enter_loop(from, mon, dest, tab, request) do
    with {:ok, pid, present_info} <- Mux.Client.Dispatcher.lookup(dest),
         {present, state} = present_info,
         {:ok, body} = apply(present, :encode, [request, state]),
         {_, _} = to <- Mux.Client.Connection.dispatch(pid, dest, tab, body) do
      loop(from, mon, to, present_info)
    else
      :nack ->
        terminate(:nack, from)
    end
  end

  defp loop({_, tag} = from, mon, {cli, ref} = to, present_info) do
    receive do
      {^ref, result} ->
        terminate(result, from, present_info)
      {:async_cancel, ^tag, why} ->
        Mux.Client.Connection.async_cancel(cli, ref, why)
        exit(:cancel)
      {:cancel, sync, ^tag, why} ->
        # existed at some point, cancel provides no guarantee about whether
        # remote handled the request, only whether a result will arrive later
        # (in the case of :error) or not (:ok)
        Mux.Client.Connection.async_cancel(cli, ref, why)
        reply(sync, :ok)
        exit(:cancel)
      {:cancel, sync, _, _} ->
        reply(sync, {:error, :not_found})
        loop(from, mon, to, present_info)
      {:DOWN, ^ref, _, _, normal} when normal in [:normal, :noproc] ->
        # never hit network
        terminate(:nack, from)
      {:DOWN, ^ref, _, _, reason} ->
        # forward exit reason
        exit(reason)
      {:DOWN, ^mon, _, _, _} ->
        exit(:cancel)
    end
  end

  defp terminate({:ok, body}, from, {present, state}) do
    {:ok, _} = ok = apply(present, :decode, [body, state])
    terminate(ok, from)
  end
  defp terminate(result, from, _),
    do: terminate(result, from)

  defp terminate(result, from) do
    reply(from, result)
    exit(:normal)
  end

  defp reply({pid, tag}, resp),
    do: send(pid, {tag, resp})
end
