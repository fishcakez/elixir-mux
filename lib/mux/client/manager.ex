defmodule Mux.Client.Manager do
  @moduledoc false

  @behaviour :gen_statem

  @connect_timeout 5_000
  @connect_interval 5_000
  @connect_options [active: false]
  @drain_alarms []

  defmodule Data do
    @moduledoc false

    @enforce_keys [:dest, :address, :port, :socket_opt, :timeout, :interval,
                   :supervisor, :monitor, :session, :drain, :alarms]
    defstruct [:dest, :address, :port, :socket_opt, :timeout, :interval,
               :supervisor, :monitor, :session, :drain, :alarms]
  end

  @type address :: :inet.sock_address() | :inet.hostname()
  @type option ::
    {:socket_opt, [:gen_tcp.connect_option]} |
    {:connect_timeout, timeout} |
    {:connect_interval, timeout} |
    {:grace, timeout}

  @spec child_spec({Mux.Packet.dest, address, :inet.port_number, option}) ::
    Supervisor.child_spec
  def child_spec({dest, addr, port, opts}) do
    %{id: {dest, __MODULE__},
      start: {__MODULE__, :start_link, [dest, addr, port, opts]},
      type: :worker, shutdown: Keyword.get(opts, :grace, 5_000)}
  end

  def start_link(dest, addr, port, opts) do
    parent = self()
    :gen_statem.start_link(__MODULE__, {dest, parent, addr, port, opts}, [])
  end

  def init({dest, sup, addr, port, opts}) do
    Process.flag(:trap_exit, true)
    sock_opt = Keyword.get(opts, :socket_opt, [])
    timeout = Keyword.get(opts, :connect_timeout, @connect_timeout)
    interval = Keyword.get(opts, :connect_interval, @connect_interval)
    drain_ref = make_ref()
    alarms = watch_alarms(drain_ref, opts)
    data = %Data{dest: dest, address: addr, port: port, socket_opt: sock_opt,
                 timeout: timeout, interval: interval, supervisor: sup,
                 monitor: nil, session: nil, drain: drain_ref, alarms: alarms}
    {:ok, :init, data, [{:next_event, :internal, :init}]}
  end

  def callback_mode(),
    do: :state_functions

  def init(:internal, :init, %Data{dest: dest, supervisor: sup} = data) do
    # get pool process to start sessions under
    children = Supervisor.which_children(sup)
    {_, pool, _, _} = List.keyfind(children, {dest, Mux.Client.Pool}, 0)
    {:next_state, :closed, %Data{data | supervisor: pool}, connect()}
  end

  def open(:info, {:DOWN, mon, _, _, reason}, %Data{monitor: mon} = data) do
    data = %Data{data | monitor: nil, session: nil}
    case reason do
      :normal ->
        {:next_state, :closed, data, connect()}
      {:tcp_error, _} ->
        {:next_state, :closed, data, connect()}
      reason ->
        {:stop, {:shutdown, reason}, data}
    end
  end
  def open(:info, {:SET, ref, id},
      %Data{drain: ref, session: pid, alarms: alarms} = data) do
    # session stays open until exchanges are processed
    Mux.ClientSession.drain(pid)
    {:keep_state, %Data{data | alarms: MapSet.put(alarms, id)}}
  end
  def open(:info, msg, data),
    do: info(msg, data)

  def closed(_, {:connect, _}, data),
    do: connect(data)

  def closed(:info, {:SET, ref, id},
      %Data{drain: ref, alarms: alarms} = data) do
    {:keep_state, %Data{data | alarms: MapSet.put(alarms, id)}}
  end
  def closed(:info, msg, data),
    do: info(msg, data)

  def terminate(_, _, %Data{monitor: nil}),
    do: :ok
  def terminate(_, _, %Data{monitor: mon, session: pid}) do
    Mux.ClientSession.drain(pid)
    receive do
      {:DOWN, ^mon, _, _, _} ->
        :ok
    end
  end

  ## Helpers

  # clearing an alarm doesn't change state, must wait for session change
  defp info({:CLEAR, ref, id}, %Data{drain: ref, alarms: alarms} = data) do
    {:keep_state, %Data{data | alarms: MapSet.delete(alarms, id)}}
  end
  defp info(msg, _) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    :keep_state_and_data
  end

  defp connect(),
    do: [{:next_event, :internal, {:connect, 0}}]

  defp backoff(interval) do
    backoff = rand_interval(interval)
    [{:state_timeout, backoff, {:connect, backoff}}]
  end

  defp connect(%Data{alarms: alarms, interval: interval} = data) do
    if MapSet.size(alarms) > 0 do
      {:keep_state_and_data, backoff(interval)}
    else
      handle_connect(data)
    end
  end

  defp handle_connect(data) do
    %Data{address: addr, port: port, socket_opt: opts, timeout: timeout,
          supervisor: sup, interval: interval} = data
    case :gen_tcp.connect(addr, port, @connect_options ++ opts, timeout) do
      {:ok, sock} ->
        {pid, mon} = Mux.Client.Pool.start_session(sup, sock)
        {:next_state, :open, %Data{data | monitor: mon, session: pid}}
      {:error, _} ->
        {:keep_state_and_data, backoff(interval)}
    end
  end

  defp rand_interval(interval) do
    # randomise uniform around [0.5 interval, 1.5 interval]
    div(interval, 2) + :rand.uniform(interval + 1) - 1
  end

  defp watch_alarms(drain_ref, opts) do
    # register returns true if alarm is set
    opts
    |> Keyword.get(:drain_alarms, @drain_alarms)
    |> Enum.filter(&Mux.Alarm.register(Mux.Alarm, &1, drain_ref))
    |> Enum.into(MapSet.new())
  end
end
