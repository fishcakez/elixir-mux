defmodule Mux.Server.Manager do
  @moduledoc false

  @behaviour :gen_statem
  @behaviour :acceptor

  @options [:lease_interval, :lease_alarms, :drain_alarms]
  @lease_interval :infinity
  @lease_alarms []
  @drain_alarms []

  defmodule Data do
    @moduledoc false

    @enforce_keys [:session, :drain, :lease, :alarms, :interval]
    defstruct [:session, :drain, :lease, :alarms, :interval]
  end

  def acceptor_init(_, lsock, {dest, session, present, handler, opts}) do
    ref = :erlang.monitor(:port, lsock)
    {:ok, {dest, ref, session, present, handler, opts}}
  end

  def acceptor_continue(_, sock, arg) do
    {dest, ref, session, present, handler, opts} = arg
    Process.flag(:trap_exit, true)
    {man_opts, opts} = Keyword.split(opts, @options)
    pid = spawn_session(dest, ref, session, present, handler, opts)
    actions = [{:next_event, :internal, {:init, sock, man_opts}}]
    interval = Keyword.get(man_opts, :lease_interval, @lease_interval)
    data = %Data{session: pid, drain: ref, lease: make_ref(),
                 alarms: MapSet.new(), interval: interval}
    :gen_statem.enter_loop(__MODULE__, [], :init, data, self(), actions)
  end

  def acceptor_terminate(_, _),
    do: :ok

  def init(_),
    do: {:stop, :acceptor}

  def callback_mode(),
    do: [:state_functions, :state_enter]

  def init(:enter, :init, _data),
    do: :keep_state_and_data

  def init(:internal, {:init, sock, opts}, data) do
    %Data{session: pid, drain: ref} = data
    # give away and watch alarms inside loop for logging on error
    case :gen_tcp.controlling_process(sock, pid) do
      :ok ->
        send(pid, {:enter_loop, ref, sock})
        handle_alarms(opts, data)
      {:error, reason} ->
        {:stop, {:tcp_error, reason}}
    end
  end

  def drain(:enter, _, %Data{session: pid}) do
    Mux.Server.Connection.drain(pid)
    :keep_state_and_data
  end

  def drain(:info, msg, data),
    do: info(msg, data)

  def lease(:enter, _, %Data{interval: :infinity}),
    do: :keep_state_and_data

  def lease(:enter, _, %Data{session: pid, interval: interval}) do
    timeout = rand_interval(interval)
    Mux.Server.Connection.lease(pid, :millisecond, timeout)
    # try to grant a new lease in plenty of time before this one ends
    renew = div(2 * timeout, 3)
    {:keep_state_and_data, [{:state_timeout, renew, renew}]}
  end

  def lease(:state_timeout, _, data),
    do: handle_state_timeout(:lease, data)

  def lease(:info, msg, data),
    do: info(msg, data)

  def alarm(:enter, _, %Data{interval: interval}) do
    timeout = rand_interval(interval)
    {:keep_state_and_data, [{:state_timeout, timeout, timeout}]}
  end

  def alarm(:state_timeout, _, data),
    do: handle_state_timeout(:alarm, data)

  def alarm(:info, msg, data),
    do: info(msg, data)

  defp info({:DOWN, ref, _, _, _}, %Data{drain: ref} = data) do
    {:next_state, :drain, data}
  end
  defp info({:SET, ref, _}, %Data{drain: ref} = data) do
    {:next_state, :drain, data}
  end
  defp info({:CLEAR, ref, _}, %Data{drain: ref}) do
    # already draining or already think clear
    :keep_state_and_data
  end
  defp info({:SET, ref, id}, %Data{lease: ref, alarms: alarms} = data) do
    {:keep_state, %Data{data | alarms: MapSet.put(alarms, id)}}
  end
  defp info({:CLEAR, ref, id}, %Data{lease: ref, alarms: alarms} = data) do
    {:keep_state, %Data{data | alarms: MapSet.delete(alarms, id)}}
  end
  defp info({:EXIT, pid, reason}, %Data{session: pid}) do
    case reason do
      {:tcp_error, reason} ->
        # want transient exit for tcp_error's and no reporting (session will
        # have reported)
        {:stop, {:shutdown, reason}}
      reason ->
        # hopefully :normal for clean stop but could be crash
        {:stop, reason}
    end
  end
  defp info(msg, _) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    :keep_state_and_data
  end

  def terminate(_, _, %Data{session: pid}) do
    # monitor as pid could have exited and so :noproc
    mon = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    receive do
      {:DOWN, ^mon, _, _, _} ->
        :ok
    end
  end

  ## Helpers

  defp handle_alarms(opts, data) do
    %Data{lease: lease_ref, drain: drain_ref, interval: interval,
          session: pid} = data
    {alarms, drain_set} = watch_alarms(lease_ref, drain_ref, opts)
    data = %Data{data | alarms: alarms}
    cond do
      MapSet.size(drain_set) > 0 ->
        {:next_state, :drain, data}
      MapSet.size(alarms) > 0 and interval != :infinity ->
        # send a 0 lease so peer knows there are leases and it doesn't have one
        Mux.Server.Connection.lease(pid, :millisecond, 0)
        {:next_state, :alarm, data}
      true ->
        {:next_state, :lease, data}
    end
  end

  defp watch_alarms(lease_ref, drain_ref, opts) do
    lease_alarms = Keyword.get(opts, :lease_alarms, @lease_alarms)
    drain_alarms = Keyword.get(opts, :drain_alarms, @drain_alarms)
    {register_alarms(lease_ref, lease_alarms),
      register_alarms(drain_ref, drain_alarms)}
  end

  defp register_alarms(ref, alarms) do
    # register returns true if alarm is set
    alarms
    |> Enum.filter(&Mux.Alarm.register(Mux.Alarm, &1, ref))
    |> Enum.into(MapSet.new())
  end

  defp spawn_session(dest, ref, session, present, handler, opts) do
    {spawn_opts, opts} = Keyword.split(opts, [:spawn_opt])
    spawn_args = [ref, dest, session, present, handler, opts]
    spawn_opts = [:link | spawn_opts]
    :proc_lib.spawn_opt(__MODULE__, :init_session, spawn_args, spawn_opts)
  end

  def init_session(ref, dest, session, present, handler, opts) do
    {module, _} = handler
    {:ok, _} = Registry.register(Mux.Server.Connection, dest, module)
    receive do
      {:enter_loop, ^ref, sock} ->
        arg = {session, present, handler}
        Mux.Server.Connection.enter_loop(Mux.Server.Delegator, sock, arg, opts)
    end
  end

  defp handle_state_timeout(state, data) do
    case next_state(data) do
      ^state ->
        :repeat_state_and_data
      next_state ->
        {:next_state, next_state, data}
    end
  end

  defp next_state(%Data{alarms: alarms}) do
    case MapSet.size(alarms) do
      0 ->
        :lease
      _ ->
        :alarm
    end
  end

  defp rand_interval(interval) do
    # randomise uniform around [0.5 interval, 1.5 interval]
    div(interval, 2) + :rand.uniform(interval + 1) - 1
  end
end
