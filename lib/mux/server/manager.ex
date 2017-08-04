defmodule Mux.Server.Manager do
  @moduledoc false

  @behaviour :gen_statem
  @behaviour :acceptor

  @options [:drain_alarms]
  @drain_alarms []

  defmodule Data do
    @moduledoc false

    @enforce_keys [:session, :drain]
    defstruct [:session, :drain]
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
    data = %Data{session: pid, drain: ref}
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

  def open(:enter, _, _),
    do: :keep_state_and_data

  def open(:info, msg, data),
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

  defp handle_alarms(opts, %Data{drain: drain_ref} = data) do
    if register_alarms(drain_ref, opts) do
        {:next_state, :drain, data}
    else
        {:next_state, :open, data}
    end
  end

  defp register_alarms(ref, opts) do
    # register returns true if alarm is set
    opts
    |> Keyword.get(:drain_alarms, @drain_alarms)
    |> Enum.any?(&Mux.Alarm.register(Mux.Alarm, &1, ref))
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
end
