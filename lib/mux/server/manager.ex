defmodule Mux.Server.Manager do
  @moduledoc false

  @behaviour :gen_statem
  @behaviour :acceptor

  @drain_alarms []

  def acceptor_init(_, lsock, {module, dest, arg, opts}) do
    ref = :erlang.monitor(:port, lsock)
    {:ok, {module, dest, ref, arg, opts}}
  end

  def acceptor_continue(_, sock, {module, dest, ref, arg, opts}) do
    Process.flag(:trap_exit, true)
    {man_opts, opts} = Keyword.split(opts, [:drain_alarms])
    pid = spawn_worker(module, dest, ref, arg, opts)
    actions = [{:next_event, :internal, {:init, sock, man_opts}}]
    :gen_statem.enter_loop(__MODULE__, [], :init, {pid, ref}, self(), actions)
  end

  def acceptor_terminate(_, _),
    do: :ok

  def init(_),
    do: {:stop, :acceptor}

  def callback_mode(),
    do: [:state_functions, :state_enter]

  def init(:enter, :init, _data),
    do: :keep_state_and_data

  def init(:internal, {:init, sock, opts}, {pid, ref} = data) do
    # give away inside loop for logging on error
    case :gen_tcp.controlling_process(sock, pid) do
      :ok ->
        send(pid, {:enter_loop, ref, sock})
        state = watch_alarms(ref, opts)
        {:next_state, state, data}
      {:error, reason} ->
        {:stop, {:tcp_error, reason}}
    end
  end

  def drain(:enter, _, {pid, _}) do
    Mux.ServerSession.drain(pid)
    :keep_state_and_data
  end

  def drain(:info, msg, data),
    do: info(msg, data)

  def lease(:enter, _, _),
    do: :keep_state_and_data

  def lease(:info, msg, data),
    do: info(msg, data)

  defp info({:DOWN, ref, _, _, _}, {_, ref} = data) do
    {:next_state, :drain, data}
  end
  defp info({:SET, ref, _}, {_, ref} = data) do
    {:next_state, :drain, data}
  end
  defp info({:CLEAR, ref, _}, {_, ref}) do
    # already draining or don't want to lease immediately as would allow on all
    :keep_state_and_data
  end
  defp info({:EXIT, pid, reason}, {pid, _}) do
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

  def terminate(_, _, {pid, _}) do
    # monitor as pid could have exited and so :noproc
    mon = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    receive do
      {:DOWN, ^mon, _, _, _} ->
        :ok
    end
  end

  ## Helpers

  defp watch_alarms(ref, opts) do
    alarms = Keyword.get(opts, :drain_alarms, @drain_alarms)
    # register returns true if alarm is set
    if Enum.any?(alarms, &Mux.Alarm.register(Mux.Alarm, &1, ref)) do
      :drain
    else
      :lease
    end
  end

  defp spawn_worker(module, dest, ref, arg, opts) do
    {spawn_opts, opts} = Keyword.split(opts, [:spawn_opt])
    spawn_args = [module, dest, ref, arg, opts]
    :proc_lib.spawn_opt(__MODULE__, :init_worker, spawn_args, spawn_opts)
  end

  def init_worker(module, dest, ref, args, opts) do
    {:ok, _} = Registry.register(Mux.ServerSession, dest, module)
    receive do
      {:enter_loop, ^ref, sock} ->
        Mux.ServerSession.enter_loop(module, sock, args, opts)
    end
  end
end
