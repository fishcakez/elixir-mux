defmodule Mux.Server.Manager do
  @moduledoc false

  use GenServer

  @behaviour :acceptor

  def acceptor_init(_, lsock, {module, dest, arg, opts}) do
    ref = :erlang.monitor(:port, lsock)
    {:ok, {module, dest, ref, arg, opts}}
  end

  def acceptor_continue(_, sock, {module, dest, ref, arg, opts}) do
    Process.flag(:trap_exit, true)
    pid = spawn_worker(module, dest, ref, arg, opts)
    :ok = :gen_tcp.controlling_process(sock, pid)
    send(pid, {:enter_loop, ref, sock})
    :gen_server.enter_loop(__MODULE__, [], {pid, ref})
  end

  def acceptor_terminate(_, _),
    do: :ok

  def handle_info({:DOWN, ref, _, _, _}, {pid, ref} = state) do
    Mux.ServerSession.drain(pid)
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, reason}, {pid, _} = state) do
    case reason do
      {:tcp_error, reason} ->
        # want transient exit for tcp_error's and no reporting (session will
        # have reported)
        {:stop, {:shutdown, reason}, state}
      reason ->
        # hopefully :normal for clean stop but could be crash
        {:stop, reason, state}
    end
  end
  def handle_info(msg, state) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    {:noreply, state}
  end

  def terminate(_, {pid, _}) do
    # monitor as pid could have exited and so :noproc
    mon = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    receive do
      {:DOWN, ^mon, _, _, _} ->
        :ok
    end
  end

  ## Helpers

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
