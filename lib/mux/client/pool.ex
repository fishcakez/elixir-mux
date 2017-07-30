defmodule Mux.Client.Pool do
  @moduledoc false

  use Supervisor

  @type option ::
    Mux.ClientSession.option |
    {:spawn_opt, [:proc_lib.spawn_option]}

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

  @spec start_session(pid, :gen_tcp.socket) ::
    {:ok, pid, reference} | {:error, reason :: any}
  def start_session(pool, sock) do
    ref = make_ref()
    case Supervisor.start_child(pool, [ref]) do
      {:ok, pid} ->
       start_session(pool, pid, sock, ref)
      {:error, _} = error ->
        error
    end
  end

  def init({module, dest, arg, opts}) do
    # only register on init and not code change
    if Registry.keys(__MODULE__, self()) == [] do
      Registry.register(__MODULE__, dest, module)
    end
    session =
      %{id: {dest, Mux.ClientSession},
        start: {__MODULE__, :spawn_session, [module, dest, arg, opts]},
        type: :worker, restart: :temporary}
    Supervisor.init([session], [strategy: :simple_one_for_one])
  end

  defp start_session(pool, pid, sock, ref) do
    case :gen_tcp.controlling_process(sock, pid) do
      :ok ->
        mon = Process.monitor(pid)
        send(pid, {:enter_loop, ref, sock})
        {pid, mon}
      {:error, _} = error ->
        _ = Supervisor.terminate_child(pool, pid)
        error
    end
  end

  def spawn_session(module, dest, arg, opts, ref) do
    {spawn_opts, opts} = Keyword.split(opts, [:spawn_opt])
    spawn_args = [module, dest, ref, arg, opts]
    spawn_opts = [:link | spawn_opts]
    pid = :proc_lib.spawn_opt(__MODULE__, :init_session, spawn_args, spawn_opts)
    {:ok, pid}
  end

  def init_session(module, dest, ref, args, opts) do
    {:ok, _} = Registry.register(Mux.ClientSession, dest, module)
    receive do
      {:enter_loop, ^ref, sock} ->
        Mux.ClientSession.enter_loop(module, sock, args, opts)
    end
  end
end
