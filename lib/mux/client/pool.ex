defmodule Mux.Client.Pool do
  @moduledoc false

  use Supervisor

  @type option ::
    Mux.Client.Connection.option |
    {:spawn_opt, [:proc_lib.spawn_option]}

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

  def init({dest, opts}) do
    {session, opts} = pop!(opts, :session)
    {{module, _arg} = present, opts} = pop!(opts, :presentation)
    # only register on init and not code change
    if Registry.keys(__MODULE__, self()) == [] do
      Registry.register(__MODULE__, dest, module)
    end
    args = [dest, session, present, opts]
    session =
      %{id: {dest, Mux.Client.Connection},
        start: {__MODULE__, :spawn_session, args},
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

  defp pop!(opts, key) do
    case Keyword.pop(opts, key) do
      {nil, _opts} ->
        raise ArgumentError, "#{key} callback not specified"
      {{_mod, _arg}, _opts} = result ->
        result
      {bad, _opts} ->
        raise ArgumentError,
          "expected {module, arg} for #{key}, got: #{inspect bad}"
    end
  end

  def spawn_session(dest, session, present, opts, ref) do
    {spawn_opts, opts} = Keyword.split(opts, [:spawn_opt])
    spawn_args = [ref, dest, session, present, opts]
    spawn_opts = [:link | spawn_opts]
    pid = :proc_lib.spawn_opt(__MODULE__, :init_session, spawn_args, spawn_opts)
    {:ok, pid}
  end

  def init_session(ref, dest, session, presenation, opts) do
    receive do
      {:enter_loop, ^ref, sock} ->
        arg = {dest, session, presenation}
        Mux.Client.Connection.enter_loop(Mux.Client.Delegator, sock, arg, opts)
    end
  end
end
