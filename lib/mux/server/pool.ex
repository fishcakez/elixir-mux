defmodule Mux.Server.Pool do
  @moduledoc false

  @behaviour :acceptor_pool

  @session {Mux.ServerSession.Default, nil}
  @presentation {Mux.ServerPresentation.Default, nil}

  @spec child_spec({module, Mux.Packet.dest, any, Keyword.t}) ::
    Supervisor.child_spec
  def child_spec({module, dest, arg, opts}) do
    %{id: {dest, __MODULE__},
      start: {__MODULE__, :start_link, [module, dest, arg, opts]},
      type: :supervisor}
  end

  @spec accept_socket(Mux.Packet.dest, :gen_tcp.socket) :: {:ok, reference}
  def accept_socket(dest, sock) do
    name = {:via, Registry, {__MODULE__, dest}}
    acceptors = System.schedulers_online()
    :acceptor_pool.accept_socket(name, sock, acceptors)
  end

  @spec start_link(module, Mux.Packet.dest, any, Keyword.t) ::
    {:ok, pid} | {:error, reason :: any}
  def start_link(module, dest, arg, opts) do
    name = {:via, Registry, {__MODULE__, dest}}
    :acceptor_pool.start_link(name, __MODULE__, {module, dest, arg, opts})
  end

  def init({module, dest, arg, opts}) do
    {restart, opts} = Keyword.pop(opts, :restart, :temporary)
    {grace, opts} = Keyword.pop(opts, :grace, 5_000)
    {max_restarts, opts} = Keyword.pop(opts, :max_restarts, 3)
    {max_seconds, opts} = Keyword.pop(opts, :max_seconds, 5)

    flags = %{intensity: max_restarts, period: max_seconds}

    {session, opts} = pop(opts, :session, @session)
    {present, opts} = pop(opts, :presentation, @presentation)
    man_arg = {dest, session, present, {module, arg}, opts}

    spec = %{id: {dest, module},
             start: {Mux.Server.Manager, man_arg, []},
             type: :supervisor,
             restart: restart,
             grace: grace}

    {:ok, {flags, [spec]}}
  end

  defp pop(opts, key, default) do
    case Keyword.pop(opts, key, default) do
      {{_mod, _arg}, _opts} = result ->
        result
      {bad, _opts} ->
        raise ArgumentError,
          "expected {module, arg} for #{key}, got: #{inspect bad}"
    end
  end
end
