defmodule Mux.Server.Socket do
  @moduledoc false

  use GenServer

  @listen_options [active: false, reuseaddr: true]

  @spec child_spec({Mux.Packet.dest, :inet.port_number, Keyword.t}) ::
    Supervisor.child_spec
  def child_spec({dest, port, opts}) do
    %{id: {dest, __MODULE__},
      start: {__MODULE__, :start_link, [dest, port, opts]},
      type: :worker}
  end

  def start_link(dest, port, opts) do
    name = {:via, Registry, {__MODULE__, dest}}
    GenServer.start_link(__MODULE__, {dest, port, opts}, [name: name])
  end

  def init({dest, port, opts}) do
    # trap exit so can sync close socket on terminate
    Process.flag(:trap_exit, true)
    case :gen_tcp.listen(port, @listen_options ++ opts) do
      {:ok, sock} = ok ->
        {:ok, sockname} = :inet.sockname(sock)
        _ = Registry.update_value(Mux.Server.Socket, dest, fn _ -> sockname end)
        {:ok, _} = Mux.Server.Pool.accept_socket(dest, sock)
        ok
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_info({:EXIT, sock, reason}, sock) do
    {:stop, reason, sock}
  end
  def handle_info(msg, sock) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    {:noreply, sock}
  end

  def terminate(_, sock),
    do: :gen_tcp.close(sock)
end
