defmodule Mux.Server.Socket do
  @moduledoc false

  use GenServer

  @listen_options [active: false, reuseaddr: true]

  def start_link({port, opts}) do
    parent = self()
    GenServer.start_link(__MODULE__, {parent, port, opts})
  end

  def init({parent, port, opts}) do
    # trap exit so can sync close socket on terminate
    Process.flag(:trap_exit, true)
    case :gen_tcp.listen(port, @listen_options ++ opts) do
      {:ok, sock} = ok ->
        send(self(), {:accept_socket, parent, sock})
        ok
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_info({:accept_socket, parent, sock}, sock) do
    children = Supervisor.which_children(parent)
    {_, pool, _, _} = List.keyfind(children, Mux.Server.Pool, 0)
    acceptors = System.schedulers_online()
    {:ok, _} = :acceptor_pool.accept_socket(pool, sock, acceptors)
    {:noreply, sock}
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
