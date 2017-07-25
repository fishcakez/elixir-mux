defmodule Mux.Client do
  @moduledoc """
  Client loop for a Mux session.

  This module defines a TCP Mux client that multiplexes dispatches over a
  single session.
  """

  @behaviour Mux.Connection

  defmodule State do
    @moduledoc false
    @enforce_keys [:tags, :exchanges, :monitors, :clients]
    defstruct [:tags, :exchanges, :monitors, :clients]
  end

  @discarded_message "process discarded"

  @type client :: Mux.Connection.connection
  @type option :: Mux.Connection.option | {:session_size, pos_integer}

  @type result ::
    {:ok, Mux.Packet.context, body :: binary} |
    {:error, %Mux.ApplicationError{} | %Mux.ServerError{}} |
    :nack

  @spec sync_dispatch(client, Mux.Packet.context, Mux.Packet.dest,
        Mux.Packet.dest_table, body :: binary, timeout) :: result
  def sync_dispatch(client, context, dest, tab, body, timeout \\ 5_000) do
    # call will use a middleman process that exits on timeout, so client will
    # monitor middleman (instead of caller) and discard when it exits on timeout
    Mux.Connection.call(client, {:dispatch, context, dest, tab, body}, timeout)
  end

  @spec async_dispatch(client, Mux.Packet.context, Mux.Packet.dest,
        Mux.Packet.dispatch, body :: binary) :: reference
  def async_dispatch(client, context, dest, tab, body) do
    ref = make_ref()
    from = {self(), ref}
    Mux.Connection.cast(client, {:dispatch, from, context, dest, tab, body})
    ref
  end

  @spec cancel(client, reference, why :: String.t, timeout) ::
    :ok | {:error, :not_found}
  def cancel(client, ref, why \\ @discarded_message, timeout \\ 5_000) do
    Mux.Connection.call(client, {:cancel, ref, why}, timeout)
  end

  @spec async_cancel(client, reference, why :: String.t) :: :ok
  def async_cancel(client, ref, why \\ @discarded_message) do
    Mux.Connection.cast(client, {:cancel, ref, why})
  end

  @spec enter_loop(:gen_tcp.socket, [option]) :: no_return
  def enter_loop(socket, opts) do
    {session_size, conn_opts} = Keyword.pop(opts, :session_size, 32)
    Mux.Connection.enter_loop(__MODULE__, socket, session_size, conn_opts)
  end

  @doc false
  def init(session_size) when session_size > 0 and session_size < 0x80_FF_FF do
    tags = tags_new(1..session_size)
    {[], %State{tags: tags, exchanges: %{}, monitors: %{}, clients: %{}}}
  end

  @doc false
  def handle({:packet, tag}, packet, state) do
    handle_packet(tag, packet, state)
  end
  def handle({:call, from}, {:dispatch, context, dest, tab, body}, state) do
    handle_dispatch(from, context, dest, tab, body, state)
  end
  def handle(:cast, {:dispatch, from, context, dest, tab, body}, state) do
    handle_dispatch(from, context, dest, tab, body, state)
  end
  def handle(:cast, {:cancel, ref, why}, state) do
    {_, cmds, state} = handle_cancel(ref, why, state)
    {cmds, state}
  end
  def handle({:call, from}, {:cancel, ref, why}, state) do
    case handle_cancel(ref, why, state) do
      {:ok, cmds, state} ->
        {[{:reply, from, :ok} | cmds], state}
      {:error, cmds, state} ->
        {[{:reply, from, {:error, :not_found}} | cmds], state}
      end
  end
  def handle(:info, {:DOWN, mon, _, _, _}, state) do
    handle_down(mon, state)
  end
  def handle(:info, msg, state) do
    :error_logger.error_msg("Mux.Client received unexpected message: ~p~n",
                            [msg])
    {[], state}
  end

  @doc false
  def terminate(_, _),
    do: :ok

  # 0 tag is a one way, this could be a lease but don't handle that yet, the
  # server will likely nack at some point if it requires a lease
  defp handle_packet(0, _cast, state),
    do: {[], state}
  defp handle_packet(tag, packet, state) do
    case packet do
      {:receive_dispatch, status, context, body} ->
        handle_result(tag, status, context, body, state)
      :receive_discarded ->
        handle_discarded(tag, state)
      {:receive_error, why} ->
        handle_error(tag, why, state)
      {:transmit_request, _, _} ->
        # this is a client only
        {[receive_error(tag, "can not handle request")], state}
      {:transmit_dispatch, _, _, _, _} ->
        # this is a client only
        {[receive_error(tag, "can not handle dispatch")], state}
      :transmit_drain ->
        # should handle drain
        {[receive_error(tag, "can not handle drain")], state}
      :transmit_ping ->
        {[receive_ping(tag)], state}
      {:transmit_init, _, _} ->
        # don't expect server to send transmit_init so ok to error
        {[receive_error(tag, "can not handle init")], state}
    end
  end

  defp handle_result(tag, status, context, body, state) do
    case receive_pop(tag, state) do
      {nil, state} ->
        {[], state}
      {from, state} ->
        {[reply(from, status, context, body)], state}
    end
  end

  defp handle_error(tag, why, state) do
    case receive_pop(tag, state) do
      {nil, state} ->
        {[], state}
      {from, state} ->
        err = Mux.ServerError.exception(why)
        {[reply_error(from, err)], state}
    end
  end

  defp handle_discarded(tag, state) do
    case receive_pop(tag, state) do
      {nil, state} ->
        {[], state}
      {from, state} ->
        # server discarded an exchange that we didn't discard?
        err = Mux.ServerError.exception("server discarded")
        {[reply_error(from, err)], state}
    end
  end

  defp reply(from, :ok, context, body),
    do: {:reply, from, {:ok, context, body}}

  defp reply(from, :error, _context, why),
    do: reply_error(from, Mux.ApplicationError.exception(why))

  defp reply(from, :nack, _context, _body),
    do: reply_nack(from)

  defp reply_error(from, err),
    do: {:reply, from, {:error, err}}

  defp reply_nack(from),
    do: {:reply, from, :nack}

  defp receive_pop(tag, state) do
    %State{tags: tags, exchanges: exchanges, monitors: monitors,
          clients: clients} = state
    tags = tags_put(tags, tag)
    case Map.pop(exchanges, tag) do
      {{{_, ref} = from, mon}, exchanges} ->
        Process.demonitor(mon, [:flush])
        monitors = Map.delete(monitors, mon)
        clients = Map.delete(clients, ref)
        state = %State{state | tags: tags, exchanges: exchanges,
                               monitors: monitors, clients: clients}
        {from, state}
      {:discarded, exchanges} ->
        {nil, %State{state | tags: tags, exchanges: exchanges}}
    end
  end

  defp receive_error(tag, why),
    do: {:send, tag, {:receive_error, why}}

  defp receive_ping(tag),
    do: {:send, tag, :receive_ping}

  defp handle_dispatch(from, context, dest, tab, body, state) do
    %State{tags: tags, exchanges: exchanges, monitors: monitors,
          clients: clients} = state
    case tags_pop(tags) do
      {nil, _} ->
        {[reply_nack(from)], state}
      {tag, tags} ->
        {pid, ref} = from
        mon = Process.monitor(pid)
        exchanges = Map.put(exchanges, tag, {from, mon})
        mon = Process.monitor(pid)
        monitors = Map.put(monitors, mon, {tag, ref})
        clients = Map.put(clients, ref, {tag, mon})
        state = %State{state | tags: tags, exchanges: exchanges,
                               monitors: monitors, clients: clients}
        {[transmit_dispatch(tag, context, dest, tab, body)], state}
    end
  end

  defp handle_cancel(ref, why, state) do
    %State{exchanges: exchanges, monitors: monitors, clients: clients} = state
    case Map.pop(clients, ref) do
      {nil, _} ->
        {:error, [], state}
      {{tag, mon}, clients} ->
        exchanges = Map.put(exchanges, tag, :discarded)
        monitors = Map.delete(monitors, mon)
        state = %State{state | exchanges: exchanges, monitors: monitors,
                               clients: clients}
        {:ok, [transmit_discarded(tag, why)], state}
    end
  end

  defp handle_down(mon, state) do
    %State{exchanges: exchanges, monitors: monitors, clients: clients} = state
    case Map.pop(monitors, mon) do
      {nil, _} ->
        {[], state}
      {{tag, ref}, monitors} ->
        exchanges = Map.put(exchanges, tag, :discarded)
        clients = Map.delete(clients, ref)
        state = %State{state | exchanges: exchanges, monitors: monitors,
                               clients: clients}
        {[transmit_discarded(tag, @discarded_message)], state}
    end
  end

  defp transmit_dispatch(tag, context, dest, tab, body),
    do: {:send, tag, {:transmit_dispatch, context, dest, tab, body}}

  defp transmit_discarded(tag, why),
    do: {:send, 0, {:transmit_discarded, tag, why}}

  defp tags_new(range) do
    range
    |> Enum.to_list()
    |> :gb_sets.from_list()
  end

  defp tags_put(tags, tag),
    do: :gb_sets.add_element(tag, tags)

  defp tags_pop(tags) do
    if :gb_sets.is_empty(tags) do
      {nil, tags}
    else
      # should reuse smallest tags
      :gb_sets.take_smallest(tags)
    end
  end
end