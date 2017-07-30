defmodule Mux.ClientSession do
  @moduledoc """
  Client loop for a Mux session.

  This module defines a TCP Mux client that multiplexes dispatches over a
  single session.
  """

  @behaviour Mux.Connection

  defmodule State do
    @moduledoc false
    @enforce_keys [:tags, :exchanges, :monitors, :refs, :socket, :timer,
                   :handler, :handshake]
    defstruct [:tags, :exchanges, :monitors, :refs, :socket, :timer,
               :handler, :handshake]
  end

  @discarded_message "process discarded"
  @handshake_timeout 5_000
  @handshake_tag 1
  @mux_version 1
  @session_size 32
  @frame_size 0xFFFF
  @ping_interval 5_000
  @ping_tag 1

  @type state :: any
  @type client :: Mux.Connection.connection
  @type option ::
    Mux.Connection.option |
    {:handshake, {module, any}} |
    {:handshake_timeout, timeout}

  @type session_option ::
    {:session_size, pos_integer} |
    {:frame_size, pos_integer} |
    {:ping_interval, timeout}

  @type result ::
    {:ok, Mux.Packet.context, body :: binary} |
    {:error, Mux.Packet.context, %Mux.ApplicationError{}} |
    {:nack, Mux.Packet.context} |
    {:error, %Mux.ServerError{}}

  @callback init(state) :: {:ok, state}

  @callback nack(Mux.Packet.context, Mux.Packet.dest, Mux.Packet.dest_table,
            body :: binary, state) :: {:nack, Mux.Packet.context}

  @callback lease(System.time_unit, timeout, state) :: {:ok, state}

  @callback drain(state) :: {:ok, state}

  @callback terminate(reason :: any, state) :: any

  @spec sync_dispatch(client, Mux.Packet.context, Mux.Packet.dest,
        Mux.Packet.dest_table, body :: binary, timeout) :: result
  def sync_dispatch(client, context, dest, tab, body, timeout \\ 5_000) do
    # call will use a middleman process that exits on timeout, so client will
    # monitor middleman (instead of caller) and discard when it exits on timeout
    Mux.Connection.call(client, {:dispatch, context, dest, tab, body}, timeout)
  end

  @spec dispatch(client, Mux.Packet.context, Mux.Packet.dest,
        Mux.Packet.dest_table, body :: binary) :: reference
  def dispatch(client, context, dest, tab, body) do
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

  @spec enter_loop(module, :gen_tcp.socket, state, [option]) :: no_return
  def enter_loop(mod, sock, state, opts) do
    {cli_opts, opts} = Keyword.split(opts, [:handshake, :handshake_timeout])
    args = {{mod, state}, sock, cli_opts}
    Mux.Connection.enter_loop(__MODULE__, sock, args, opts)
  end

  @doc false
  def init({handler, sock, opts}) do
    default = fn -> Application.fetch_env!(:mux, :client_handshake) end
    handshake = Keyword.get_lazy(opts, :handshake, default)
    timeout = Keyword.get(opts, :handshake_timeout, @handshake_timeout)
    {headers, handler, handshake} = pair_init(handler, handshake)
    state = %State{tags: :handshake, exchanges: %{}, monitors: %{}, refs: %{},
                   handler: handler, handshake: handshake, socket: sock,
                   timer: {@handshake_tag, start_timer(timeout)}}
    {[{:send, @handshake_tag, {:transmit_init, @mux_version, headers}}], state}
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
  def handle(:info, {:timeout, tref, interval}, %State{timer: tref} = state) do
    handle_ping(interval, state)
  end
  def handle(:info, {:timeout, tref, _}, %State{timer: {_tag, tref}}) do
    # no response from last ping or handshake
    exit({:tcp_error, :timeout})
  end
  def handle(:info, :shutdown_write, state) do
    {[], check_drain(state)}
  end
  def handle(:info, msg, state) do
    :error_logger.error_msg("Mux.Client received unexpected message: ~p~n",
                            [msg])
    {[], state}
  end

  @doc false
  def terminate(:normal, %State{exchanges: exchanges} = state)
      when map_size(exchanges) > 0 do
    # awaiting a responses so not a clean stop
    reason = {:tcp_error, :closed}
    terminate(reason, state)
    exit(reason)
  end
  def terminate(reason, %State{handler: handler, handshake: handshake}),
    do: pair_terminate(reason, handler, handshake)

  # 0 tag is a one way
  defp handle_packet(0, {:transmit_lease, unit, timeout}, state),
    do: handle_lease(unit, timeout, state)
  defp handle_packet(0, _cast, state),
    do: {[], state}
  # ping came back before next ping was due to be sent
  defp handle_packet(tag, :receive_ping, %State{timer: {tag, tref}} = state),
    do: {[], %State{state | timer: tref}}
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
        handle_drain(tag, state)
      :transmit_ping ->
        {[receive_ping(tag)], state}
      {:transmit_init, _, _} ->
        # don't expect server to send transmit_init so ok to error
        {[receive_error(tag, "can not handle init")], state}
      {:receive_init, @mux_version, headers} ->
        # only support @mux_version
        handle_init(tag, headers, state)
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
      {nil, %State{timer: {^tag, _}}} ->
        # init/ping got an error back?!
        raise Mux.ServerError, why
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

  defp reply(from, :error, context, why),
    do: {:reply, from, {:error, context, Mux.ApplicationError.exception(why)}}

  defp reply(from, :nack, context, _body),
    do: reply_nack(from, context)

  defp reply_error(from, err),
    do: {:reply, from, {:error, err}}

  defp reply_nack(from, context),
    do: {:reply, from, {:nack, context}}

  defp receive_pop(tag, state) do
    %State{tags: tags, exchanges: exchanges, monitors: monitors,
          refs: refs} = state
    tags = tags_put(tags, tag)
    case Map.pop(exchanges, tag) do
      {{{_, ref} = from, mon}, exchanges} ->
        Process.demonitor(mon, [:flush])
        monitors = Map.delete(monitors, mon)
        refs = Map.delete(refs, ref)
        state = %State{state | tags: tags, exchanges: exchanges,
                               monitors: monitors, refs: refs}
        {from, check_drain(state)}
      {:discarded, exchanges} ->
        state = %State{state | tags: tags, exchanges: exchanges}
        {nil, check_drain(state)}
      {nil, _} ->
        {nil, state}
    end
  end

  # if drain and empty exchanges it's time to close the socket
  def check_drain(%State{tags: drain, exchanges: exchanges} = state)
      when drain in [:handshake_drain, :drain] do
    case map_size(exchanges) do
      0 ->
        shutdown_write(state)
      _ ->
        state
    end
  end
  def check_drain(state),
    do: state

  defp shutdown_write(%State{socket: socket} = state) do
    # exchanges is empty so no dispatches in send queue, shutdown write side and
    # wait for server to close
    _ = :gen_tcp.shutdown(socket, :write)
    state
  end

  defp receive_error(tag, why),
    do: {:send, tag, {:receive_error, why}}

  defp receive_ping(tag),
    do: {:send, tag, :receive_ping}

  defp handle_dispatch(from, context, dest, tab, body, state) do
    %State{tags: tags, exchanges: exchanges, monitors: monitors,
           refs: refs, handler: handler} = state
    case tags_pop(tags) do
      {nil, _} ->
        {:nack, context} = handler_nack(context, dest, tab, body, handler)
        {[reply_nack(from, context)], state}
      {tag, tags} ->
        {pid, ref} = from
        mon = Process.monitor(pid)
        exchanges = Map.put(exchanges, tag, {from, mon})
        monitors = Map.put(monitors, mon, {tag, ref})
        refs = Map.put(refs, ref, {tag, mon})
        state = %State{state | tags: tags, exchanges: exchanges,
                               monitors: monitors, refs: refs}
        {[transmit_dispatch(tag, context, dest, tab, body)], state}
    end
  end

  defp handle_cancel(ref, why, state) do
    %State{exchanges: exchanges, monitors: monitors, refs: refs} = state
    case Map.pop(refs, ref) do
      {nil, _} ->
        {:error, [], state}
      {{tag, mon}, refs} ->
        exchanges = Map.put(exchanges, tag, :discarded)
        monitors = Map.delete(monitors, mon)
        state = %State{state | exchanges: exchanges, monitors: monitors,
                               refs: refs}
        {:ok, [transmit_discarded(tag, why)], state}
    end
  end

  defp handle_down(mon, state) do
    %State{exchanges: exchanges, monitors: monitors, refs: refs} = state
    case Map.pop(monitors, mon) do
      {nil, _} ->
        {[], state}
      {{tag, ref}, monitors} ->
        exchanges = Map.put(exchanges, tag, :discarded)
        refs = Map.delete(refs, ref)
        state = %State{state | exchanges: exchanges, monitors: monitors,
                               refs: refs}
        {[transmit_discarded(tag, @discarded_message)], state}
    end
  end

  defp handle_init(tag, headers, state) do
    case state do
      %State{tags: :handshake, timer: {^tag, tref}} ->
        cancel_timer(tref)
        handle_handshake(headers, state)
      %State{tags: :handshake_drain, timer: {^tag, tref}} ->
        cancel_timer(tref)
        {actions, state} = handle_handshake(headers, state)
        {actions, %State{state | tags: :drain}}
      state ->
        {[receive_error(tag, "unexpected rinit")], state}
    end
  end

  defp handle_handshake(headers, %State{handshake: handshake} = state) do
    {opts, handshake} = handshake(headers, handshake)

    session_size = Keyword.get(opts, :session_size, @session_size)
    ping_interval = Keyword.get(opts, :ping_interval, @ping_interval)
    frame_size = Keyword.get(opts, :frame_size, @frame_size)

    state = %State{state | handshake: handshake, tags: tags_new(session_size),
                           timer: start_interval(ping_interval)}
    {[frame_size: frame_size], state}
  end

  defp pair_init({mod1, state1}, {mod2, state2}) do
    {:ok, state1} = apply(mod1, :init, [state1])
    try do
      {:ok, _headers, _state2} = apply(mod2, :init, [state2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        # undo mod1.init/1 as terminate/2 won't be called
        apply(mod1, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, headers, state2} ->
        {headers, {mod1, state1}, {mod2, state2}}
    end
  end

  defp handshake(headers, {mod, state}) do
    {:ok, opts, state} = apply(mod, :handshake, [headers, state])
    {opts, {mod, state}}
  end

  defp handler_nack(context, dest, dest_table, body, {mod, state}),
    do: apply(mod, :nack, [context, dest, dest_table, body, state])

  defp handler_lease(unit, timeout, {mod, state}) do
    {:ok, state} = apply(mod, :lease, [unit, timeout, state])
    {mod, state}
  end

  defp handler_drain({mod, state}) do
    {:ok, state} = apply(mod, :drain, [state])
    {mod, state}
  end

  defp pair_terminate(reason, {mod1, state1}, {mod2, state2}) do
    # reverse order of pair_init/2
    try do
      apply(mod2, :terminate, [reason, state2])
    after
      apply(mod1, :terminate, [reason, state1])
    end
  end

  defp handle_ping(ping_interval, state) do
    state = %State{state | timer: {@ping_tag, start_interval(ping_interval)}}
    {[transmit_ping(@ping_tag)], state}
  end

  defp handle_lease(unit, timeout, %State{handler: handler} = state),
    do: {[], %State{state | handler: handler_lease(unit, timeout, handler)}}

  defp handle_drain(tag, %State{tags: :handshake} = state),
    do: handle_drain(tag, :handshake_drain, state)
  defp handle_drain(tag, state),
    do: handle_drain(tag, :drain, state)

  # set fake tags so no new tags can be assigned and so no new dispatches
  defp handle_drain(tag, tags, state) do
    %State{handler: handler, exchanges: exchanges} = state
    handler = handler_drain(handler)
    if map_size(exchanges) == 0 do
        # delay shutting down write so that receive_drain is sent to server
        # before shutting down socket
        send(self(), :shutdown_write)
    end
    {[receive_drain(tag)], %State{state | tags: tags, handler: handler}}
  end

  defp transmit_dispatch(tag, context, dest, tab, body),
    do: {:send, tag, {:transmit_dispatch, context, dest, tab, body}}

  defp transmit_discarded(tag, why),
    do: {:send, 0, {:transmit_discarded, tag, why}}

  defp transmit_ping(tag),
    do: {:send, tag, :transmit_ping}

  defp receive_drain(tag),
    do: {:send, tag, :receive_drain}

  # start from 2 as 1 is used for pings
  defp tags_new(session_size) do
    2..session_size+1
    |> Enum.to_list()
    |> :gb_sets.from_list()
  end

  defp tags_put(status, _) when is_atom(status),
    do: status
  defp tags_put(tags, tag),
    do: :gb_sets.add_element(tag, tags)

  defp tags_pop(status) when is_atom(status),
    do: {nil, status}
  defp tags_pop(tags) do
    if :gb_sets.is_empty(tags) do
      {nil, tags}
    else
      # should reuse smallest tags
      :gb_sets.take_smallest(tags)
    end
  end

  defp start_timer(:infinity),
    do: make_ref()
  defp start_timer(timeout),
    do: :erlang.start_timer(timeout, self(), timeout)

  defp start_interval(:infinity),
    do: make_ref()
  defp start_interval(interval) do
    # randomise uniform around [0.5 interval, 1.5 interval]
    delay = div(interval, 2) + :rand.uniform(interval + 1) - 1
    :erlang.start_timer(delay, self(), interval)
  end

  defp cancel_timer(tref) do
    if :erlang.cancel_timer(tref) do
      :ok
    else
      receive do
        {:timeout, ^tref, _} ->
          :ok
      after
        0 ->
          :ok
      end
    end
  end
end
