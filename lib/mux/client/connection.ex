defmodule Mux.Client.Connection do
  @moduledoc false

  @behaviour Mux.Connection

  defmodule State do
    @moduledoc false
    @enforce_keys [:tags, :exchanges, :monitors, :refs, :socket, :timer,
                   :handler, :lease]
    defstruct [:tags, :exchanges, :monitors, :refs, :socket, :timer,
               :handler, :lease]
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
    {:handshake_timeout, timeout}

  @type session_option ::
    {:session_size, pos_integer} |
    {:frame_size, pos_integer} |
    {:ping_interval, timeout}

  @type result ::
    {:ok, body :: binary} |
    {:error, %Mux.ApplicationError{}} |
    :nack |
    {:error, %Mux.ServerError{}}

  @type sync_timeout ::
    timeout |
    {:clean_timeout, timeout} |
    {:dirty_timeout, timeout}

  @callback init(state) ::
    {:ok, Mux.Packet.headers, state}

  @callback handshake(Mux.Packet.headers, state) ::
    {:ok, [session_option], state}

  @callback lease(System.time_unit, timeout, state) :: {:ok, state}

  @callback drain(state) :: {:ok, state}

  @callback terminate(reason :: any, state) :: any

  @spec sync_dispatch(client, Mux.Packet.dest, Mux.Packet.dest_table,
        body :: binary, sync_timeout) :: result
  def sync_dispatch(client, dest, tab, body, timeout \\ 5_000) do
    # call will use a middleman process that exits on timeout, so client will
    # monitor middleman (instead of caller) and discard when it exits on timeout
    request = {:dispatch, wire_context(), dest, tab, body}
    Mux.Connection.call(client, request, timeout)
  end

  @spec dispatch(client, Mux.Packet.dest, Mux.Packet.dest_table,
        body :: binary) :: reference
  def dispatch(client, dest, tab, body) do
    ref = make_ref()
    request = {:dispatch, {self(), ref}, wire_context(), dest, tab, body}
    Mux.Connection.cast(client, request)
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

  @spec drain(client) :: :ok
  def drain(client),
    do: Mux.Connection.cast(client, :drain)

  @spec enter_loop(module, :gen_tcp.socket, state, [option]) :: no_return
  def enter_loop(mod, sock, state, opts) do
    {cli_opts, opts} = Keyword.split(opts, [:handshake_timeout])
    args = {{mod, state}, sock, cli_opts}
    Mux.Connection.enter_loop(__MODULE__, sock, args, opts)
  end

  @doc false
  def init({handler, sock, opts}) do
    timeout = Keyword.get(opts, :handshake_timeout, @handshake_timeout)
    {headers, handler} = handler_init(handler)
    state = %State{tags: :handshake, exchanges: %{}, monitors: %{}, refs: %{},
                   handler: handler, socket: sock, lease: make_ref(),
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
  def handle(:cast, :drain, state) do
    # act like received drain request but don't send back reply
    {_, state} = handle_drain(0, state)
    {[], state}
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
  def handle(:info, {:timeout, tref, _}, %State{lease: tref} = state) do
    handle_lease(:millisecond, 0, state)
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
  def terminate(reason, %State{handler: handler}),
    do: handler_terminate(reason, handler)

  ## Helpers

  defp wire_context() do
    Mux.Context.get()
    |> start_trace()
    |> Mux.Context.to_wire()
  end

  defp start_trace(%{Mux.Trace => trace} = ctx),
    do: %{ctx | Mux.Trace => Mux.Trace.start(trace)}
  defp start_trace(ctx),
    do: ctx

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

  # context from downstream is ignore
  defp reply(from, :ok, _context, body),
    do: {:reply, from, {:ok, body}}

  defp reply(from, :error, _context, why),
    do: {:reply, from, {:error, Mux.ApplicationError.exception(why)}}

  defp reply(from, :nack, context, _body),
    do: reply_nack(from, context)

  defp reply_error(from, err),
    do: {:reply, from, {:error, err}}

  defp reply_nack(from, _context),
    do: {:reply, from, :nack}

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
           refs: refs} = state
    case tags_pop(tags) do
      {nil, _} ->
        {[reply_nack(from, %{})], state}
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

  defp handle_handshake(headers, %State{handler: handler} = state) do
    {opts, handler} = handler_handshake(headers, handler)

    session_size = Keyword.get(opts, :session_size, @session_size)
    ping_interval = Keyword.get(opts, :ping_interval, @ping_interval)
    frame_size = Keyword.get(opts, :frame_size, @frame_size)

    state = %State{state | handler: handler, tags: tags_new(session_size),
                           timer: start_interval(ping_interval)}
    {[frame_size: frame_size], state}
  end

  defp handler_init({mod, state}) do
    {:ok, headers, state} = apply(mod, :init, [state])
    {headers, {mod, state}}
  end

  defp handler_handshake(headers, {mod, state}) do
    {:ok, opts, state} = apply(mod, :handshake, [headers, state])
    {opts, {mod, state}}
  end

  defp handler_lease(unit, timeout, {mod, state}) do
    {:ok, state} = apply(mod, :lease, [unit, timeout, state])
    {mod, state}
  end

  defp handler_drain({mod, state}) do
    {:ok, state} = apply(mod, :drain, [state])
    {mod, state}
  end

  defp handler_terminate(reason, {mod, state}),
    do: apply(mod, :terminate, [reason, state])

  defp handle_ping(ping_interval, state) do
    state = %State{state | timer: {@ping_tag, start_interval(ping_interval)}}
    {[transmit_ping(@ping_tag)], state}
  end

  defp handle_lease(unit, timeout, state) do
    %State{handler: handler, lease: lease} = state
    cancel_timer(lease)
    handler = handler_lease(unit, timeout, handler)
    case timeout do
      0 ->
        {[], %State{state | handler: handler, lease: make_ref()}}
      _ ->
        ms_timeout = System.convert_time_unit(timeout, unit, :millisecond)
        {[], %State{state | handler: handler, lease: start_timer(ms_timeout)}}
    end
  end

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
