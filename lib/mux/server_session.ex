defmodule Mux.ServerSession do
  @moduledoc """
  Server loop for a Mux session.

  This module defines a TCP Mux server that multiplexes dispatches over a
  single session.
  """

  @behaviour Mux.Connection

  defmodule State do
    @moduledoc false
    @enforce_keys [:exchanges, :tasks, :session_size, :handler, :handshake,
                   :contexts, :ref, :drain, :lease, :timer]
    defstruct [:exchanges, :tasks, :session_size, :handler, :handshake,
               :contexts, :ref, :drain, :lease, :timer]
  end

  @handshake_timeout 5_000
  @handshake_check "tinit check"
  @wire_contexts [Mux.Deadline, Mux.Trace]
  @mux_version 1
  @session_size 32
  @frame_size 0xFFFF
  @ping_interval 5_000
  @ping_tag 1
  @lease_tag 0
  @drain_tag 2

  @type state :: any
  @type server :: Mux.Connection.connection
  @type option ::
    Mux.Connection.option |
    {:handshake, {module, any}} |
    {:handshake_timeout, timeout} |
    {:wire_contexts, [module]}

  @type result ::
    {:ok, body :: binary} |
    {:error, %Mux.ApplicationError{}} |
    :nack |
    {:error, %Mux.ServerError{}}

  @callback init(state) ::
    {:ok, state}

  @callback dispatch(Mux.Packet.dest, Mux.Packet.dest_table,
            body :: binary, state) :: result

  @callback terminate(reason :: any, state) :: any

  @spec lease(server, System.time_unit, non_neg_integer) :: :ok
  def lease(server, time_unit, non_neg_integer),
    do: Mux.Connection.cast(server, {:lease, time_unit, non_neg_integer})

  @spec drain(server) :: :ok
  def drain(server),
    do: Mux.Connection.cast(server, :drain)

  @spec enter_loop(module, :gen_tcp.socket, state, [option]) :: no_return
  def enter_loop(mod, sock, state, opts) do
    {srv_opts, opts} =
      Keyword.split(opts, [:handshake, :handshake_timeout, :context])
    arg = {{mod, state}, srv_opts}
    Mux.Connection.enter_loop(__MODULE__, sock, arg, opts)
  end

  @doc false
  def init({handler, opts}) do
    Process.flag(:trap_exit, true)
    default = fn -> Application.fetch_env!(:mux, :server_handshake) end
    handshake = Keyword.get_lazy(opts, :handshake, default)
    timeout = Keyword.get(opts, :handshake_timeout, @handshake_timeout)
    {handler, handshake} = pair_init(handler, handshake)
    contexts = Keyword.get(opts, :wire_contexts, @wire_contexts)
    # session_size is 0 until handshake succeeds, lease is assumed to be held
    # for infinity until told otherwise.
    state = %State{exchanges: %{}, tasks: %{}, session_size: 0, ref: make_ref(),
                   handler: handler, handshake: handshake, contexts: contexts,
                   drain: false, lease: make_ref(),
                   timer: {:handshake, start_timer(timeout)}}
    {[], state}
  end

  @doc false
  def handle({:packet, tag}, packet, state) do
    handle_packet(tag, packet, state)
  end
  def handle(:info, {:EXIT, pid, reason}, state) do
    handle_exit(pid, reason, state)
  end
  def handle(:info, {:timeout, tref, interval}, %State{timer: tref} = state) do
    handle_ping(interval, state)
  end
  def handle(:info, {:timeout, tref, _}, %State{timer: {_tag, tref}}) do
    # no response from last ping or handshake
    exit({:tcp_error, :timeout})
  end
  def handle(:info, {:timeout, lease, _}, %State{lease: lease} = state) do
    {[], %State{state | lease: false}}
  end
  def handle(:info, msg, state) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    {[], state}
  end
  def handle(:cast, {:lease, time_unit, timeout}, state),
    do: handle_lease(time_unit, timeout, state)
  def handle(:cast, :drain, state),
    do: handle_drain(state)

  @doc false
  def terminate(:normal, %State{tasks: tasks} = state)
      when map_size(tasks) > 0 do
    # still handling dispatches so not a clean stop
    reason = {:tcp_error, :closed}
    terminate(reason, state)
    exit(reason)
  end
  def terminate(reason, state) do
    %State{tasks: tasks, handler: handler, handshake: handshake} = state
    pids = Map.keys(tasks)
    for pid <- pids do
      Process.exit(pid, :kill)
    end
    for pid <- pids do
      receive do
        {:EXIT, ^pid, _} ->
          :ok
      end
    end
    pair_terminate(reason, handler, handshake)
  end

  # 0 tag is a one way, only handle one-way transmit_discard
  defp handle_packet(0, {:transmit_discarded, tag, _why}, state),
    do: handle_discarded(tag, state)
  defp handle_packet(0, _cast, state),
    do: {[], state}
  # ping came back before next ping was due to be sent
  defp handle_packet(tag, :receive_ping, %State{timer: {tag, tref}} = state),
    do: {[], %State{state | timer: tref}}
  # client checking that support handshake
  defp handle_packet(tag, {:receive_error, @handshake_check},
       %State{timer: {:handshake, _}} = state),
    do: {[receive_error(tag, @handshake_check)], state}
  # check to see if sent drain request, wait for client to close
  defp handle_packet(tag, :receive_drain, %State{drain: tag} = state),
    do: {[], %State{state | drain: true}}
  defp handle_packet(tag, packet, state) do
    case packet do
      {:transmit_dispatch, context, dest, dest_table, body} ->
        handle_dispatch(tag, context, dest, dest_table, body, state)
      {:transmit_request, _, _} ->
        # don't handle old clients :(
        {[receive_error(tag, "can not handle request")], state}
      :transmit_drain ->
        # unexpected but don't send any dispatch/requests so can reply straight
        # back
        {[receive_drain(tag)], state}
      :transmit_ping ->
        {[receive_ping(tag)], state}
      {:transmit_init, vsn, headers} when vsn >= @mux_version ->
        # only support this version
        handle_init(tag, headers, state)
      {:transmit_init, vsn, _} when vsn < @mux_version ->
        {[receive_error(tag, "mux version #{vsn} not supported")], state}
    end
  end

  defp handle_discarded(tag, state) do
    %State{exchanges: exchanges, tasks: tasks} = state
    case Map.pop(exchanges, tag) do
      {nil, _} ->
        # response already sent or client sent duplicate tags and not tracking
        {[], state}
      {pid, exchanges} ->
        tasks = Map.delete(tasks, pid)
        state = %State{state | exchanges: exchanges, tasks: tasks}
        handle_discarded(tag, pid, state)
    end
  end

  defp handle_discarded(tag, pid, state) do
    Process.exit(pid, :kill)
    receive do
      {:EXIT, ^pid, _} ->
        # possible exit reason has result but client doesn't care
        {[receive_discarded(tag)], state}
    end
  end

  defp handle_dispatch(tag, wire_context, dest, table, body, state) do
    %State{exchanges: exchanges, tasks: tasks, session_size: session_size,
           lease: lease, handler: handler, contexts: ctxs, ref: ref} = state
    if map_size(tasks) < session_size and lease do
      context = {wire_context, ctxs}
      {:ok, pid} = start_task(handler, ref, context, dest, table, body)
      # if client sent duplicate tag don't track this one, client must dedup but
      # server won't be able to handle a future transmit_discarded
      exchanges = Map.put_new(exchanges, tag, pid)
      tasks = Map.put(tasks, pid, tag)
      {[], %State{state | exchanges: exchanges, tasks: tasks}}
    else
      {[receive_dispatch_nack(tag, %{})], state}
    end
  end

  defp start_task({mod, state}, ref, context, dest, dest_table, body) do
    args = [dest, dest_table, body, state]
    Task.start_link(__MODULE__, :dispatch, [ref, context, mod, :dispatch, args])
  end

  @doc false
  def dispatch(ref, {wire, mods}, mod, fun, args) do
    res = Mux.Context.bind_wire(wire, mods, fn -> dispatch(mod, fun, args) end)
    dispatch_result(ref, res)
  end

  defp dispatch(mod, fun, args) do
    case apply(mod, fun, args) do
      {:ok, _body} = ok ->
        ok
      {:error, %Mux.ApplicationError{}} = error->
        error
      {:error, %Mux.ServerError{}} = error ->
        error
      :nack ->
        :nack
    end
  end

  # use shutdown exit to only send a single signal to server (and not to log)
  defp dispatch_result(ref, result),
    do: exit({:shutdown, {ref, result}})

  # special exit reason from previous line
  defp handle_exit(pid, {:shutdown, {ref, result}}, %State{ref: ref} = state) do
    case tasks_pop(pid, state) do
      {nil, _} ->
        {[], state}
      {tag, state} ->
        handle_result(tag, result, state)
    end
  end
  defp handle_exit(pid, _, state) do
    case tasks_pop(pid, state) do
      {nil, state} ->
        {[], state}
      {tag, state} ->
        # task will have logged (if abnormal), pass simple reason to client
        err = Mux.ServerError.exception("process exited")
        handle_result(tag, {:error, err}, state)
    end
  end

  defp tasks_pop(pid, %State{exchanges: exchanges, tasks: tasks} = state) do
    with {tag, tasks} when tag != nil <- Map.pop(tasks, pid) do
      case Map.pop(exchanges, tag) do
        {^pid, exchanges} ->
          {tag, %State{state | exchanges: exchanges, tasks: tasks}}
        {_, _} ->
          # duplicate tag, not being tracked by server but can still respond,
          # client must dedup
          {tag, %State{state | tasks: tasks}}
      end
    else
      _ ->
        # exit signal not from one of the tasks
        {nil, state}
    end
  end

  defp handle_result(tag, result, state) do
    # no support for sending context upstream
    case result do
      {:ok, body} ->
        {[receive_dispatch_ok(tag, %{}, body)], state}
      {:error, %Mux.ApplicationError{message: msg}} ->
        {[receive_dispatch_error(tag, %{}, msg)], state}
      {:error, %Mux.ServerError{message: msg}} ->
        {[receive_error(tag, msg)], state}
      :nack ->
        {[receive_dispatch_nack(tag, %{})], state}
    end
  end

  defp handle_init(tag, headers, %State{timer: {:handshake, tref}} = state) do
    handshake(tag, headers, tref, state)
  end
  # only handle init when expecting handshake
  defp handle_init(tag, _, state),
    do: {[receive_error(tag, "reinitialization not supported")], state}

  defp handshake(tag, headers, tref, %State{handshake: handshake} = state) do
    case handshake(headers, handshake) do
      {:ok, headers, opts, handshake} ->
        cancel_timer(tref)
        session_size = Keyword.get(opts, :session_size, @session_size)
        ping_interval = Keyword.get(opts, :ping_interval, @ping_interval)
        frame_size = Keyword.get(opts, :frame_size, @frame_size)

        commands = [receive_init(tag, @mux_version, headers),
                    {:frame_size, frame_size}]
        state = %State{state | handshake: handshake, session_size: session_size,
                               timer: start_interval(ping_interval)}
        {commands, state}
      {:error, %Mux.ServerError{message: why}, handshake} ->
        {[receive_error(tag, why)], %State{state | handshake: handshake}}
    end
  end

  defp pair_init({mod1, state1}, {mod2, state2}) do
    {:ok, state1} = apply(mod1, :init, [state1])
    try do
      {:ok, state2} = apply(mod2, :init, [state2])
      state2
    catch
      kind, reason ->
        stack = System.stacktrace()
        # undo mod1.init/1 as terminate/2 won't be called
        apply(mod1, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      state2 ->
        {{mod1, state1}, {mod2, state2}}
    end
  end

  defp handshake(headers, {mod, state}) do
    case apply(mod, :handshake, [headers, state]) do
      {:ok, headers, opts, state} ->
        {:ok, headers, opts, {mod, state}}
      {:error, %Mux.ServerError{} = err, state} ->
        {:error, err, {mod, state}}
    end
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

  defp handle_lease(time_unit, timeout, %State{lease: lease} = state) do
    if lease, do: cancel_timer(lease)
    case timeout do
      0 ->
        # no lease
        state = %State{state | lease: false}
        {[transmit_lease(@lease_tag, time_unit, 0)], state}
      _ ->
        # got a lease!
        ms_timeout = System.convert_time_unit(timeout, time_unit, :millisecond)
        state = %State{state | lease: start_timer(ms_timeout)}
        {[transmit_lease(@lease_tag, time_unit, timeout)], state}
    end
  end

  # only send drain if haven't already
  defp handle_drain(%State{drain: false} = state),
    do: {[transmit_drain(@drain_tag)], %State{state | drain: @drain_tag}}
  defp handle_drain(state),
    do: {[], state}

  defp transmit_ping(tag),
    do: {:send, tag, :transmit_ping}

  defp transmit_lease(tag, time_unit, timeout),
    do: {:send, tag, {:transmit_lease, time_unit, timeout}}

  defp transmit_drain(tag),
    do: {:send, tag, :transmit_drain}

  defp receive_dispatch_ok(tag, context, body),
    do: receive_dispatch(tag, :ok, context, body)

  defp receive_dispatch_error(tag, context, why),
    do: receive_dispatch(tag, :error, context, why)

  defp receive_dispatch_nack(tag, context),
    do: receive_dispatch(tag, :nack, context, "")

  defp receive_dispatch(tag, status, context, body),
    do: {:send, tag, {:receive_dispatch, status, context, body}}

  defp receive_error(tag, why),
    do: {:send, tag, {:receive_error, why}}

  defp receive_discarded(tag),
    do: {:send, tag, :receive_discarded}

  defp receive_init(tag, vsn, headers),
    do: {:send, tag, {:receive_init, vsn, headers}}

  defp receive_drain(tag),
    do: {:send, tag, :receive_drain}

  defp receive_ping(tag),
    do: {:send, tag, :receive_ping}

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
