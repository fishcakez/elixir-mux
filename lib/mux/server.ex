defmodule Mux.Server do
  @moduledoc """
  Server loop for a Mux session.

  This module defines a TCP Mux server that multiplexes dispatches over a
  single session.
  """

  @behaviour Mux.Connection

  @drain_tag 1

  defmodule State do
    @moduledoc false
    @enforce_keys [:exchanges, :tasks, :session_size, :handler, :ref, :drain]
    defstruct [:exchanges, :tasks, :session_size, :handler, :ref, :drain]
  end

  @type state :: any
  @type server :: Mux.Connection.connection
  @type option :: Mux.Connection.option | {:session_size, pos_integer}
  @type result ::
    {:ok, Mux.Packet.context, body :: binary} |
    {:error, Mux.Packet.context, %Mux.ApplicationError{}} |
    {:nack, Mux.Packet.context} |
    {:error, %Mux.ServerError{}}

  @callback handle(Mux.Packet.context, Mux.Packet.dest, Mux.Packet.dest_table,
            body :: binary, state) :: result

  @spec drain(server) :: :ok
  def drain(server),
    do: Mux.Connection.cast(server, :drain)

  @spec enter_loop(module, :gen_tcp.socket, state, [option]) :: no_return
  def enter_loop(mod, sock, state, opts) do
    {session_size, conn_opts} = Keyword.pop(opts, :session_size, 32)
    arg = {{mod, state}, session_size}
    Mux.Connection.enter_loop(__MODULE__, sock, arg, conn_opts)
  end

  @doc false
  def init({handler, session_size}) do
    Process.flag(:trap_exit, true)
    state = %State{exchanges: %{}, tasks: %{}, session_size: session_size,
                   handler: handler, ref: make_ref(), drain: false}
    {[], state}
  end

  @doc false
  def handle({:packet, tag}, packet, state) do
    handle_packet(tag, packet, state)
  end
  def handle(:info, {:EXIT, pid, reason}, state) do
    handle_exit(pid, reason, state)
  end
  def handle(:info, msg, state) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    {[], state}
  end
  def handle(:cast, :drain, state),
    do: handle_drain(state)

  @doc false
  def terminate(_, %State{tasks: tasks}) do
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
  end

  # 0 tag is a one way, only handle one-way transmit_discard
  defp handle_packet(0, {:transmit_discarded, tag, _why}, state),
    do: handle_discarded(tag, state)
  defp handle_packet(0, _cast, state),
    do: {[], state}
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
      {:transmit_init, _, _} ->
        # should handle init
        {[receive_error(tag, "can not handle init")], state}
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

  defp handle_dispatch(tag, context, dest, dest_table, body, state) do
    %State{exchanges: exchanges, tasks: tasks, session_size: session_size,
           handler: handler, ref: ref} = state
    if map_size(tasks) < session_size do
      {:ok, pid} = start_task(handler, ref, context, dest, dest_table, body)
      # if client sent duplicate tag don't track this one, client must dedup but
      # server won't be able to handle a future transmit_discarded
      exchanges = Map.put_new(exchanges, tag, pid)
      tasks = Map.put(tasks, pid, tag)
      {[], %State{state | exchanges: exchanges, tasks: tasks}}
    else
      # consider adding MuxFailure flag to context of noop nack
      {[receive_dispatch_nack(tag, %{})], state}
    end
  end

  defp start_task({mod, state}, ref, context, dest, dest_table, body) do
    args = [context, dest, dest_table, body, state]
    Task.start_link(__MODULE__, :dispatch, [ref, mod, :handle, args])
  end

  @doc false
  def dispatch(ref, mod, fun, args) do
    case apply(mod, fun, args) do
      {:ok, _context, _body} = ok ->
        dispatch_result(ref, ok)
      {:error, _context, %Mux.ApplicationError{}} = error ->
        dispatch_result(ref, error)
      {:error, %Mux.ServerError{}} = error ->
        dispatch_result(ref, error)
      {:nack, _context} = nack ->
        dispatch_result(ref, nack)
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
    case result do
      {:ok, context, body} ->
        {[receive_dispatch_ok(tag, context, body)], state}
      {:error, context, %Mux.ApplicationError{message: msg}} ->
        {[receive_dispatch_error(tag, context, msg)], state}
      {:error, %Mux.ServerError{message: msg}} ->
        {[receive_error(tag, msg)], state}
      {:nack, context} ->
        {[receive_dispatch_nack(tag, context)], state}
    end
  end

  # only send drain if haven't already
  defp handle_drain(%State{drain: false} = state),
    do: {[transmit_drain(@drain_tag)], %State{state | drain: @drain_tag}}
  defp handle_drain(state),
    do: {[], state}

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

  defp receive_drain(tag),
    do: {:send, tag, :receive_drain}

  defp receive_ping(tag),
    do: {:send, tag, :receive_ping}
end
