defmodule Mux.Connection do
  @moduledoc """
  Connection behaviour to handle framing of exchanges over a Mux session.

  This module defines a TCP Mux transport that allows multiple streams to
  multiplex both directions over a single session without blocking to send or
  receive. Outgoing packets are split and interleaved based on max frame size,
  incoming packets are merged and discarding is handled where possible.
  """

  @behaviour :gen_statem
  use Bitwise

  defmodule Data do
    @moduledoc false
    @enforce_keys [:mod, :sock, :state, :frame_size, :acks, :discards,
                   :fragments]
    defstruct [:mod, :sock, :state, :frame_size, :acks, :discards, :fragments]
  end

  @socket_options [
    # for packet parsing
    packet: 4, mode: :binary,
    # for exit reason as econnreset is abnormal exit
    show_econnreset: true,
    # ensure first timeout closes socket to prevent undefined behaviour with
    # async send technique
    send_timeout_close: true,
    # connections may enter half-close (client shuts down write and waits for
    # server to close
    exit_on_close: false]

  @type connection :: :gen_statem.server_ref
  @type option ::
    :gen_statem.debug_opt |
    :gen_statem.hibernate_after_opt |
    {:name, GenServer.name} |
    {:frame_size, pos_integer | :infinity}

  @type command ::
    {:frame_size, pos_integer | :infinity} |
    {:reply, from, any} |
    {:send, Mux.Packet.tag, Mux.Packet.t}

  @type state :: any
  @type from :: :gen_statem.from
  @type event_type :: :cast | :info | {:call, from} | {:packet, Mux.Packet.tag}

  @callback init(state) :: {[command], state}

  @callback handle(event_type, event :: any, state) :: {[command], state}

  @callback terminate(reason :: any, state) :: any

  @spec cast(connection, request :: any) :: :ok
  defdelegate cast(connection, request), to: :gen_statem

  @spec call(connection, request :: any, timeout) :: :ok
  defdelegate call(connection, request, timeout), to: :gen_statem

  @spec enter_loop(module, :gen_tcp.socket, state, [option]) :: no_return
  def enter_loop(mod, sock, state, opts \\ []) do
    {loop_opts, opts} = Keyword.split(opts, [:debug, :hibernate_after])
    data = %Data{mod: mod, sock: sock, state: state, frame_size: :infinity,
                 acks: :queue.new(), discards: %{}, fragments: %{}}
    {name, opts} = Keyword.pop(opts, :name, self())
    actions = [{:next_event, :internal, {:init, opts}}]
    :gen_statem.enter_loop(__MODULE__, loop_opts, :init, data, name, actions)
  end

  @doc false
  def init(_),
    do: {:stop, :enter_loop}

  @doc false
  def callback_mode(), do: :state_functions

  @doc false
  def init(:internal, {:init, opts}, data) do
    %Data{mod: mod, state: state, sock: sock} = data
    {commands, state} = apply(mod, :init, [state])
    actions = init_actions(sock, commands, opts)
    {:next_state, :ready, %Data{data | state: state}, actions}
  end

  @doc false
  def ready(:internal, event, data),
    do: internal(event, data)
  def ready(:info, event, data),
    do: info(event, data)
  def ready(event_type, event, data),
    do: handle(event_type, event, data)

  @doc false
  def code_change(_, state, data, _),
    do: {:ok, state, data}


  @doc false
  # if state is :init then init/1 failed and no state!
  def terminate(_, :init, _),
    do: :ok
  def terminate(:normal, _, %Data{acks: acks, fragments: fragments} = data) do
    if :queue.is_empty(acks) and map_size(fragments) === 0 do
      terminate(:normal, data)
    else
      # have not finished sending or receiving data
      reason = {:tcp_error, :closed}
      terminate(reason, data)
      exit(reason)
    end
  end
  def terminate(reason, _, data),
    do: terminate(reason, data)

  ## Helpers

  defp internal({:send, tag, packet}, data),
    do: send(tag, packet, data)
  defp internal({:packet, tag, packet}, data) do
    handle({:packet, tag}, packet, data)
  end
  defp internal({:frame_size, frame_size}, %Data{sock: sock} = data) do
    case frame_size do
      :infinity ->
        # can ignore error as will get :tcp_error/:tcp_closed message
        :ok = :inet.setopts(sock, [packet_size: 0] ++ @socket_options)
        {:keep_state, %Data{data | frame_size: :infinity}}
      _ when is_integer(frame_size) and frame_size > 0 ->
        # 4 for size, 1 for type and 3 for tag
        packet_size = max(frame_size + 4 + 1 + 3, 0xFFFF)
        :ok = :inet.setopts(sock, [packet_size: packet_size] ++ @socket_options)
        {:keep_state, %Data{data | frame_size: frame_size}}
    end
  end

  defp info({:inet_reply, sock, status}, %Data{sock: sock} = data) do
    case status do
      :ok ->
        ack(data)
      {:error, reason} ->
        {:stop, {:tcp_error, reason}}
    end
  end
  defp info({:tcp, sock, binary}, %Data{sock: sock} = data) do
    parse(binary, data)
  end
  defp info({:tcp_passive, sock}, %Data{sock: sock}) do
    case :inet.setopts(sock, [active: 32]) do
      :ok ->
        :keep_state_and_data
      {:error, reason} ->
        {:stop, {:tcp_error, reason}}
    end
  end
  defp info({:tcp_error, sock, reason}, %Data{sock: sock}),
    do: {:stop, {:tcp_error, reason}}
  defp info({:tcp_closed, sock}, %Data{sock: sock}),
    do: {:stop, :normal}
  defp info({:EXIT, sock, reason}, %Data{sock: sock}),
    do: {:stop, {:tcp_error, reason}}
  defp info(msg, data),
    do: handle(:info, msg, data)

  defp handle(event_type, event, %Data{mod: mod, state: state} = data) do
    {commands, state} = apply(mod, :handle, [event_type, event, state])
    {:keep_state, %Data{data | state: state}, actions(commands)}
  end

  defp init_actions(sock, commands, opts) do
    [{:next_event, :internal, {:frame_size, opts[:frame_size] || :infinity}},
     # fake passive msg to ensure active, if passive already will sync later
     {:next_event, :info, {:tcp_passive, sock}} |
     actions(commands)]
  end

  defp actions(commands) do
    for command <- commands, do: action(command)
  end

  defp action(command) do
    case command do
      {:reply, _, _} ->
        command
      {:frame_size, _} ->
        {:next_event, :internal, command}
      {:send, _, _} = command ->
        {:next_event, :internal, command}
    end
  end

  # don't let an oversized tag get sent.
  defp send(tag, packet, data) when (tag >>> 23) === 0 do
    {type, iodata} = Mux.Packet.encode(packet)
    case packet do
      {:transmit_discarded, discard_tag, _} ->
        send_discard(type, tag, iodata, :transmit, discard_tag, data)
      :receive_discarded ->
        send_discard(type, tag, iodata, :receive, tag, data)
      {:receive_error, _} ->
        send_discard(type, tag, iodata, :receive, tag, data)
      {:transmit_dispatch, _, _, _, _} ->
        send_split(:transmit, type, tag, iodata, data)
      {:receive_dispatch, _, _, _} ->
        send_split(:receive, type, tag, iodata, data)
      {:transmit_init, _, _} ->
        send_flush(type, tag, iodata, data)
      {:receive_init, _, _} ->
        send_flush(type, tag, iodata, data)
      _ ->
        send_full(type, tag, iodata, data)
    end
  end

  defp send_discard(type, tag, iodata, direction, discard_tag, data) do
    # must not send any more dispatch fragments with discard_tag so block any
    # already waiting from sending more fragments
    %Data{sock: sock, acks: acks, discards: discards} = data
    send(sock, {self(), {:command, [<<type::signed, 0::1, tag::23>> | iodata]}})
    # uniquely identify discard so if another dicarded is sent later the socket
    # ack for this packet does not stop the discarding
    ref = make_ref()
    op = {:discard, direction, discard_tag, ref}
    key = {direction, discard_tag}
    data = %Data{data | acks: :queue.in(op, acks),
                        discards: Map.put(discards, key, ref)}
    {:keep_state, data}
  end

  defp send_split(direction, type, tag, iodata, data) do
    %Data{frame_size: max} = data
    case split(iodata, max) do
      :nosplit ->
        send_full(type, tag, iodata, data)
      {chunk, rest} ->
        send_split(direction, type, tag, chunk, rest, data)
    end
  end

  defp send_split(direction, type, tag, chunk, rest, data) do
    %Data{sock: sock} = data
    send(sock, {self(), {:command, [<<type::signed, 1::1, tag::23>> | chunk]}})
    op = {direction, type, tag, rest}
    {:keep_state, update_in(data.acks, &:queue.in(op, &1))}
  end

  defp send_full(type, tag, iodata, data) do
    %Data{sock: sock} = data
    send(sock, {self(), {:command, [<<type::signed, 0::1, tag::23>> | iodata]}})
    {:keep_state, update_in(data.acks, &:queue.in(:noop, &1))}
  end

  defp send_flush(type, tag, iodata, data) do
    case flush(data) do
      {:keep_state, data} ->
        send_full(type, tag, iodata, data)
      {:stop, _} = stop ->
        stop
    end
  end

  # callback module wants to ensure all pending packets are ahead in the TCP
  # stream, i.e. before it sends a transmit_init or receive_init. Run through
  # the pending actions (as if ack'ed) until exhaustion, counting all the
  # fake acks that occur and replace acks with noop of same queue as all actions
  # handled.
  defp flush(packets \\ 0, %Data{acks: acks} = data) do
    if :queue.is_empty(acks) do
      flush_rebuild(packets, data)
    else
      flush_ack(packets, data)
    end
  end

  defp flush_ack(packets, data) do
    case ack(data) do
      {:keep_state, data} ->
        flush(packets + 1, data)
      {:stop, _reason} = stop ->
        stop
    end
  end

  # all ack functions handled so nothing to discard and every existing ack is a
  # noop.
  defp flush_rebuild(packets, data) do
    noops =
      :noop
      |> List.duplicate(packets)
      |> :queue.from_list()
    {:keep_state, %Data{data | acks: noops, discards: %{}}}
  end

  defp ack(data) do
    {{:value, item}, data} = get_and_update_in(data.acks, &:queue.out/1)
    case item do
      :noop ->
        {:keep_state, data}
      {:discard, direction, tag, ref} ->
        ack_discard(direction, tag, ref, data)
      {direction, type, tag, iodata} ->
        ack_split(direction, type, tag, iodata, data)
    end
  end

  defp ack_discard(direction, tag, ref, %Data{discards: discards} = data) do
    case Map.pop(discards, {direction, tag}) do
      {^ref, discards} ->
        # stop discarding as tag will get reused
        {:keep_state, %Data{data | discards: discards}}
      _ ->
        # another discard for same tag was sent, keep discarding
        {:keep_state, data}
    end
  end

  defp ack_split(direction, type, tag, iodata, data) do
    %Data{discards: discards} = data
    # checking whether discard was sent after this packet began, and if so must
    # discard
    if Map.has_key?(discards, {direction, tag}) do
      {:keep_state, data}
    else
      send_split(direction, type, tag, iodata, data)
    end
  end

  defp parse(<<type::signed, 1::1, tag::23, rest::bits>>, data) do
    key = {Mux.Packet.direction(type), tag}
    append = fn {^type, sofar} -> {type, [sofar | rest]} end
    update = &Map.update(&1, key, {type, rest}, append)
    {:keep_state, update_in(data.fragments, update)}
  end
  defp parse(<<type::signed, 0::1, tag::23, rest::bits>>, data) do
    key = {Mux.Packet.direction(type), tag}
    {status, data} = pop_in(data.fragments[key])
    case status do
      {^type, sofar} ->
        parse(type, tag, [sofar | rest], data)
      _ ->
        # either nothing in fragments or type changed (dispatch -> discarded)
        parse(type, tag, rest, data)
    end
  end
  defp parse(_binary, _data),
    do: {:stop, {:tcp_error, :badarg}}

  defp parse(type, tag, body, data) do
    packet = Mux.Packet.decode(type, body)
    action = {:next_event, :internal, {:packet, tag, packet}}
    case packet do
      {:transmit_discarded, discard_tag, _} ->
        transmit_discarded(discard_tag, action, data)
      :receive_discarded ->
        receive_discarded(tag, action, data)
      {:receive_error, _} ->
        receive_discarded(tag, action, data)
      {:transmit_init, _, _} ->
        transmit_init(tag, action, data)
      {:receive_init, _, _} ->
        receive_init(action, data)
      _ ->
        {:keep_state, data, action}
    end
  end

  defp transmit_discarded(discard_tag, action, data) do
    {sofar, data} = pop_in(data.fragments[{:transmit, discard_tag}])
    if sofar do
      # never finished reading the dispatch so reply with discarded without
      # passing anything to callback module
      command = {:send, discard_tag, :receive_discarded}
      {:keep_state, data, {:next_event, :internal, command}}
    else
      # either callback module is handling or it's unknown (possibly because was
      # already handled)
      {:keep_state, data, action}
    end
  end

  defp receive_discarded(discard_tag, action, data) do
    # remove pending fragments on same tag
    data = update_in(data.fragments, &Map.delete(&1, {:receive, discard_tag}))
    {:keep_state, data, action}
  end

  def transmit_init(tag, action, %Data{fragments: fragments} = data) do
    if fragments === %{} do
      {:keep_state, data, action}
    else
      # peer attempting to re-init connection but didn't clean up last
      # session, tricky for callback modules to handle this situation in a sane
      # manner as intentions of peer unclear
      msg = "can not handle reinitializing session when incomplete fragments"
      command = {:send, tag, {:receive_error, msg}}
      {:keep_state, data, {:next_event, :internal, command}}
    end
  end

  defp receive_init(action, data) do
    # remove all pending fragments (transmit and receive) as peer has reset all
    # it's known tags with this packet
    {:keep_state, %Data{data | fragments: %{}}, action}
  end

  defp split(iodata, max) do
    case IO.iodata_length(iodata) do
      size when size > max ->
        <<chunk::size(max)-binary, rest::bits>> = IO.iodata_to_binary(iodata)
        {chunk, rest}
      _ ->
        :nosplit
    end
  end

  defp terminate(reason, %Data{mod: mod, state: state}),
    do: apply(mod, :terminate, [reason, state])
end
