defmodule Mux.Packet do
  @moduledoc """
  Encode and decode Mux packet.
  """

  use Bitwise

  # Type values from https://github.com/twitter/finagle/blob/462643f4f89e70150307d3005711c4deb8e563e1/finagle-mux/src/main/scala/com/twitter/finagle/mux/transport/Message.scala

  @transmit_request 1
  @receive_request -@transmit_request

  @status_ok 1
  @status_error 2
  @status_nack 3

  @transmit_dispatch 2
  @receive_dispatch -@transmit_dispatch

  @transmit_drain 64
  @receive_drain -@transmit_drain

  @transmit_ping 65
  @receive_ping -@transmit_ping

  @transmit_discarded 66
  @old_transmit_discarded -62
  @receive_discarded -@transmit_discarded

  @transmit_lease 67
  @millisecond 0

  @transmit_init 68
  @receive_init -@transmit_init

  @receive_error -128
  @old_receive_error 127

  @type type :: -128..-1 | 1..127 # non-zero 8 bit signed int
  @type tag :: 0..0x7F_FF_FF # 23 bit unsigned int
  @type id :: 0..0xFFFF_FFFF_FFFF_FFFF # 64 bit unsigned int

  @type keys :: %{optional(String.t) => binary}
  @type status :: :ok | :error | :nack
  @type context :: %{optional(String.t) => binary}
  @type dest :: String.t
  @type dest_table :: %{optional(dest) => dest}
  @type version :: 0..0xFF_FF # 16 bit unsigned int
  @type headers :: %{optional(String.t) => binary}

  @type packet ::
    {:transmit_request, keys, body :: binary} |
    {:receive_request, status, body :: binary} |
    {:transmit_dispatch, context, dest, dest_table, body :: binary} |
    {:receive_dispatch, status, context, body :: binary} |
    :transmit_drain |
    :receive_drain |
    :transmit_ping |
    :receive_ping |
    {:transmit_discarded, tag, why :: String.t} |
    :receive_discarded |
    {:transmit_lease, System.time_unit, len :: non_neg_integer} |
    {:receive_error, why :: String.t} |
    {:transmit_init, version, headers} |
    {:receive_init, version, headers}

  @doc """
  Encode a packet to a type integer and iodata.
  """
  @spec encode(packet) :: {type, iodata}
  def encode(packet) do
    case packet do
      {:transmit_request, keys, body} ->
        {@transmit_request, [encode_keys(keys) | body]}
      {:receive_request, status, body} ->
        {@receive_request, [encode_status(status) | body]}
      {:transmit_dispatch, context, dest, dest_table, body} ->
        {@transmit_dispatch,
         [encode_context(context), encode_dest(dest),
          encode_dispatch(dest_table) | body]}
      {:receive_dispatch, status, context, body} ->
        {@receive_dispatch,
         [encode_status(status), encode_context(context) | body]}
      :transmit_drain ->
        {@transmit_drain, []}
      :receive_drain ->
        {@receive_drain, []}
      :transmit_ping ->
        {@transmit_ping, []}
      :receive_ping ->
        {@receive_ping, []}
      {:transmit_discarded, tag, why} ->
        {@transmit_discarded, [encode_tag(tag) | why]}
      :receive_discarded ->
        {@receive_discarded, []}
      {:transmit_lease, unit, len} ->
        {@transmit_lease, encode_lease(unit, len)}
      {:receive_error, why} ->
        {@receive_error, why}
      {:transmit_init, version, headers} ->
        {@transmit_init, [<<version::16>> | encode_headers(headers)]}
      {:receive_init, version, headers} ->
        {@receive_init, [<<version::16>> | encode_headers(headers)]}
    end
  end

  @doc """
  Decode a type integer and iodata to a packet.
  """
  @spec decode(type, iodata) ::
    packet | {:unknown_transmit | :unknown_receive, type, iodata}
  def decode(type, iodata) do
    binary = IO.iodata_to_binary(iodata)
    case type do
      @transmit_request -> transmit_request(binary)
      @receive_request -> receive_request(binary)
      @transmit_dispatch -> transmit_dispatch(binary)
      @receive_dispatch -> receive_dispatch(binary)
      @transmit_drain -> transmit_drain(binary)
      @receive_drain -> receive_drain(binary)
      @transmit_ping -> transmit_ping(binary)
      @receive_ping -> receive_ping(binary)
      @transmit_discarded -> transmit_discarded(binary)
      @old_transmit_discarded -> transmit_discarded(binary)
      @receive_discarded -> receive_discarded(binary)
      @transmit_lease -> transmit_lease(binary)
      @transmit_init -> transmit_init(binary)
      @receive_init -> receive_init(binary)
      @receive_error -> receive_error(binary)
      @old_receive_error -> receive_error(binary)
      _ when type > 0 -> {:unknown_transmit, type, iodata}
      _ when type < 0 -> {:unknown_receive, type, iodata}
    end
  end

  @doc false
  @spec direction(1..127) :: :transmit
  @spec direction(-128..-1) :: :receive
  def direction(type) when type > 0,
    do: :transmit
  def direction(type) when type < 0,
    do: :receive

  ## Encoders

  # n:1 (key:1 value~1){n}

  defp encode_keys(keys),
    do: encode_map(keys, 8)

  defp encode_map(keys, len) do
    [<<map_size(keys)::size(len)>> | encode_map(Map.to_list(keys), len, [])]
  end

  defp encode_map([{key, value} | rest], len, acc) do
    acc = [<<byte_size(key)::size(len)>>, key,
           <<byte_size(value)::size(len)>>, value |
          acc]
    encode_map(rest, len, acc)
  end
  defp encode_map([], _, acc),
    do: acc

  # status:1

  defp encode_status(:ok),
    do: @status_ok
  defp encode_status(:error),
    do: @status_error
  defp encode_status(:nack),
    do: @status_nack

  # nc:2 (ckey~2 cval~2){nc}

  defp encode_context(context),
    do: encode_map(context, 16)

  # dst~2

  defp encode_dest(dest),
    do: [<<byte_size(dest)::16>> | dest]

  # nd:2 (from~2 to~2){nd}

  defp encode_dispatch(dispatch),
    do: encode_map(dispatch, 16)

  # discard_tag:3

  # set msb to 1 as can only discard fragments and those have msb set to 1
  defp encode_tag(tag),
    do: <<1::1, tag::23>>

  # unit:1 howmuch:8

  defp encode_lease(unit, howmuch),
    do: [@millisecond |
         <<System.convert_time_unit(howmuch, unit, :millisecond)::64>>]

  # (key~4 value~4)*

  defp encode_headers(headers),
    do: encode_map(Map.to_list(headers), 32, [])

  ## Decoding

  # n:1 (key:1 value~1){n} body

  defp transmit_request(<<pairs::8, rest::bits>>),
    do: transmit_request(rest, pairs, [])

  defp transmit_request(<<body::bits>>, 0, context),
    do: {:transmit_request, Map.new(context), body}
  defp transmit_request(<<klen::8, key::size(klen)-binary,
       vlen::8, value::size(vlen)-binary, rest::bits>>, pairs, context),
    do: transmit_request(rest, pairs-1, [{key, value} | context])

  # status:1 body

  defp receive_request(<<@status_ok, body::bits>>),
    do: {:receive_request, :ok, body}
  defp receive_request(<<@status_error, body::bits>>),
    do: {:receive_request, :error, body}
  defp receive_request(<<@status_nack, body::bits>>),
    do: {:receive_request, :nack, body}

  # nc:2 (ckey~2 cval~2){nc} dst~2 nd:2 (from~2 to~2){nd} body

  defp transmit_dispatch(<<pairs::16, rest::bits>>),
    do: transmit_dispatch_con(rest, pairs, [])

  defp transmit_dispatch_con(<<rest::bits>>, 0, context),
    do: transmit_dispatch_dest(rest, Map.new(context))
  defp transmit_dispatch_con(<<klen::16, key::size(klen)-binary,
       vlen::16, value::size(vlen)-binary, rest::bits>>, pairs, context),
    do: transmit_dispatch_con(rest, pairs-1, [{key, value} | context])

  defp transmit_dispatch_dest(<<dlen::16, dest::size(dlen)-binary, rest::bits>>,
       context),
    do: transmit_dispatch_tab(rest, context, dest)

  defp transmit_dispatch_tab(<<pairs::16, rest::bits>>, context, dest),
    do: transmit_dispatch_tab(rest, context, dest, pairs, [])

  defp transmit_dispatch_tab(<<body::bits>>, context, dst, 0, tab),
    do: {:transmit_dispatch, context, dst, Map.new(tab), body}
  defp transmit_dispatch_tab(<<flen::16, from::size(flen)-binary,
       tlen::16, to::size(tlen)-binary, rest::bits>>, context, dst, pairs, tab),
    do: transmit_dispatch_tab(rest, context, dst, pairs-1, [{from, to} | tab])

  # status:1 nctx:2 (key~2 value~2){nctx} body

  defp receive_dispatch(<<@status_ok, rest::bits>>),
    do: receive_dispatch(rest, :ok)
  defp receive_dispatch(<<@status_error, rest::bits>>),
    do: receive_dispatch(rest, :error)
  defp receive_dispatch(<<@status_nack, rest::bits>>),
    do: receive_dispatch(rest, :nack)

  defp receive_dispatch(<<pairs::16, rest::bits>>, status),
    do: receive_dispatch(rest, status, pairs, [])

  # The context can contain "MuxFailure" flag
  defp receive_dispatch(<<body::bits>>, status, 0, context),
    do: {:receive_dispatch, status, Map.new(context), body}
  defp receive_dispatch(<<klen::16, key::size(klen)-binary,
       vlen::16, value::size(vlen)-binary, rest::bits>>, status, pairs, context),
    do: receive_dispatch(rest, status, pairs-1, [{key, value} | context])

  # (empty)

  defp transmit_drain(<<>>),
    do: :transmit_drain

  # (empty)

  defp receive_drain(<<>>),
    do: :receive_drain

  # (empty)

  defp transmit_ping(<<>>),
    do: :transmit_ping

  # (empty)

  defp receive_ping(<<>>),
    do: :receive_ping

  # discard_tag:3 why

  defp transmit_discarded(<<_::1, tag::23, why::bits>>),
    do: {:transmit_discarded, tag, why}

  # (empty)

  defp receive_discarded(<<>>),
    do: :receive_discarded

  # unit:1 howmuch:8

  defp transmit_lease(<<@millisecond, len::64>>),
    do: {:transmit_lease, :millisecond, len}

  # why

  def receive_error(why),
    do: {:receive_error, why}

  # version:2 (key~4 value~4)*

  defp transmit_init(<<vsn::16, rest::bits>>),
    do: init(rest, :transmit_init, vsn, [])

  # version:2 (key~4 value~4)*

  defp receive_init(<<vsn::16, rest::bits>>),
    do: init(rest, :receive_init, vsn, [])

  defp init(<<hlen::32, header::binary-size(hlen), vlen::32,
       value::binary-size(vlen), rest::bits>>, type_name, vsn, headers) do
    init(rest, type_name, vsn, [{header, value} | headers])
  end
  defp init(<<>>, type_name, vsn, headers),
    do: {type_name, vsn, Map.new(headers)}
end
