defmodule Mux.ClientSession do
  @moduledoc """
  Client OSI session layer behaviour.
  """
  @type option ::
    {:session_size, pos_integer} |
    {:frame_size, pos_integer} |
    {:ping_interval, timeout}

  @callback init(arg :: any) ::
    {:ok, Mux.Packet.headers, state :: any}

  @callback handshake(Mux.Packet.headers, state :: any) ::
    {:ok, [option], state :: any}

  @callback terminate(reason :: any, state :: any) :: any
end

defmodule Mux.ClientSession.Default do
  @moduledoc false
  @behaviour Mux.ClientSession
  @session_size 32
  @frame_size 0xFFFF
  @ping_interval 10_000

  def init(state),
    do: {:ok, %{"mux-framer" => <<@frame_size::32>>}, state}

  def handshake(headers, state) do
    opts = [frame_size: Map.get(headers, "mux-framer", @frame_size),
            session_size: @session_size,
            ping_interval: @ping_interval]
   {:ok, opts, state}
  end

  def terminate(_, _),
    do: :ok
end
