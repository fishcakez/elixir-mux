defmodule Mux.ServerSession do
  @moduledoc """
  Server OSI session layer behaviour.
  """
  @type state :: any
  @type option ::
    {:session_size, pos_integer} |
    {:frame_size, pos_integer} |
    {:ping_interval, timeout}

  @callback init(state) ::
    {:ok, state}

  @callback handshake(Mux.Packet.headers, state) ::
    {:ok, Mux.Packet.headers, [option], state} |
    {:error, %Mux.ServerError{}, state}

  @callback terminate(reason :: any, state) :: any
end

defmodule Mux.ServerSession.Default do
  @moduledoc false
  @behaviour Mux.ServerSession
  @session_size 32
  @frame_size 0xFFFF
  @ping_interval 10_000

  def init(state),
    do: {:ok, state}

  def handshake(headers, state) do
    frame_size = Map.get(headers, "mux-framer", @frame_size)
    opts = [frame_size: frame_size,
            session_size: @session_size,
            ping_interval: @ping_interval]
   {:ok, %{"mux-framer" => frame_size}, opts, state}
  end

  def terminate(_, _),
    do: :ok
end
