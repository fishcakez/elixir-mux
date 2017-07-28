defmodule Mux.ClientHandshake do
  @moduledoc """
  Client handshake behaviour.
  """
  @type state :: any
  @type session_option ::
    {:session_size, pos_integer} |
    {:frame_size, pos_integer} |
    {:ping_interval, timeout}

  @callback init(state) ::
    {:ok, Mux.Packet.headers, state}

  @callback handshake(Mux.Packet.headers, state) ::
    {:ok, [session_option], state}

  @callback terminate(reason :: any, state) :: any
end
