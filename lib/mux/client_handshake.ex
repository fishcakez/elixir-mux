defmodule Mux.ClientHandshake do
  @moduledoc """
  Client handshake behaviour.
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
