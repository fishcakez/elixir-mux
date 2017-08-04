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
