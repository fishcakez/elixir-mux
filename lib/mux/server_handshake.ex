defmodule Mux.ServerHandshake do
  @moduledoc """
  Server handshake behaviour.
  """
  @type state :: any
  @type session_option ::
    {:session_size, pos_integer} |
    {:frame_size, pos_integer} |
    {:ping_interval, timeout}

  @callback init(state) ::
    {:ok, state}

  @callback handshake(Mux.Packet.headers, state) ::
    {:ok, Mux.Packet.headers, [session_option], state} |
    {:error, %Mux.ServerError{}, state}

  @callback terminate(reason :: any, state) :: any
end
