defmodule Mux.ServerHandshake do
  @moduledoc """
  Server handshake behaviour.
  """
  @type state :: any

  @callback init(state) ::
    {:ok, state}

  @callback handshake(Mux.Packet.headers, state) ::
    {:ok, Mux.Packet.headers, [Mux.ServerSession.session_option], state} |
    {:error, %Mux.ServerError{}, state}

  @callback terminate(reason :: any, state) :: any
end
