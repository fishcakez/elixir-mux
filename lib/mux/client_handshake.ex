defmodule Mux.ClientHandshake do
  @moduledoc """
  Client handshake behaviour.
  """
  @callback init(arg :: any) ::
    {:ok, Mux.Packet.headers, state :: any}

  @callback handshake(Mux.Packet.headers, state :: any) ::
    {:ok, [Mux.ClientSession.session_option], state :: any}

  @callback terminate(reason :: any, state :: any) :: any
end
