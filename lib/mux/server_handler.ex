defmodule Mux.ServerHandler do
  @moduledoc """
  Server handler behaviour.
  """

  @callback init(state :: any) ::
    {:ok, state :: any}

  @callback dispatch(Mux.Packet.dest, Mux.Packet.dest_table,
            body :: binary, state :: any) :: Mux.ServerSession.result

  @callback terminate(reason :: any, state :: any) :: any
end
