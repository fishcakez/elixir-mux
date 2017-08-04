defmodule Mux.ServerHandler do
  @moduledoc """
  Server handler behaviour.
  """
  @type result ::
    {:ok, body :: binary} |
    {:error, %Mux.ApplicationError{}} |
    :nack |
    {:error, %Mux.ServerError{}}

  @callback init(state :: any) ::
    {:ok, contexts :: [module], state :: any}

  @callback dispatch(Mux.Packet.dest, Mux.Packet.dest_table,
            body :: binary, state :: any) :: result

  @callback terminate(reason :: any, state :: any) :: any
end
