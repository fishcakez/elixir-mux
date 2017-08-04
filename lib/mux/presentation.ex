defmodule Mux.Presentation do
  @moduledoc """
  Presenation layer behaviour.
  """
  @callback init(arg :: any) ::
    {:ok, state :: any}

  @callback encode(request :: any, state :: any) ::
    {:ok, body :: iodata}

  @callback decode(body :: iodata, state :: any) ::
    {:ok, response :: any}

  @callback terminate(reason :: any, state :: any) :: any
end
