defmodule Mux.Presentation do
  @moduledoc """
  OSI presenation layer behaviour.
  """
  @callback init(arg :: any) ::
    {:ok, state :: any}

  @callback encode(request :: any, state :: any) ::
    {:ok, body :: iodata}

  @callback decode(body :: iodata, state :: any) ::
    {:ok, response :: any}

  @callback terminate(reason :: any, state :: any) :: any
end

defmodule Mux.Presentation.Default do
  @moduledoc false
  @behaviour Mux.Presentation

  def init(state),
    do: state

  def encode(iodata, _),
    do: iodata

  def decode(iodata, _),
    do: iodata

  def terminate(_, _),
    do: :ok
end
