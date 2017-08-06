defmodule Mux.ClientPresentation do
  @moduledoc """
  Client OSI presenation layer behaviour.
  """
  @callback init(arg :: any) ::
    {:ok, state :: any}

  @callback encode(request :: any, state :: any) ::
    {:ok, metadata :: any, body :: iodata}

  @callback decode(metadata :: any, body :: iodata, state :: any) ::
    {:ok, response :: any} | {:error, Exception.t}

  @callback terminate(reason :: any, state :: any) :: any
end

defmodule Mux.ClientPresentation.Default do
  @moduledoc false
  @behaviour Mux.ClientPresentation

  def init(state),
    do: {:ok, state}

  def encode(iodata, _),
    do: {:ok, nil, iodata}

  def decode(_, iodata, _),
    do: {:ok, iodata}

  def terminate(_, _),
    do: :ok
end
