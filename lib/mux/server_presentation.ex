defmodule Mux.ServerPresentation do
  @moduledoc """
  Server OSI presenation layer behaviour.
  """
  @callback init(arg :: any) ::
    {:ok, state :: any}

  @callback decode(body :: iodata, state :: any) ::
    {:ok, meta_data :: any, request :: any}

  @callback encode(metadata :: any, response :: any, state :: any) ::
    {:ok, body :: iodata}

  @callback terminate(reason :: any, state :: any) :: any
end

defmodule Mux.ServerPresentation.Default do
  @moduledoc false
  @behaviour Mux.ServerPresentation

  def init(state),
    do: {:ok, state}

  def decode(iodata, _),
    do: {:ok, nil, iodata}

  def encode(_, iodata, _),
    do: {:ok, iodata}

  def terminate(_, _),
    do: :ok
end
