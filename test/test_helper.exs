defmodule MuxData do

  import StreamData

  def packet() do
    frequency([
      {20, transmit_request()},
      {20, receive_request()},
      {40, transmit_dispatch()},
      {40, receive_dispatch()},
      {1, transmit_drain()},
      {1, receive_drain()},
      {1, transmit_ping()},
      {1, receive_ping()},
      {5, transmit_discarded()},
      {1, receive_discarded()},
      {5, transmit_lease()},
      {5, receive_error()},
      {10, transmit_init()},
      {10, receive_init()}
    ])
  end

  ## Packets

  defp transmit_request(),
    do: tuple({constant(:transmit_request), keys(), body()})

  defp receive_request(),
    do: tuple({constant(:receive_request), status(), body()})

  defp transmit_dispatch() do
    tuple({constant(:transmit_dispatch), context(), dest(), dispatch(), body()})
  end

  defp receive_dispatch(),
    do: tuple({constant(:receive_dispatch), status(), context(), body()})

  defp transmit_drain(),
    do: constant(:transmit_drain)

  defp receive_drain(),
    do: constant(:receive_drain)

  defp transmit_ping(),
    do: constant(:transmit_ping)

  defp receive_ping(),
    do: constant(:receive_ping)

  defp transmit_discarded(),
    do: tuple({constant(:transmit_discarded), tag(), why()})

  defp receive_discarded(),
    do: constant(:receive_discarded)

  defp transmit_lease(),
    do: tuple({constant(:transmit_lease), time_unit(), time_length()})

  defp receive_error(),
    do: tuple({constant(:receive_error), why()})

  defp transmit_init(),
    do: tuple({constant(:transmit_init), version(), headers()})

  defp receive_init(),
    do: tuple({constant(:receive_init), version(), headers()})

  # Helpers

  defp keys(),
    do: map_of(ascii_string(), binary())

  defp body(),
    do: binary()

  defp status(),
    do: one_of([constant(:ok), constant(:error), constant(:nack)])

  defp context(),
    do: map_of(ascii_string(), binary())

  defp dest(),
    do: ascii_string()

  defp dispatch(),
    do: map_of(dest(), dest())

  defp tag(),
    do: int(0..0x7F_FF_FF)

  defp why(),
    do: ascii_string()

  defp time_unit(),
    do: constant(:millisecond)

  defp time_length(),
    do: filter(int(), &(&1 >= 0))

  defp version(),
    do: int(0..0xFF_FF)

  defp headers(),
    do: map_of(ascii_string(), binary())
end

{:ok, _} = Application.ensure_all_started(:logger)

# set seed to satisfy stream_data (seed set on ~> v1.5)
{_, seed, _} = :os.timestamp()
ExUnit.start([seed: seed])
