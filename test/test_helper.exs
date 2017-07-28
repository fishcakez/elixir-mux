defmodule MuxProxy do
  @behaviour Mux.Connection

  def commands(pid, commands),
    do: send(pid, {:commands, commands})

  def spawn_link(socket, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init_it, [self(), opts])
    :ok = :gen_tcp.controlling_process(socket, pid)
    send(pid, {self(), socket})
    pid
  end

  def init_it(parent, opts) do
    receive do
      {^parent, socket} ->
        Mux.Connection.enter_loop(__MODULE__, socket, parent, opts)
    end
  end

  def init(parent),
    do: {[], parent}

  def handle(_, {:commands, commands}, parent),
    do: {commands, parent}
  def handle(event_type, event, parent) do
    send(parent, {self(), event_type, event})
    {[], parent}
  end

  def terminate(reason, parent),
    do: send(parent, {self(), :terminate, reason})
end

defmodule MuxClientProxy do
  @behaviour Mux.Client

  def spawn_link(socket, headers, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init_it, [self(), headers, opts])
    :ok = :gen_tcp.controlling_process(socket, pid)
    send(pid, {self(), socket})
    pid
  end

  def init_it(parent, headers, opts) do
    receive do
      {^parent, socket} ->
        Mux.Client.enter_loop(__MODULE__, socket, {headers, parent}, opts)
    end
  end

  def init({headers, parent}),
    do: {:ok, headers, parent}

  def handshake(headers, parent) do
    send(parent, {self(), :handshake, headers})
    receive do
      {^parent, result} ->
        result
    end
  end

  def nack(context, dest, dest_tab, body, parent) do
    send(parent, {self(), :nack, {context, dest, dest_tab, body}})
    receive do
      {^parent, result} ->
        result
    end
  end

  def lease(time_unit, timeout, parent) do
    send(parent, {self(), :lease, {time_unit, timeout}})
    receive do
      {^parent, result} ->
        result
    end
  end

  def drain(parent) do
    send(parent, {self(), :drain, nil})
    receive do
      {^parent, result} ->
        result
    end
  end

  def terminate(reason, parent) do
    send(parent, {self(), :terminate, reason})
  end
end
defmodule MuxServerProxy do
  @behaviour Mux.ServerSession

  def spawn_link(socket, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init_it, [self(), opts])
    :ok = :gen_tcp.controlling_process(socket, pid)
    send(pid, {self(), socket})
    pid
  end

  def init_it(parent, opts) do
    receive do
      {^parent, socket} ->
        Mux.ServerSession.enter_loop(__MODULE__, socket, parent, opts)
    end
  end

  def init(parent),
    do: {:ok, parent}

  def handshake(headers, parent) do
    send(parent, {self(), :handshake, headers})
    receive do
      {^parent, result} ->
        result
    end
  end

  def dispatch(context, dest, dest_tab, body, parent) do
    send(parent, {self(), :dispatch, {context, dest, dest_tab, body}})
    receive do
      {^parent, result} ->
        result
    end
  end

  def nack(context, dest, dest_tab, body, parent) do
    send(parent, {self(), :nack, {context, dest, dest_tab, body}})
    receive do
      {^parent, result} ->
        result
    end
  end

  def terminate(reason, parent) do
    send(parent, {self(), :terminate, reason})
  end
end

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
