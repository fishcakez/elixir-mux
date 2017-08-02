defmodule Mux.Trace do
  @moduledoc """
  Trace context for Mux.Context.

  This module provides B3-propagation over (multiple) Mux dispatch contexts and
  passes the trace id downstream.
  """

  use Bitwise

  @behaviour Mux.Context

  @uint128_max (1 <<< 128) - 1
  @uint64_max (1 <<< 64) - 1

  @wire_key "com.twitter.finagle.tracing.TraceContext"

  @debug (1 <<< 0)
  @sampling_known (1 <<< 1)
  @sampled (1 <<< 2)
  @root (1 <<< 3)

  @enforce_keys [:trace_id, :parent_id, :span_id, :flags]
  defstruct [:trace_id, :parent_id, :span_id, :flags]

  @type trace_id :: 0..unquote(@uint128_max)
  @type span_id :: 0..unquote(@uint64_max)
  @type flag :: :debug | :sampling_known | :sampled | :root
  @type t :: %Mux.Trace{trace_id: trace_id,
                        parent_id: span_id,
                        span_id: span_id,
                        flags: [flag]}

  @doc """
  Start a new trace.

  If the argument is an existing trace, a child trace with new span id is
  returned. Otherwise if the argument is a list of trace flags a new root trace
  is returned (`:root` flag is implicit).

  ## Examples

        Mux.Trace.start([:sampling_known])

  This function does not use Mux.Context.
  """
  @spec start(t) :: t
  @spec start([flag]) :: t
  def start(%Mux.Trace{trace_id: trace_id, span_id: parent_id, flags: flags}) do
    start(trace_id, parent_id, flags)
  end
  def start(flags) do
    # trace_id should be 128 bits but wire protocol only speals 64 bits for now.
    trace_id = uint64()
    join(trace_id, trace_id, trace_id, [:root | flags])
  end

  @doc """
  Start a new child trace.

  ## Examples

      Mux.Trace.start(11412414, 1233232, [:debug])

  This function does not use Mux.Context.
  """
  @spec start(trace_id, span_id, [flag]) :: t
  def start(trace_id, parent_id, flags) do
    join(trace_id, parent_id, uint64(), Enum.filter(flags, &(&1 != :root)))
  end

  @doc """
  Join an existing trace.

  ## Examples

      Mux.Trace.join(1141241467676, 1233232787878686, 8895314843, [:debug])

  This function does not use Mux.Context.
  """
  @spec join(trace_id, span_id, span_id, [flag]) :: t
  def join(trace_id, parent_id, span_id, flags) do
    %Mux.Trace{trace_id: trace_id, parent_id: parent_id, span_id: span_id,
               flags: flags}
  end

  @doc """
  Bind a trace to the scope of an anonymous function and run the function.

  If a trace already exists in the current context the function raises a
  RuntimeError.

  ## Examples

      Mux.Trace.bind(Mux.Trace.start([]), fn -> RPC.call() end)
  """
  @spec bind(t, (() -> result)) :: result when result: var
  def bind(%Mux.Trace{} = trace, fun) do
    if Mux.Context.has_key?(Mux.Trace) do
      raise "Mux.Trace already bound in Mux.Context"
    else
      Mux.Context.bind(Mux.Trace, trace, fun)
    end
  end

  @doc """
  Bind a trace to the scope of an anonymous function and run the function.

  If a trace already exists in the current context the function raises a
  RuntimeError.

  ## Examples

      Mux.Trace.bind(124323127846, 44323123123125, 95421439, [], fn ->
        Mux.Trace.span(fn -> RPC.call() end)
      end)
  """
  @spec bind(trace_id, span_id, span_id, [flag], (() -> result)) ::
        result when result: var
  def bind(trace_id, parent_id, span_id, flags, fun) do
    trace = join(trace_id, parent_id, span_id, flags)
    bind(trace, fun)
  end

  @doc """
  Start a trace in the scope of an anonymous function and run the function.

  The trace in the current context is used as the parent trace.

  ## Examples

      Mux.Trace.span([:sampling_known], fn ->
        Mux.Trace.span(fn -> RPC.call() end)
      end)
  """
  @spec span((() -> result)) :: result when result: var
  def span(fun) do
    trace = Mux.Context.fetch!(Mux.Trace)
    Mux.Context.bind(Mux.Trace, start(trace), fun)
  end

  @doc """
  Start a trace in the scope of an anonymous function and run the function.

  If the first argument is an existing trace, a child trace with new span id is
  used. If the first argument is a list of trace flags a new root trace is
  started (`:root` flag is implicit).

  If a trace already exists in the current context the function raises a
  RuntimeError.

  ## Examples

      Mux.Trace.span([:sampling_known], fn ->
        Mux.Trace.span(fn -> RPC.call() end)
      end
  """
  @spec span(t | [flag], (() -> result)) :: result when result: var
  def span(trace_or_flags, fun) do
    trace_or_flags
    |> start()
    |> bind(fun)
  end

  @doc false
  @impl Mux.Context
  @spec put_wire(Mux.Context.wire, t) :: Mux.Context.wire
  def put_wire(ctx, trace) do
    %Mux.Trace{trace_id: trace_id, parent_id: parent_id, span_id: span_id,
               flags: flags} = trace
    # only encodes trace_id as 64bit, discarding hi 64 bits
    data = <<span_id::64, parent_id::64, trace_id::64, flags_to_int(flags)::64>>
    Map.put(ctx, @wire_key, data)
  end

  @doc false
  @impl Mux.Context
  @spec fetch_wire(Mux.Context.wire) :: {:ok, t} | :error
  def fetch_wire(%{@wire_key => data}) do
    case data do
      <<span_id::64, parent_id::64, trace_id::64, int::64>> ->
        {:ok, join(trace_id, parent_id, span_id, int_to_flags(int))}
      _ ->
        raise ArgumentError, "expected 32 bytes, got: #{inspect data}"
    end
  end
  def fetch_wire(_),
    do: :error

  ## Helpers

  defp uint64(),
    do: :rand.uniform(@uint64_max + 1) - 1

  defp int_to_flags(int) do
    pairs =
      [root: @root, debug: @debug, sampling_known: @sampling_known,
       sampled: @sampled]
    int_to_flags(pairs, int, [])
  end

  defp int_to_flags([], _int, flags),
    do: flags
  defp int_to_flags([{flag, val} | pairs], int, flags) do
    case val &&& int do
        ^val ->
          int_to_flags(pairs, int, [flag | flags])
        _ ->
          int_to_flags(pairs, int, flags)
    end
  end

  defp flags_to_int(flags, int \\ 0)

  defp flags_to_int([], int),
    do: int
  defp flags_to_int([:root | flags], int),
    do: flags_to_int(flags, int ||| @root)
  defp flags_to_int([:debug | flags], int),
    do: flags_to_int(flags, int ||| @debug)
  defp flags_to_int([:sampling_known | flags], int),
    do: flags_to_int(flags, int ||| @sampling_known)
  defp flags_to_int([:sampled | flags], int),
    do: flags_to_int(flags, int ||| @sampled)
end
