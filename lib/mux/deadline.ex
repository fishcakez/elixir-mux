defmodule Mux.Deadline do
  @moduledoc """
  Deadline context for Mux.Context.

  This module provides deadlines over (multiple) Mux dispatch contexts and
  passes the deadline downstream.
  """

  @behaviour Mux.Context

  @wire_key "com.twitter.finagle.Deadline"
  @time_unit :nanosecond

  @enforce_keys [:start, :finish, :time_offset]
  defstruct [:start, :finish, :time_offset]

  @type t :: %__MODULE__{start: integer,
                         finish: integer,
                         time_offset: integer}

  @doc """
  Create a deadline struct.

  This function does not use Mux.Context.
  """
  @spec new(non_neg_integer) :: t
  def new(timeout) do
    start = System.monotonic_time(@time_unit)
    time_offset = System.time_offset(@time_unit)
    %__MODULE__{start: start, finish: start + timeout, time_offset: time_offset}
  end

  @doc """
  Merge two deadlines and return a new deadline.

  This function is pure and does not use Mux.Context.
  """
  @spec merge(t, t) :: t
  def merge(deadline1, deadline2) do
    %Mux.Deadline{start: start1, finish: finish1} = deadline1
    %Mux.Deadline{start: start2, finish: finish2} = deadline2
    # most recent start/time_offset (accuracy) and earliest finish (strict)
    finish = min(finish1, finish2)
    if start1 > start2 do
      %Mux.Deadline{deadline1 | finish: finish}
    else
      %Mux.Deadline{deadline2 | finish: finish}
    end
  end

  @doc """
  Bind a deadline to the scope of an anonymouns function and run the function.

  The first argument is either a deadline struct or a non-infinity timeout
  (creating a new deadline). If a deadline already exists in the current context
  the deadlines are merged.

  ## Examples

      Mux.Deadline.bind(1000, fn ->
        GenServer.call(MyServer, :request, Mux.Deadline.timeout())
      end)
  """
  @spec bind(t | non_neg_integer, (() -> result)) :: result when result: var
  def bind(%Mux.Deadline{} = deadline, fun) do
    case Mux.Context.fetch(Mux.Deadline) do
      {:ok, old} ->
        Mux.Context.bind(Mux.Deadline, merge(old, deadline), fun)
      :error ->
        Mux.Context.bind(Mux.Deadline, deadline, fun)
    end
  end
  def bind(timeout, fun) do
    timeout
    |> new()
    |> bind(fun)
  end

  @doc """
  Start a timer that sends a message when the deadline expires.

  If a deadline is not supplied, the deadline from the current context is used.

  ## Examples

    Mux.Deadline.bind(1000, fn -> Mux.Deadline.start_timer(self(), :BOOM) end)
  """
  @spec start_timer(pid | atom, any) :: reference()
  @spec start_timer(t, pid | atom, any) :: reference()
  def start_timer(deadline \\ Mux.Context.fetch!(Mux.Deadline), dest, msg)
  def start_timer(%Mux.Deadline{finish: finish}, dest, msg) do
    abs = System.convert_time_unit(finish, @time_unit, :millisecond)
    :erlang.start_timer(abs, dest, msg, [abs: true])
  end

  @doc """
  Return a timeout for when the deadline expires.

  If a deadline is not supplied, the deadline from the current context is used.

  ## Examples

      Mux.Deadline.bind(1000, fn ->
        GenServer.call(MyServer, :request, Mux.Deadline.timeout())
      end)
  """
  @spec timeout() :: non_neg_integer
  @spec timeout(t) :: non_neg_integer
  def timeout(deadline \\ Mux.Context.fetch!(Mux.Deadline))
  def timeout(%Mux.Deadline{finish: finish}) do
    now = System.monotonic_time(@time_unit)
    finish
    |> Kernel.-(now)
    |> System.convert_time_unit(@time_unit, :millisecond)
    |> max(0)
  end

  @doc false
  @impl Mux.Context
  @spec put_wire(Mux.Context.wire, t) :: Mux.Context.wire
  def put_wire(wire, deadline) do
    %Mux.Deadline{start: start, finish: finish, time_offset: offset} = deadline
    sys_start = start + offset
    sys_finish = finish + offset
    data = <<sys_start::64, sys_finish::64>>
    Map.put(wire, @wire_key, data)
  end

  @doc false
  @impl Mux.Context
  @spec fetch_wire(Mux.Context.wire) :: {:ok, t} | :error
  def fetch_wire(%{@wire_key => data}) do
    case data do
      <<sys_start::64, sys_finish::64>> ->
        offset = System.time_offset(:nanosecond)
        start = sys_start - offset
        finish = sys_finish - offset
        {:ok, %Mux.Deadline{start: start, finish: finish, time_offset: offset}}
      _ ->
        raise "expected 16 bytes, got: #{inspect data}"
    end
  end
  def fetch_wire(_),
    do: :error
end
