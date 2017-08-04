defmodule Mux.Client.Delegator do
  @moduledoc false

  @behaviour Mux.Client.Connection

  def init({dest, {session, arg1}, {present, arg2}}) do
    {:ok, headers, state1} = apply(session, :init, [arg1])
    try do
      {:ok, _state2} = apply(present, :init, [arg2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        apply(session, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, state2} ->
        {:ok, headers, {dest, {session, state1}, {present, state2}}}
    end
  end

  def handshake(headers, {dest, {session, state}, present_info}) do
    {:ok, opts, state} = apply(session, :handshake, [headers, state])
    {:ok, _} = Registry.register(Mux.Client.Connection, dest, present_info)
    {:ok, opts, {dest, {session, state}, present_info}}
  end

  def lease(_, _, data),
    do: {:ok, data}

  def drain({dest, _, _} = data) do
    Registry.unregister(Mux.Client.Connection, dest)
    {:ok, data}
  end

  def terminate(reason, {dest, session_info, present_info}) do
    Registry.unregister(Mux.Client.Connection, dest)
  after
    terminate(reason, session_info, present_info)
  end

  defp terminate(reason, {session, state1}, {present, state2}) do
    # terminate in reverse order of init/1
    apply(present, :terminate, [reason, state2])
  after
    apply(session, :terminate, [reason, state1])
  end
end
