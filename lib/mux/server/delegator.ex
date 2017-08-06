defmodule Mux.Server.Delegator do
  @moduledoc false

  @behaviour Mux.Server.Connection

  def init({{session, arg1}, {present, arg2}, handler_info}) do
    {:ok, state1} = apply(session, :init, [arg1])
    try do
      {:ok, _state2} = apply(present, :init, [arg2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        # terminate/2 won't be called to trigger terminate
        apply(session, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, state2} ->
        init({session, state1}, {present, state2}, handler_info)
    end
  end

  def handshake(headers, {{session, state}, present_info, handler_info}) do
    {:ok, headers, opts, state} = apply(session, :handshake, [headers, state])
    {:ok, headers, opts, {{session, state}, present_info, handler_info}}
  end

  def dispatch(dest, tab, body, {_, {present, state1}, {handler, state2}}) do
    {:ok, metadata, req} = apply(present, :decode, [body, state1])
    case apply(handler, :dispatch, [dest, tab, req, state2]) do
      {:ok, resp} ->
        {:ok, _} = apply(present, :encode, [metadata, resp, state1])
      {:error, } = error ->
        error
      :nack ->
        :nack
    end
  end

  def terminate(reason, {session_info, present_info, {handler, state}}) do
    # terminate in reverse order of init/1
    apply(handler, :terminate, [reason, state])
  after
    terminate(reason, session_info, present_info)
  end

  defp init(session_info, present_info, {handler, arg}) do
    try do
      {:ok, _contexts, _state} = apply(handler, :init, [arg])
    catch
      kind, reason ->
        stack = System.stacktrace()
        # terminate/2 won't be called to trigger terminate
        terminate(reason, session_info, present_info)
        :erlang.raise(kind, reason, stack)
    else
      {:ok, contexts, state} ->
        {:ok, contexts, {session_info, present_info, {handler, state}}}
    end
  end

  defp terminate(reason, {session, state1}, {present, state2}) do
    # terminate in reverse order of init/1
    apply(present, :terminate, [reason, state2])
  after
    apply(session, :terminate, [reason, state1])
  end
end
