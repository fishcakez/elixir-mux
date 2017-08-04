defmodule Mux.Server.Delegator do
  @moduledoc false

  @behaviour Mux.Server.Connection

  def init({{handshake, arg1}, {present, arg2}, handler_info}) do
    {:ok, state1} = apply(handshake, :init, [arg1])
    try do
      {:ok, _state2} = apply(present, :init, [arg2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        # terminate/2 won't be called to trigger terminate
        apply(handshake, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, state2} ->
        init({handshake, state1}, {present, state2}, handler_info)
    end
  end

  def handshake(headers, {{handshake, state}, present_info, handler_info}) do
    {:ok, headers, opts, state} = apply(handshake, :handshake, [headers, state])
    {:ok, headers, opts, {{handshake, state}, present_info, handler_info}}
  end

  def dispatch(dest, tab, body, {_, {present, state1}, {handler, state2}}) do
    {:ok, req} = apply(present, :decode, [body, state1])
    case apply(handler, :dispatch, [dest, tab, req, state2]) do
      {:ok, resp} ->
        {:ok, _} = apply(present, :encode, [resp, state1])
      {:error, } = error ->
        error
      :nack ->
        :nack
    end
  end

  def terminate(reason, {handshake_info, present_info, {handler, state}}) do
    # terminate in reverse order of init/1
    apply(handler, :terminate, [reason, state])
  after
    terminate(reason, handshake_info, present_info)
  end

  defp init(handshake_info, present_info, {handler, arg}) do
    try do
      {:ok, _contexts, _state} = apply(handler, :init, [arg])
    catch
      kind, reason ->
        stack = System.stacktrace()
        # terminate/2 won't be called to trigger terminate
        terminate(reason, handshake_info, present_info)
        :erlang.raise(kind, reason, stack)
    else
      {:ok, contexts, state} ->
        {:ok, contexts, {handshake_info, present_info, {handler, state}}}
    end
  end

  defp terminate(reason, {handshake, state1}, {present, state2}) do
    # terminate in reverse order of init/1
    apply(present, :terminate, [reason, state2])
  after
    apply(handshake, :terminate, [reason, state1])
  end
end
