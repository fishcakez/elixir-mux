defmodule Mux.Server.Delegator do
  @moduledoc false

  @behaviour Mux.ServerSession

  def init({{handshake, arg1}, {handler, arg2}}) do
    {:ok, state1} = apply(handshake, :init, [arg1])
    try do
      {:ok, _state2} = apply(handler, :init, [arg2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        # terminate/2 won't be called to trigger terminate
        apply(handshake, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, state2} ->
        {:ok, {{handshake, state1}, {handler, state2}}}
    end
  end

  def handshake(headers, {{handshake, state}, handler_info}) do
    {:ok, headers, opts, state} = apply(handshake, :handshake, [headers, state])
    {:ok, headers, opts, {{handshake, state}, handler_info}}
  end

  def dispatch(dest, tab, body, {_, {handler, state}}),
    do: apply(handler, :dispatch, [dest, tab, body, state])

  def terminate(reason, {{handshake, state1}, {handler, state2}}) do
    # terminate in reverse order of init/1
    apply(handler, :terminate, [reason, state2])
  after
    apply(handshake, :terminate, [reason, state1])
  end
end
