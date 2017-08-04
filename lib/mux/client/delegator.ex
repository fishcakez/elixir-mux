defmodule Mux.Client.Delegator do
  @moduledoc false

  @behaviour Mux.ClientSession

  def init({dest, {handshake, arg1}, {present, arg2}}) do
    {:ok, headers, state1} = apply(handshake, :init, [arg1])
    try do
      {:ok, _state2} = apply(present, :init, [arg2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        apply(handshake, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, state2} ->
        {:ok, headers, {dest, {handshake, state1}, {present, state2}}}
    end
  end

  def handshake(headers, {dest, {handshake, state}, present_info}) do
    {:ok, opts, state} = apply(handshake, :handshake, [headers, state])
    {:ok, _} = Registry.register(Mux.ClientSession, dest, present_info)
    {:ok, opts, {dest, {handshake, state}, present_info}}
  end

  def lease(_, _, data),
    do: {:ok, data}

  def drain({dest, _, _} = data) do
    Registry.unregister(Mux.ClientSession, dest)
    {:ok, data}
  end

  def terminate(reason, {dest, handshake_info, present_info}) do
    Registry.unregister(Mux.ClientSession, dest)
  after
    terminate(reason, handshake_info, present_info)
  end

  defp terminate(reason, {handshake, state1}, {present, state2}) do
    # terminate in reverse order of init/1
    apply(present, :terminate, [reason, state2])
  after
    apply(handshake, :terminate, [reason, state1])
  end
end
