defmodule Mux.Client.Delegator do
  @moduledoc false

  @behaviour Mux.ClientSession

  def init({dest, {handshake, arg}}) do
    {:ok, headers, state} = apply(handshake, :init, [arg])
    {:ok, headers, {dest, {handshake, state}}}
  end

  def handshake(headers, {dest, {handshake, state}}) do
    {:ok, opts, state} = apply(handshake, :handshake, [headers, state])
    {:ok, _} = Registry.register(Mux.ClientSession, dest, handshake)
    {:ok, opts, {dest, {handshake, state}}}
  end

  def lease(_, _, data),
    do: {:ok, data}

  def drain({dest, _} = data) do
    Registry.unregister(Mux.ClientSession, dest)
    {:ok, data}
  end

  def terminate(reason, {dest, {handshake, state}}) do
    Registry.unregister(Mux.ClientSession, dest)
  after
    apply(handshake, :terminate, [reason, state])
  end
end
